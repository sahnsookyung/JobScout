#!/usr/bin/env python3
"""Tests for the current pipeline task manager API."""

import asyncio
import threading
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from pipeline.runner import MatchingPipelineResult
from web.backend.services.pipeline_service import (
    PipelineTask,
    PipelineTaskManager,
    get_pipeline_manager,
)


class TestPipelineTask:
    def test_create_task_with_minimal_fields(self):
        task = PipelineTask(
            task_id="test-123",
            task_type="matching",
            status="pending",
        )

        assert task.task_id == "test-123"
        assert task.task_type == "matching"
        assert task.status == "pending"
        assert task.step is None
        assert task.orchestrator_task_id is None
        assert task.error is None
        assert task.message is None
        assert task.phases is None
        assert isinstance(task.created_at, datetime)
        assert isinstance(task.stop_event, threading.Event)

    def test_create_task_with_all_fields(self):
        stop_event = threading.Event()
        phases = {"matching": {"status": "running"}}

        task = PipelineTask(
            task_id="test-456",
            task_type="resume_etl",
            status="running",
            step="embedding",
            orchestrator_task_id="orch-789",
            message="Processing",
            phases=phases,
            stop_event=stop_event,
            cancellation_requested=True,
            persistence_started=True,
        )

        assert task.task_type == "resume_etl"
        assert task.step == "embedding"
        assert task.orchestrator_task_id == "orch-789"
        assert task.message == "Processing"
        assert task.phases == phases
        assert task.stop_event is stop_event
        assert task.cancellation_requested is True
        assert task.persistence_started is True


class TestPipelineTaskManager:
    @pytest.fixture
    def manager(self):
        return PipelineTaskManager()

    def test_create_task_creates_resume_task(self, manager):
        task_id = manager.create_task()
        task = manager.get_task(task_id)

        assert task is not None
        assert task.task_type == "resume_etl"
        assert task.status == "pending"
        assert task.step == "initializing"

    def test_create_task_returns_distinct_resume_tasks(self, manager):
        first = manager.create_task()
        second = manager.create_task()

        assert first != second

    @patch("web.backend.services.pipeline_service.threading.Thread")
    def test_create_matching_task_reuses_active_matching_task(self, mock_thread_cls, manager):
        manager._tasks["task-1"] = PipelineTask(
            task_id="task-1",
            task_type="matching",
            status="pending",
        )

        assert manager.create_matching_task() == "task-1"
        mock_thread_cls.assert_not_called()

    @patch("web.backend.services.pipeline_service.threading.Thread")
    def test_create_matching_task_starts_thread(self, mock_thread_cls, manager):
        thread = MagicMock()
        mock_thread_cls.return_value = thread

        task_id = manager.create_matching_task()

        task = manager.get_task(task_id)
        assert task is not None
        assert task.task_type == "matching"
        assert task.status == "pending"
        thread.start.assert_called_once_with()

    def test_get_active_task_returns_matching_only(self, manager):
        manager._tasks["resume"] = PipelineTask(
            task_id="resume",
            task_type="resume_etl",
            status="running",
        )
        manager._tasks["match"] = PipelineTask(
            task_id="match",
            task_type="matching",
            status="running",
        )

        assert manager.get_active_task().task_id == "match"

    def test_request_stop_existing_task(self, manager):
        manager._tasks["task-1"] = PipelineTask(
            task_id="task-1",
            task_type="matching",
            status="running",
            step="scoring",
        )

        result = manager.request_stop("task-1")

        assert result == "cancellation_requested"
        task = manager.get_task("task-1")
        assert task.cancellation_requested is True
        assert task.stop_event.is_set() is True

    def test_request_stop_nonexistent_task(self, manager):
        assert manager.request_stop("missing") is None

    def test_stop_active_task_returns_selected_task(self, manager):
        manager._tasks["task-1"] = PipelineTask(
            task_id="task-1",
            task_type="matching",
            status="pending",
        )

        stopped = manager.stop_active_task()

        assert stopped is not None
        assert stopped.task_id == "task-1"
        assert stopped.status == "cancellation_requested"

    def test_subscribe_publish_and_unsubscribe(self, manager):
        queue = manager.subscribe("task-1")
        manager.publish_update("task-1", {"status": "running"})
        assert queue.get_nowait() == {"status": "running"}

        manager.unsubscribe("task-1")
        assert "task-1" not in manager._event_queues

    def test_cleanup_completed_tasks_keeps_recent(self, manager):
        for idx in range(7):
            manager._tasks[f"done-{idx}"] = PipelineTask(
                task_id=f"done-{idx}",
                task_type="matching",
                status="completed",
            )

        manager._cleanup_completed_tasks(keep_count=3)

        remaining = [task for task in manager._tasks.values() if task.status == "completed"]
        assert len(remaining) == 3

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_success(
        self,
        _mock_load_config,
        mock_build,
        mock_resume_etl,
        mock_run_matching,
        manager,
    ):
        manager._tasks["task-1"] = PipelineTask(
            task_id="task-1",
            task_type="matching",
            status="pending",
        )
        mock_build.return_value = MagicMock()
        mock_run_matching.return_value = MatchingPipelineResult(
            success=True,
            matches_count=1,
            saved_count=1,
            notified_count=0,
        )

        manager._run_pipeline_background("task-1")

        task = manager.get_task("task-1")
        assert task.status == "completed"
        mock_resume_etl.assert_called_once()
        mock_run_matching.assert_called_once()

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_failure(
        self,
        _mock_load_config,
        mock_build,
        _mock_resume_etl,
        mock_run_matching,
        manager,
    ):
        manager._tasks["task-2"] = PipelineTask(
            task_id="task-2",
            task_type="matching",
            status="pending",
        )
        mock_build.return_value = MagicMock()
        mock_run_matching.return_value = MatchingPipelineResult(
            success=False,
            matches_count=0,
            saved_count=0,
            notified_count=0,
            error="pipeline failed",
        )

        manager._run_pipeline_background("task-2")

        task = manager.get_task("task-2")
        assert task.status == "failed"
        assert task.error == "pipeline failed"

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_cancelled_before_matching(
        self,
        _mock_load_config,
        mock_build,
        mock_resume_etl,
        mock_run_matching,
        manager,
    ):
        task = PipelineTask(
            task_id="task-3",
            task_type="matching",
            status="pending",
        )
        task.stop_event.set()
        manager._tasks["task-3"] = task
        mock_build.return_value = MagicMock()

        manager._run_pipeline_background("task-3")

        assert manager.get_task("task-3").status == "cancelled"
        mock_run_matching.assert_not_called()

    def test_run_pipeline_background_task_not_found(self, manager, caplog):
        manager._run_pipeline_background("missing")
        assert "not found" in caplog.text.lower()


class TestGetPipelineManager:
    def test_get_pipeline_manager_returns_singleton(self):
        assert get_pipeline_manager() is get_pipeline_manager()

    def test_get_pipeline_manager_returns_pipeline_task_manager(self):
        assert isinstance(get_pipeline_manager(), PipelineTaskManager)
