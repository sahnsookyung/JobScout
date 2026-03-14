#!/usr/bin/env python3
"""
Tests for Pipeline Service
Covers: web/backend/services/pipeline_service.py
"""

import pytest
import threading
import asyncio
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from uuid import uuid4

from web.backend.services.pipeline_service import (
    PipelineTaskManager,
    PipelineTask,
    get_pipeline_manager,
)


class TestPipelineTask:
    """Test PipelineTask dataclass."""

    def test_create_task_with_minimal_fields(self):
        """Test creating a task with minimal required fields."""
        task = PipelineTask(
            task_id="test-123",
            status="pending"
        )

        assert task.task_id == "test-123"
        assert task.status == "pending"
        assert task.step is None
        assert task.orchestrator_task_id is None
        assert task.error is None
        assert task.message is None
        assert task.phases is None
        assert isinstance(task.created_at, datetime)
        assert isinstance(task.stop_event, threading.Event)

    def test_create_task_with_all_fields(self):
        """Test creating a task with all fields populated."""
        stop_event = threading.Event()
        phases = {"extraction": "completed", "embeddings": "pending"}

        task = PipelineTask(
            task_id="test-456",
            status="running",
            step="vector_matching",
            orchestrator_task_id="orch-789",
            error=None,
            message="Processing",
            phases=phases,
            stop_event=stop_event
        )

        assert task.task_id == "test-456"
        assert task.status == "running"
        assert task.step == "vector_matching"
        assert task.orchestrator_task_id == "orch-789"
        assert task.message == "Processing"
        assert task.phases == phases
        assert task.stop_event is stop_event


class TestPipelineTaskManager:
    """Test PipelineTaskManager functionality."""

    @pytest.fixture
    def manager(self):
        """Create a fresh PipelineTaskManager instance."""
        return PipelineTaskManager()

    # FIX 1: add @patch to prevent the background thread from changing status
    # before the assertion runs — all other tests in this class already do this
    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_create_task_when_no_running_tasks(self, mock_run, manager):
        """Test creating a new task when no tasks are running."""
        task_id = manager.create_task()

        assert task_id is not None
        assert len(task_id) == 36  # UUID format

        task = manager.get_task(task_id)
        assert task is not None
        assert task.task_id == task_id
        assert task.status == "pending"
        assert task.step == "initializing"


    def test_create_task_starts_background_thread(self):
        """Test that create_task starts a background thread."""
        manager = PipelineTaskManager()

        with patch('web.backend.services.pipeline_service.threading.Thread') as mock_thread_cls:
            mock_thread_instance = Mock()
            mock_thread_cls.return_value = mock_thread_instance

            task_id = manager.create_task()

            mock_thread_cls.assert_called_once()

            # Use .call_args.kwargs (the named attribute) instead of [1] indexing —
            # more reliable across mock versions and avoids KeyError if positional
            # args are present
            call_kwargs = mock_thread_cls.call_args.kwargs

            # Thread uses a closure so task_id is captured, not passed as args
            assert 'args' not in call_kwargs
            assert call_kwargs['daemon'] is True
            assert callable(call_kwargs['target'])

            # Invoke the closure under a patch to confirm it calls
            # _run_pipeline_background with the correct task_id
            with patch.object(manager, '_run_pipeline_background') as mock_run:
                call_kwargs['target']()
                mock_run.assert_called_once_with(task_id)

            mock_thread_instance.start.assert_called_once()

    def test_create_task_returns_existing_pending_task(self, manager):
        """Test that create_task returns existing task ID when one is pending."""
        first_task_id = manager.create_task()
        second_task_id = manager.create_task()

        assert second_task_id == first_task_id

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_get_task_exists(self, mock_run, manager):
        """Test getting an existing task."""
        task_id = manager.create_task()
        task = manager.get_task(task_id)

        assert task is not None
        assert task.task_id == task_id
        assert task.status == "pending"

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_get_task_not_exists(self, mock_run, manager):
        """Test getting a non-existent task."""
        task = manager.get_task("non-existent-id")
        assert task is None

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_get_active_task_running(self, mock_run, manager):
        """Test getting active task when one is running."""
        task_id = manager.create_task()
        manager.update_task_status(task_id, "running", step="matching")

        active_task = manager.get_active_task()

        assert active_task is not None
        assert active_task.task_id == task_id
        assert active_task.status == "running"

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_get_active_task_pending(self, mock_run, manager):
        """Test getting active task when one is pending."""
        task_id = manager.create_task()

        active_task = manager.get_active_task()

        assert active_task is not None
        assert active_task.task_id == task_id
        assert active_task.status == "pending"

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_get_active_task_none(self, mock_run, manager):
        """Test getting active task when none are active."""
        task_id = manager.create_task()
        manager.update_task_status(task_id, "completed")

        active_task = manager.get_active_task()
        assert active_task is None

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_update_task_status_basic(self, mock_run, manager):
        """Test basic task status update."""
        task_id = manager.create_task()

        manager.update_task_status(task_id, "running", step="vector_matching")

        task = manager.get_task(task_id)
        assert task.status == "running"
        assert task.step == "vector_matching"

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_update_task_status_with_error(self, mock_run, manager):
        """Test task status update with error."""
        task_id = manager.create_task()

        manager.update_task_status(
            task_id, "failed", step="matching", error="Database connection failed"
        )

        task = manager.get_task(task_id)
        assert task.status == "failed"
        assert task.step == "matching"
        assert task.error == "Database connection failed"

    def test_update_task_status_nonexistent_task(self, manager):
        """Test updating status of non-existent task doesn't raise."""
        # Should not raise
        manager.update_task_status("non-existent", "running")
        assert True

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_request_stop_existing_task(self, mock_run, manager):
        """Test requesting stop of existing task."""
        task_id = manager.create_task()

        result = manager.request_stop(task_id)

        assert result is True
        task = manager.get_task(task_id)
        assert task.stop_event.is_set() is True

    def test_request_stop_nonexistent_task(self, manager):
        """Test requesting stop of non-existent task."""
        result = manager.request_stop("non-existent")

        assert result is False

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_stop_active_task_running(self, mock_run, manager):
        """Test stopping active running task."""
        task_id = manager.create_task()
        manager.update_task_status(task_id, "running")

        stopped_id = manager.stop_active_task()

        assert stopped_id == task_id
        task = manager.get_task(task_id)
        assert task.stop_event.is_set() is True

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_stop_active_task_pending(self, mock_run, manager):
        """Test stopping active pending task."""
        task_id = manager.create_task()

        stopped_id = manager.stop_active_task()

        assert stopped_id == task_id
        task = manager.get_task(task_id)
        assert task.stop_event.is_set() is True

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_stop_active_task_none(self, mock_run, manager):
        """Test stopping when no active task."""
        task_id = manager.create_task()
        manager.update_task_status(task_id, "completed")

        stopped_id = manager.stop_active_task()

        assert stopped_id is None

    def test_subscribe_creates_queue(self, manager):
        """Test subscribing creates a new queue."""
        task_id = "test-123"

        queue = manager.subscribe(task_id)

        assert queue is not None
        assert isinstance(queue, asyncio.Queue)

    def test_subscribe_increments_subscriber_count(self, manager):
        """Test subscribing increments subscriber count."""
        task_id = "test-123"

        queue1 = manager.subscribe(task_id)
        queue2 = manager.subscribe(task_id)

        # Should return same queue
        assert queue1 is queue2

    def test_unsubscribe_decrements_subscriber_count(self, manager):
        """Test unsubscribing decrements subscriber count."""
        task_id = "test-123"

        manager.subscribe(task_id)
        manager.subscribe(task_id)
        manager.unsubscribe(task_id)

        # Queue should still exist (one subscriber remaining)
        assert task_id in manager._event_queues

        manager.unsubscribe(task_id)
        # Queue should be removed (no subscribers)
        assert task_id not in manager._event_queues

    def test_unsubscribe_nonexistent_task(self, manager):
        """Test unsubscribing from non-existent task doesn't raise."""
        # Should not raise
        manager.unsubscribe("non-existent")
        assert True

    def test_publish_update_success(self, manager):
        """Test publishing update to subscribers."""
        task_id = "test-123"
        queue = manager.subscribe(task_id)

        data = {"status": "running", "step": "matching"}
        manager.publish_update(task_id, data)

        # Check queue has the message
        assert not queue.empty()
        queued_data = queue.get_nowait()
        assert queued_data == data

    def test_publish_update_no_subscribers(self, manager):
        """Test publishing update with no subscribers doesn't raise."""
        # Should not raise
        manager.publish_update("non-existent", {"status": "running"})
        assert True

    def test_publish_update_queue_full(self, manager):
        """Test publishing update when queue is full."""
        task_id = "test-123"
        queue = manager.subscribe(task_id)

        # Fill the queue (default maxsize is 0 = unlimited, so we need to mock)
        with patch.object(queue, 'put_nowait', side_effect=asyncio.QueueFull()):
            # Should not raise, just log error
            manager.publish_update(task_id, {"status": "running"})

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_cleanup_completed_tasks_keeps_recent(self, mock_run, manager):
        """Test cleanup keeps recent completed tasks."""
        # Create multiple tasks and complete them
        task_ids = []
        for i in range(10):
            task_id = manager.create_task()
            task_ids.append(task_id)
            # Directly update status without triggering background execution
            manager._tasks[task_id].status = "completed"

        # Cleanup should keep 5 most recent
        manager._cleanup_completed_tasks(keep_count=5)

        remaining = len([t for t in manager._tasks.values()])
        assert remaining == 5

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_cleanup_completed_tasks_all_completed(self, mock_run, manager):
        """Test cleanup with all completed tasks."""
        task_id = manager.create_task()
        manager._tasks[task_id].status = "completed"

        manager._cleanup_completed_tasks(keep_count=5)

        # Should keep the task since it's under keep_count
        assert manager.get_task(task_id) is not None

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_cleanup_completed_tasks_mixed_status(self, mock_run, manager):
        """Test cleanup with mixed task statuses."""
        # Create running task directly without triggering background thread
        running_id = "running-task-id"
        manager._tasks[running_id] = PipelineTask(
            task_id=running_id,
            status="running",
            step="matching"
        )

        # Create completed tasks directly
        completed_ids = []
        for i in range(7):
            task_id = f"completed-task-{i}"
            manager._tasks[task_id] = PipelineTask(
                task_id=task_id,
                status="completed",
                step="matching"
            )
            completed_ids.append(task_id)

        manager._cleanup_completed_tasks(keep_count=3)

        # Running task should still exist
        assert manager.get_task(running_id) is not None

        # Only 3 completed should remain (the most recent ones)
        completed_remaining = sum(
            1 for tid in completed_ids if manager.get_task(tid) is not None
        )
        assert completed_remaining == 3


    @patch('web.backend.services.clients.orchestrator_client')
    def test_run_pipeline_background_success(self, mock_client, manager):
        """Test successful pipeline execution in background."""
        task_id = "test-123"
        manager._tasks[task_id] = PipelineTask(
            task_id=task_id,
            status="pending",
            step="initializing"
        )

        mock_client.start_matching.return_value = {
            'success': True,
            'task_id': 'orch-456'
        }

        manager._run_pipeline_background(task_id)

        task = manager.get_task(task_id)
        assert task.status == "completed"
        assert task.step == "matching"
        assert task.orchestrator_task_id == 'orch-456'

    @patch('web.backend.services.clients.orchestrator_client')
    def test_run_pipeline_background_orchestrator_failure(self, mock_client, manager):
        """Test pipeline execution with orchestrator failure."""
        task_id = "test-123"
        manager._tasks[task_id] = PipelineTask(
            task_id=task_id,
            status="pending",
            step="initializing"
        )

        mock_client.start_matching.return_value = {
            'success': False,
            'message': 'Orchestrator failed'
        }

        manager._run_pipeline_background(task_id)

        task = manager.get_task(task_id)
        assert task.status == "failed"
        assert task.error == 'Orchestrator failed'

    @patch('web.backend.services.clients.orchestrator_client')
    def test_run_pipeline_background_exception(self, mock_client, manager):
        """Test pipeline execution with exception."""
        task_id = "test-123"
        manager._tasks[task_id] = PipelineTask(
            task_id=task_id,
            status="pending",
            step="initializing"
        )

        mock_client.start_matching.side_effect = Exception("Database error")

        manager._run_pipeline_background(task_id)

        task = manager.get_task(task_id)
        assert task.status == "failed"
        assert "Database error" in task.error

    def test_run_pipeline_background_task_not_found(self, manager, caplog):
        """Test pipeline execution when task not found."""
        import logging
        caplog.set_level(logging.ERROR)
        task_id = "non-existent"

        manager._run_pipeline_background(task_id)

        # Should log error and not raise
        assert "not found" in caplog.text.lower()

    def test_run_pipeline_background_stop_requested(self, manager):
        """Test pipeline execution when stop is requested."""
        task_id = "test-123"
        manager._tasks[task_id] = PipelineTask(
            task_id=task_id,
            status="pending",
            step="initializing"
        )
        manager._tasks[task_id].stop_event.set()

        manager._run_pipeline_background(task_id)

        task = manager.get_task(task_id)
        assert task.status == "cancelled"


class TestGetPipelineManager:
    """Test get_pipeline_manager singleton function."""

    def test_get_pipeline_manager_returns_singleton(self):
        """Test that get_pipeline_manager returns the same instance."""
        manager1 = get_pipeline_manager()
        manager2 = get_pipeline_manager()

        assert manager1 is manager2

    def test_get_pipeline_manager_returns_pipeline_task_manager(self):
        """Test that get_pipeline_manager returns PipelineTaskManager instance."""
        manager = get_pipeline_manager()

        assert isinstance(manager, PipelineTaskManager)
