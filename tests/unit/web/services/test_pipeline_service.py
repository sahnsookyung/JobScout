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

    def test_create_task_when_no_running_tasks(self, manager):
        """Test creating a new task when no tasks are running."""
        task_id = manager.create_task()

        assert task_id is not None
        assert len(task_id) == 36  # UUID format

        task = manager.get_task(task_id)
        assert task is not None
        assert task.task_id == task_id
        assert task.status == "pending"
        assert task.step == "initializing"

    def test_create_task_returns_existing_running_task(self, manager):
        """Test that create_task returns existing task ID when one is running."""
        # Create first task
        first_task_id = manager.create_task()

        # Manually set it to running
        manager.update_task_status(first_task_id, "running")

        # Create second task should return first task's ID
        second_task_id = manager.create_task()

        assert second_task_id == first_task_id

    def test_create_task_returns_existing_pending_task(self, manager):
        """Test that create_task returns existing task ID when one is pending."""
        first_task_id = manager.create_task()
        second_task_id = manager.create_task()

        assert second_task_id == first_task_id

    def test_get_task_exists(self, manager):
        """Test getting an existing task."""
        task_id = manager.create_task()
        task = manager.get_task(task_id)

        assert task is not None
        assert task.task_id == task_id
        assert task.status == "pending"

    def test_get_task_not_exists(self, manager):
        """Test getting a non-existent task."""
        task = manager.get_task("non-existent-id")
        assert task is None

    def test_get_active_task_running(self, manager):
        """Test getting active task when one is running."""
        task_id = manager.create_task()
        manager.update_task_status(task_id, "running", step="matching")

        active_task = manager.get_active_task()

        assert active_task is not None
        assert active_task.task_id == task_id
        assert active_task.status == "running"

    def test_get_active_task_pending(self, manager):
        """Test getting active task when one is pending."""
        task_id = manager.create_task()

        active_task = manager.get_active_task()

        assert active_task is not None
        assert active_task.task_id == task_id
        assert active_task.status == "pending"

    def test_get_active_task_none(self, manager):
        """Test getting active task when none are active."""
        task_id = manager.create_task()
        manager.update_task_status(task_id, "completed")

        active_task = manager.get_active_task()
        assert active_task is None

    def test_update_task_status_basic(self, manager):
        """Test basic task status update."""
        task_id = manager.create_task()

        manager.update_task_status(task_id, "running", step="vector_matching")

        task = manager.get_task(task_id)
        assert task.status == "running"
        assert task.step == "vector_matching"

    def test_update_task_status_with_error(self, manager):
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

    def test_request_stop_existing_task(self, manager):
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

    def test_stop_active_task_running(self, manager):
        """Test stopping active running task."""
        task_id = manager.create_task()
        manager.update_task_status(task_id, "running")

        stopped_id = manager.stop_active_task()

        assert stopped_id == task_id
        task = manager.get_task(task_id)
        assert task.stop_event.is_set() is True

    def test_stop_active_task_pending(self, manager):
        """Test stopping active pending task."""
        task_id = manager.create_task()

        stopped_id = manager.stop_active_task()

        assert stopped_id == task_id
        task = manager.get_task(task_id)
        assert task.stop_event.is_set() is True

    def test_stop_active_task_none(self, manager):
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

    def test_cleanup_completed_tasks_keeps_recent(self, manager):
        """Test cleanup keeps recent completed tasks."""
        # Create multiple tasks and complete them
        task_ids = []
        for i in range(10):
            task_id = manager.create_task()
            task_ids.append(task_id)
            manager.update_task_status(task_id, "completed")

        # Cleanup should keep 5 most recent
        manager._cleanup_completed_tasks(keep_count=5)

        remaining = len([t for t in manager._tasks.values()])
        assert remaining == 5

    def test_cleanup_completed_tasks_all_completed(self, manager):
        """Test cleanup with all completed tasks."""
        task_id = manager.create_task()
        manager.update_task_status(task_id, "completed")

        manager._cleanup_completed_tasks(keep_count=5)

        # Should keep the task since it's under keep_count
        assert manager.get_task(task_id) is not None

    def test_cleanup_completed_tasks_mixed_status(self, manager):
        """Test cleanup with mixed task statuses."""
        # Create running task
        running_id = manager.create_task()
        manager.update_task_status(running_id, "running")

        # Create completed tasks
        completed_ids = []
        for i in range(7):
            task_id = manager.create_task()
            completed_ids.append(task_id)
            manager.update_task_status(task_id, "completed")

        manager._cleanup_completed_tasks(keep_count=3)

        # Running task should still exist
        assert manager.get_task(running_id) is not None

        # Only 3 completed should remain
        completed_remaining = sum(
            1 for tid in completed_ids if manager.get_task(tid) is not None
        )
        assert completed_remaining == 3

    @patch('web.backend.services.pipeline_service.PipelineTaskManager._run_pipeline_background')
    def test_create_task_starts_background_thread(self, mock_run, manager):
        """Test that create_task starts a background thread."""
        task_id = manager.create_task()

        # Verify _run_pipeline_background was called
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0]
        assert call_args[0] == task_id

    def test_run_pipeline_background_success(self, manager):
        """Test successful pipeline execution in background."""
        task_id = "test-123"
        manager._tasks[task_id] = PipelineTask(
            task_id=task_id,
            status="pending",
            step="initializing"
        )

        with patch('web.backend.services.pipeline_service.orchestrator_client') as mock_client:
            mock_client.start_matching.return_value = {
                'success': True,
                'task_id': 'orch-456'
            }

            manager._run_pipeline_background(task_id)

            task = manager.get_task(task_id)
            assert task.status == "completed"
            assert task.step == "matching"
            assert task.orchestrator_task_id == 'orch-456'

    def test_run_pipeline_background_orchestrator_failure(self, manager):
        """Test pipeline execution with orchestrator failure."""
        task_id = "test-123"
        manager._tasks[task_id] = PipelineTask(
            task_id=task_id,
            status="pending",
            step="initializing"
        )

        with patch('web.backend.services.pipeline_service.orchestrator_client') as mock_client:
            mock_client.start_matching.return_value = {
                'success': False,
                'message': 'Orchestrator failed'
            }

            manager._run_pipeline_background(task_id)

            task = manager.get_task(task_id)
            assert task.status == "failed"
            assert task.error == 'Orchestrator failed'

    def test_run_pipeline_background_exception(self, manager):
        """Test pipeline execution with exception."""
        task_id = "test-123"
        manager._tasks[task_id] = PipelineTask(
            task_id=task_id,
            status="pending",
            step="initializing"
        )

        with patch('web.backend.services.pipeline_service.orchestrator_client') as mock_client:
            mock_client.start_matching.side_effect = Exception("Database error")

            manager._run_pipeline_background(task_id)

            task = manager.get_task(task_id)
            assert task.status == "failed"
            assert "Database error" in task.error

    def test_run_pipeline_background_task_not_found(self, manager, caplog):
        """Test pipeline execution when task not found."""
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
