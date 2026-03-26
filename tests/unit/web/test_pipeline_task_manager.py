#!/usr/bin/env python3
"""
Unit tests for matching pipeline task state transitions.
"""

import asyncio
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from pipeline.runner import MatchingPipelineResult
from web.backend.services.pipeline_service import (
    PipelineTask,
    PipelineTaskManager,
    get_pipeline_manager,
)


class TestPipelineTaskManager(unittest.TestCase):
    """Tests truthful cancellation state handling."""

    def setUp(self):
        self.manager = PipelineTaskManager()

    def test_stop_active_task_marks_cancellation_requested_before_persisting(self):
        task = PipelineTask(
            task_id="task-1",
            task_type="matching",
            status="running",
            step="scoring",
        )
        self.manager._tasks[task.task_id] = task

        stopped = self.manager.stop_active_task()

        self.assertIsNotNone(stopped)
        self.assertEqual(stopped.status, "cancellation_requested")
        self.assertTrue(stopped.stop_event.is_set())
        self.assertTrue(stopped.cancellation_requested)

    def test_stop_active_task_reports_persisting_after_save_boundary(self):
        task = PipelineTask(
            task_id="task-2",
            task_type="matching",
            status="running",
            step="saving_results",
            persistence_started=True,
        )
        self.manager._tasks[task.task_id] = task

        stopped = self.manager.stop_active_task()

        self.assertIsNotNone(stopped)
        self.assertEqual(stopped.status, "persisting")
        self.assertFalse(stopped.stop_event.is_set())

    def test_create_resume_task_does_not_reuse_matching_task(self):
        matching = PipelineTask(
            task_id="task-3",
            task_type="matching",
            status="running",
            step="vector_matching",
        )
        self.manager._tasks[matching.task_id] = matching

        resume_task_id = self.manager.create_resume_task()
        resume_task = self.manager.get_task(resume_task_id)

        self.assertIsNotNone(resume_task)
        self.assertEqual(resume_task.task_type, "resume_etl")
        self.assertNotEqual(resume_task_id, matching.task_id)

    def test_subscribe_and_unsubscribe_manage_event_queue_lifecycle(self):
        queue = self.manager.subscribe("task-4")

        self.assertIsInstance(queue, asyncio.Queue)
        self.assertIn("task-4", self.manager._event_queues)

        self.manager.unsubscribe("task-4")

        self.assertNotIn("task-4", self.manager._event_queues)

    def test_request_stop_returns_none_for_unknown_task(self):
        self.assertIsNone(self.manager.request_stop("missing-task"))

    def test_request_stop_returns_terminal_status_unchanged(self):
        task = PipelineTask(
            task_id="task-5",
            task_type="matching",
            status="completed",
        )
        self.manager._tasks[task.task_id] = task

        status = self.manager.request_stop(task.task_id)

        self.assertEqual(status, "completed")
        self.assertFalse(task.stop_event.is_set())

    def test_request_stop_reports_persisting_when_save_already_started(self):
        task = PipelineTask(
            task_id="task-6",
            task_type="matching",
            status="running",
            step="saving_results",
            persistence_started=True,
        )
        self.manager._tasks[task.task_id] = task

        status = self.manager.request_stop(task.task_id)

        self.assertEqual(status, "persisting")
        self.assertEqual(task.status, "persisting")
        self.assertFalse(task.stop_event.is_set())

    def test_request_stop_marks_cancellation_requested_for_running_task(self):
        task = PipelineTask(
            task_id="task-6b",
            task_type="matching",
            status="running",
            step="vector_matching",
        )
        self.manager._tasks[task.task_id] = task

        status = self.manager.request_stop(task.task_id)

        self.assertEqual(status, "cancellation_requested")
        self.assertTrue(task.cancellation_requested)
        self.assertTrue(task.stop_event.is_set())

    def test_update_task_status_tracks_persistence_and_cancellation_flags(self):
        task = PipelineTask(
            task_id="task-7",
            task_type="matching",
            status="pending",
        )
        self.manager._tasks[task.task_id] = task

        self.manager.update_task_status(task.task_id, "cancellation_requested", step="scoring")
        self.manager.update_task_status(task.task_id, "persisting", step="saving_results")

        self.assertTrue(task.cancellation_requested)
        self.assertTrue(task.persistence_started)
        self.assertEqual(task.step, "saving_results")

    @patch("web.backend.services.pipeline_service.threading.Thread")
    def test_create_matching_task_reuses_existing_matching_task(self, mock_thread_cls):
        existing = PipelineTask(
            task_id="task-8",
            task_type="matching",
            status="cancellation_requested",
        )
        self.manager._tasks[existing.task_id] = existing

        task_id = self.manager.create_matching_task()

        self.assertEqual(task_id, "task-8")
        mock_thread_cls.assert_not_called()

    @patch("web.backend.services.pipeline_service.threading.Thread")
    def test_create_matching_task_starts_thread_for_new_task(self, mock_thread_cls):
        thread = MagicMock()
        mock_thread_cls.return_value = thread

        task_id = self.manager.create_matching_task()

        self.assertEqual(self.manager.get_task(task_id).task_type, "matching")
        thread.start.assert_called_once_with()

    def test_get_active_task_returns_matching_task_only(self):
        self.manager._tasks["resume"] = PipelineTask(
            task_id="resume",
            task_type="resume_etl",
            status="running",
        )
        active = PipelineTask(
            task_id="match",
            task_type="matching",
            status="running",
        )
        self.manager._tasks[active.task_id] = active

        self.assertIs(self.manager.get_active_task(), active)

    def test_cleanup_completed_tasks_keeps_only_most_recent_terminal_entries(self):
        now = datetime.now()
        for idx in range(4):
            self.manager._tasks[f"done-{idx}"] = PipelineTask(
                task_id=f"done-{idx}",
                task_type="matching",
                status="completed",
                created_at=now - timedelta(minutes=idx),
            )

        self.manager._cleanup_completed_tasks(keep_count=2)

        self.assertIn("done-0", self.manager._tasks)
        self.assertIn("done-1", self.manager._tasks)
        self.assertNotIn("done-2", self.manager._tasks)
        self.assertNotIn("done-3", self.manager._tasks)

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_marks_persisting_then_completed(
        self,
        _mock_load_config,
        mock_build,
        mock_resume_etl,
        mock_run_matching,
    ):
        task = PipelineTask(
            task_id="task-9",
            task_type="matching",
            status="pending",
        )
        self.manager._tasks[task.task_id] = task
        mock_build.return_value = MagicMock()

        def _run(_ctx, _stop_event, status_callback):
            status_callback("saving_results")
            return MatchingPipelineResult(
                success=True,
                matches_count=1,
                saved_count=1,
                notified_count=0,
            )

        mock_run_matching.side_effect = _run

        self.manager._run_pipeline_background(task.task_id)

        self.assertEqual(task.status, "completed")
        self.assertTrue(task.persistence_started)
        mock_resume_etl.assert_called_once()

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_keeps_cancellation_requested_status_during_callback(
        self,
        _mock_load_config,
        mock_build,
        _mock_resume_etl,
        mock_run_matching,
    ):
        task = PipelineTask(
            task_id="task-9b",
            task_type="matching",
            status="pending",
            cancellation_requested=True,
        )
        self.manager._tasks[task.task_id] = task
        mock_build.return_value = MagicMock()

        def _run(_ctx, _stop_event, status_callback):
            status_callback("vector_matching")
            return MatchingPipelineResult(
                success=True,
                matches_count=0,
                saved_count=0,
                notified_count=0,
            )

        mock_run_matching.side_effect = _run

        self.manager._run_pipeline_background(task.task_id)

        self.assertEqual(task.status, "completed")

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_updates_running_status_for_normal_callback(
        self,
        _mock_load_config,
        mock_build,
        _mock_resume_etl,
        mock_run_matching,
    ):
        task = PipelineTask(
            task_id="task-9c",
            task_type="matching",
            status="pending",
        )
        self.manager._tasks[task.task_id] = task
        mock_build.return_value = MagicMock()

        def _run(_ctx, _stop_event, status_callback):
            status_callback("vector_matching")
            return MatchingPipelineResult(
                success=True,
                matches_count=0,
                saved_count=0,
                notified_count=0,
            )

        mock_run_matching.side_effect = _run

        self.manager._run_pipeline_background(task.task_id)

        self.assertEqual(task.step, "vector_matching")

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_maps_cancelled_result(
        self,
        _mock_load_config,
        mock_build,
        _mock_resume_etl,
        mock_run_matching,
    ):
        task = PipelineTask(
            task_id="task-10",
            task_type="matching",
            status="pending",
        )
        self.manager._tasks[task.task_id] = task
        mock_build.return_value = MagicMock()
        mock_run_matching.return_value = MatchingPipelineResult(
            success=False,
            matches_count=0,
            saved_count=0,
            notified_count=0,
            error="Cancelled by user",
            cancelled=True,
        )

        self.manager._run_pipeline_background(task.task_id)

        self.assertEqual(task.status, "cancelled")
        self.assertEqual(task.error, "Cancelled by user")

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_cancels_if_resume_etl_sets_stop_event(
        self,
        _mock_load_config,
        mock_build,
        mock_resume_etl,
        mock_run_matching,
    ):
        task = PipelineTask(
            task_id="task-11",
            task_type="matching",
            status="pending",
        )
        self.manager._tasks[task.task_id] = task
        mock_build.return_value = MagicMock()

        def _resume_etl(_ctx, stop_event):
            stop_event.set()

        mock_resume_etl.side_effect = _resume_etl

        self.manager._run_pipeline_background(task.task_id)

        self.assertEqual(task.status, "cancelled")
        mock_run_matching.assert_not_called()

    @patch("web.backend.services.pipeline_service.run_matching_pipeline")
    @patch("web.backend.services.pipeline_service.run_resume_etl")
    @patch("web.backend.services.pipeline_service.AppContext.build")
    @patch("web.backend.services.pipeline_service.load_config")
    def test_run_pipeline_background_maps_failed_result(
        self,
        _mock_load_config,
        mock_build,
        _mock_resume_etl,
        mock_run_matching,
    ):
        task = PipelineTask(
            task_id="task-12",
            task_type="matching",
            status="pending",
        )
        self.manager._tasks[task.task_id] = task
        mock_build.return_value = MagicMock()
        mock_run_matching.return_value = MatchingPipelineResult(
            success=False,
            matches_count=0,
            saved_count=0,
            notified_count=0,
            error="boom",
            cancelled=False,
        )

        self.manager._run_pipeline_background(task.task_id)

        self.assertEqual(task.status, "failed")
        self.assertEqual(task.error, "boom")


class TestGetPipelineManager(unittest.TestCase):
    def test_returns_singleton(self):
        self.assertIs(get_pipeline_manager(), get_pipeline_manager())

    def test_returns_pipeline_task_manager(self):
        self.assertIsInstance(get_pipeline_manager(), PipelineTaskManager)


if __name__ == "__main__":
    unittest.main()
