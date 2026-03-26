#!/usr/bin/env python3
"""Unit tests for pipeline router lifecycle edge cases."""

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pipeline.runner import MatchingPipelineResult


class TestPipelineRoutes(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from web.backend.routers.pipeline import limiter, router

        limiter.enabled = False
        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    @patch("web.backend.routers.pipeline.get_pipeline_manager")
    def test_run_matching_returns_existing_active_task_for_persisting_status(self, mock_get_manager):
        manager = MagicMock()
        manager.create_matching_task.return_value = "task-1"
        manager.get_task.return_value = SimpleNamespace(status="persisting")
        mock_get_manager.return_value = manager

        response = self.client.post("/api/pipeline/run-matching")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("already running", data["message"].lower())

    @patch("web.backend.routers.pipeline.get_pipeline_manager")
    def test_stop_matching_reports_persisting_boundary(self, mock_get_manager):
        manager = MagicMock()
        manager.stop_active_task.return_value = SimpleNamespace(
            task_id="task-2",
            status="persisting",
        )
        mock_get_manager.return_value = manager

        response = self.client.post("/api/pipeline/stop")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("finishing writes", data["message"].lower())

    @patch("web.backend.routers.pipeline.get_pipeline_manager")
    def test_stop_matching_reports_cancellation_requested_before_persisting(self, mock_get_manager):
        manager = MagicMock()
        manager.stop_active_task.return_value = SimpleNamespace(
            task_id="task-2",
            status="cancellation_requested",
        )
        mock_get_manager.return_value = manager

        response = self.client.post("/api/pipeline/stop")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIn("cancellation requested", data["message"].lower())

    @patch("web.backend.routers.pipeline.get_pipeline_manager")
    def test_pipeline_events_treats_cancelled_as_terminal_status(self, mock_get_manager):
        manager = MagicMock()
        task = SimpleNamespace(
            task_id="task-3",
            status="cancelled",
            step="scoring",
            result=MatchingPipelineResult(
                success=False,
                matches_count=2,
                saved_count=1,
                notified_count=0,
                error="Cancelled by user",
                execution_time=1.2,
                cancelled=True,
            ),
            error=None,
        )
        manager.get_task.return_value = task
        manager.subscribe.return_value = asyncio.Queue()
        mock_get_manager.return_value = manager

        with self.client.stream("GET", "/api/pipeline/events/task-3") as response:
            body = b"".join(response.iter_raw()).decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn("data: ", body)
        payload = json.loads(body.split("data: ", 1)[1].split("\n", 1)[0].strip())
        self.assertEqual(payload["status"], "cancelled")
        self.assertFalse(payload["success"])
        manager.unsubscribe.assert_called_once_with("task-3")

    def test_upload_resume_returns_processing_message_for_existing_embedding_state(self):
        files = {"file": ("resume.json", '{"name":"Test User"}', "application/json")}

        with patch("core.config_loader.load_config") as mock_config:
            cfg = MagicMock()
            cfg.etl = MagicMock()
            cfg.etl.resume = MagicMock()
            cfg.etl.resume.resume_file = "/tmp/resume.json"
            mock_config.return_value = cfg

            with patch("database.models.resume.generate_file_fingerprint", return_value="fp-123"):
                with patch("database.uow.job_uow") as mock_uow:
                    repo = MagicMock()
                    repo.is_resume_ready.return_value = False
                    repo.get_resume_processing_state.return_value = SimpleNamespace(
                        processing_status="embedding"
                    )
                    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post("/api/pipeline/upload-resume", files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIsNone(data["task_id"])
        self.assertIn("already processing", data["message"].lower())

    def test_upload_resume_returns_ready_message_for_already_processed_resume(self):
        files = {"file": ("resume.json", '{"name":"Test User"}', "application/json")}

        with patch("core.config_loader.load_config") as mock_config:
            cfg = MagicMock()
            cfg.etl = MagicMock()
            cfg.etl.resume = MagicMock()
            cfg.etl.resume.resume_file = "/tmp/resume.json"
            mock_config.return_value = cfg

            with patch("database.models.resume.generate_file_fingerprint", return_value="fp-ready"):
                with patch("database.uow.job_uow") as mock_uow:
                    repo = MagicMock()
                    repo.is_resume_ready.return_value = True
                    repo.get_resume_processing_state.return_value = None
                    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post("/api/pipeline/upload-resume", files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIsNone(data["task_id"])
        self.assertIn("ready", data["message"].lower())
