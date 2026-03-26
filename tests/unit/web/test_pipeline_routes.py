#!/usr/bin/env python3
"""Unit tests for pipeline router lifecycle edge cases."""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


class TestPipelineRoutes(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from web.backend.routers.pipeline import limiter, router

        limiter.enabled = False
        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    @patch("web.backend.routers.pipeline.enqueue_job")
    @patch("web.backend.routers.pipeline.get_redis_client", return_value=None)
    @patch("web.backend.routers.pipeline.job_uow")
    def test_run_matching_starts_when_ready_resume_exists(
        self,
        mock_uow,
        _mock_redis,
        mock_enqueue,
    ):
        repo = MagicMock()
        repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
        repo.get_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
        mock_uow.return_value.__exit__ = MagicMock(return_value=False)

        response = self.client.post("/api/pipeline/run-matching")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertTrue(data["task_id"])
        mock_enqueue.assert_called_once()

    @patch("web.backend.routers.pipeline.get_redis_client", return_value=None)
    @patch("web.backend.routers.pipeline.job_uow")
    def test_run_matching_reports_resume_processing_state(
        self,
        mock_uow,
        _mock_redis,
    ):
        repo = MagicMock()
        repo.get_latest_ready_resume_fingerprint.return_value = None
        repo.resume.get_latest_resume_processing_state.return_value = SimpleNamespace(
            processing_status="embedding"
        )
        mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
        mock_uow.return_value.__exit__ = MagicMock(return_value=False)

        response = self.client.post("/api/pipeline/run-matching")

        self.assertEqual(response.status_code, 400)
        self.assertIn("still processing (embedding)", response.json()["detail"])

    @patch("web.backend.routers.pipeline.set_task_state")
    @patch("web.backend.routers.pipeline.get_task_state")
    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_stop_matching_marks_active_redis_task_cancelled(
        self,
        mock_get_redis,
        mock_get_task_state,
        mock_set_task_state,
    ):
        redis = MagicMock()
        redis.get.return_value = b"task-2"
        mock_get_redis.return_value = redis
        mock_get_task_state.return_value = {"status": "running", "step": "scoring"}

        response = self.client.post("/api/pipeline/stop")

        self.assertEqual(response.status_code, 200)
        self.assertIn("cancellation requested", response.json()["message"].lower())
        mock_set_task_state.assert_called_once_with(
            "task-2",
            {"status": "cancelled", "step": "scoring"},
            ttl=3600,
        )

    @patch("web.backend.routers.pipeline.get_task_state")
    def test_pipeline_events_streams_terminal_redis_status(self, mock_get_task_state):
        mock_get_task_state.return_value = {
            "status": "cancelled",
            "step": "scoring",
            "result": {
                "matches_count": 2,
                "saved_count": 1,
                "execution_time": 1.2,
            },
            "error": "Cancelled by user",
        }

        with self.client.stream("GET", "/api/pipeline/events/task-3") as response:
            body = b"".join(response.iter_raw()).decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn("data: ", body)
        payload = json.loads(body.split("data: ", 1)[1].split("\n", 1)[0].strip())
        self.assertEqual(payload["status"], "cancelled")
        self.assertEqual(payload["saved_count"], 1)

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
