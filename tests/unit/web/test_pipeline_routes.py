#!/usr/bin/env python3
"""Unit tests for pipeline router lifecycle edge cases."""

import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID


class TestPipelineRoutes(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from web.backend.dependencies import get_current_user
        from web.backend.routers.pipeline import limiter, router

        limiter.enabled = False
        self.app = FastAPI()
        self.app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
            id=UUID("00000000-0000-0000-0000-000000000001")
        )
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    @patch("web.backend.routers.pipeline.enqueue_job")
    @patch("web.backend.routers.pipeline.evaluate_resume_eligibility")
    @patch("web.backend.routers.pipeline.get_redis_client", return_value=None)
    def test_run_matching_starts_when_ready_resume_exists(
        self,
        _mock_redis,
        mock_eligibility,
        mock_enqueue,
    ):
        mock_eligibility.return_value = SimpleNamespace(
            can_run=True,
            upload_id="upload-1",
            resume_fingerprint="fp-ready",
        )

        response = self.client.post("/api/pipeline/run-matching")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertTrue(data["task_id"])
        mock_enqueue.assert_called_once()

    @patch("web.backend.routers.pipeline.evaluate_resume_eligibility")
    @patch("web.backend.routers.pipeline.get_redis_client", return_value=None)
    def test_run_matching_reports_resume_processing_state(
        self,
        _mock_redis,
        mock_eligibility,
    ):
        mock_eligibility.return_value = SimpleNamespace(
            can_run=False,
            processing_status="embedding",
            message="Latest uploaded resume is still processing (embedding).",
        )

        response = self.client.post("/api/pipeline/run-matching")

        self.assertEqual(response.status_code, 409)
        self.assertIn("still processing (embedding)", response.json()["detail"])

    @patch("web.backend.routers.pipeline.evaluate_resume_eligibility")
    def test_resume_eligibility_endpoint_returns_authoritative_status(self, mock_eligibility):
        mock_eligibility.return_value = SimpleNamespace(
            can_run=False,
            processing_status="embedding",
            message="Latest uploaded resume is still processing (embedding).",
            retryable=True,
            upload_id="upload-1",
            resume_hash="hash-1",
            resume_fingerprint="fp-1",
            processing_task_id="task-1",
        )

        response = self.client.get("/api/pipeline/resume-eligibility")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertFalse(data["can_run"])
        self.assertEqual(data["status"], "embedding")
        self.assertEqual(data["task_id"], "task-1")

    @patch("web.backend.routers.pipeline.set_task_cancellation_requested")
    @patch("web.backend.routers.pipeline.set_task_state")
    @patch("web.backend.routers.pipeline.get_task_state")
    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_stop_matching_marks_active_redis_task_cancelled(
        self,
        mock_get_redis,
        mock_get_task_state,
        mock_set_task_state,
        mock_set_cancel_requested,
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
            {"status": "cancellation_requested", "step": "scoring"},
            ttl=3600,
        )
        mock_set_cancel_requested.assert_called_once_with("task-2", ttl=3600)

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

    @patch("web.backend.routers.pipeline.get_task_state")
    def test_pipeline_status_includes_stale_result_metadata(self, mock_get_task_state):
        mock_get_task_state.return_value = {
            "status": "completed",
            "step": "notifying",
            "upload_id": "upload-old",
            "resume_fingerprint": "fp-old",
            "stale_due_to_newer_upload": True,
            "latest_upload_id": "upload-new",
            "latest_resume_fingerprint": "fp-new",
            "stale_message": "These results were generated from an older resume upload.",
            "result": {
                "matches_count": 4,
                "saved_count": 3,
                "notified_count": 1,
                "execution_time": 2.5,
            },
        }

        response = self.client.get("/api/pipeline/status/task-stale")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["stale_due_to_newer_upload"])
        self.assertEqual(data["latest_upload_id"], "upload-new")
        self.assertEqual(data["resume_fingerprint"], "fp-old")

    @patch("web.backend.routers.pipeline.get_task_state")
    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_active_pipeline_endpoint_returns_running_task(
        self,
        mock_get_redis,
        mock_get_task_state,
    ):
        redis = MagicMock()
        redis.get.return_value = b"task-active"
        mock_get_redis.return_value = redis
        mock_get_task_state.return_value = {
            "status": "running",
            "step": "matching",
            "upload_id": "upload-1",
            "resume_fingerprint": "fp-1",
        }

        response = self.client.get("/api/pipeline/active")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["task_id"], "task-active")
        self.assertEqual(data["status"], "running")
        self.assertEqual(data["step"], "vector_matching")
        self.assertEqual(data["upload_id"], "upload-1")

    @patch("web.backend.routers.pipeline.get_task_state")
    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_active_pipeline_endpoint_returns_null_for_terminal_task(
        self,
        mock_get_redis,
        mock_get_task_state,
    ):
        redis = MagicMock()
        redis.get.return_value = b"task-done"
        mock_get_redis.return_value = redis
        mock_get_task_state.return_value = {"status": "completed"}

        response = self.client.get("/api/pipeline/active")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json())

    @patch("web.backend.routers.pipeline.evaluate_resume_preflight")
    def test_resume_preflight_endpoint_returns_read_only_status(self, mock_preflight):
        mock_preflight.return_value = SimpleNamespace(
            status="ready_already_known",
            message="Resume already processed and ready for matching.",
            retryable=False,
            can_skip_upload=True,
            resume_hash="hash-1",
            upload_id="upload-1",
            processing_task_id=None,
        )

        response = self.client.post("/api/pipeline/resume-preflight", json={"resume_hash": "hash-1"})

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "ready_already_known")
        self.assertTrue(data["can_skip_upload"])

    @patch("web.backend.routers.pipeline.get_redis_client")
    def test_select_resume_clears_latest_upload_marker(self, mock_get_redis):
        redis = MagicMock()
        mock_get_redis.return_value = redis

        with patch("web.backend.routers.pipeline.job_uow") as mock_uow:
            repo = MagicMock()
            repo.is_resume_ready.return_value = True
            repo.create_resume_upload.return_value = SimpleNamespace(id="upload-selected")
            mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
            mock_uow.return_value.__exit__ = MagicMock(return_value=False)

            response = self.client.post(
                "/api/pipeline/select-resume",
                json={"resume_hash": "hash-old", "original_filename": "resume-old.json"},
            )

        self.assertEqual(response.status_code, 200)
        redis.delete.assert_called_once()

    def test_upload_resume_returns_processing_message_for_existing_in_progress_upload(self):
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
                    repo.get_latest_resume_upload_for_hash.return_value = SimpleNamespace(
                        id="upload-1",
                        processing_task_id="task-1",
                        status="in_progress",
                    )
                    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post("/api/pipeline/upload-resume", files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertEqual(data["task_id"], "task-1")

    @patch("web.backend.routers.pipeline.set_task_state")
    @patch("web.backend.routers.pipeline.get_task_state")
    def test_upload_resume_reconciles_stale_in_progress_upload(
        self,
        mock_get_task_state,
        mock_set_task_state,
    ):
        files = {"file": ("resume.json", '{"name":"Test User"}', "application/json")}
        stale_upload = SimpleNamespace(
            id="upload-1",
            processing_task_id="task-stale",
            status="in_progress",
            created_at=datetime.now(timezone.utc) - timedelta(minutes=30),
            resume_fingerprint="fp-stale",
            resume_hash="hash-stale",
            owner_id=UUID("00000000-0000-0000-0000-000000000001"),
        )
        recovered_upload = SimpleNamespace(
            id="upload-1",
            processing_task_id="task-stale",
            status="failed_reupload_required",
            created_at=stale_upload.created_at,
            resume_fingerprint="fp-stale",
            resume_hash="hash-stale",
            owner_id=stale_upload.owner_id,
        )

        with patch("core.config_loader.load_config") as mock_config:
            cfg = MagicMock()
            cfg.etl = MagicMock()
            cfg.etl.resume = MagicMock()
            cfg.etl.resume.resume_file = "/tmp/resume.json"
            mock_config.return_value = cfg

            with patch("database.models.resume.generate_file_fingerprint", return_value="hash-stale"):
                with patch("database.uow.job_uow") as mock_uow:
                    repo = MagicMock()
                    repo.is_resume_ready.return_value = False
                    repo.get_latest_resume_upload_for_hash.return_value = stale_upload
                    repo.get_resume_processing_state.return_value = None
                    repo.get_structured_resume_by_fingerprint.return_value = None
                    repo.get_resume_upload.return_value = recovered_upload
                    mock_get_task_state.return_value = {"status": "processing", "step": "extracting"}
                    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post("/api/pipeline/upload-resume", files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data["task_id"], "task-stale")
        repo.update_resume_upload.assert_called()
        mock_set_task_state.assert_called()

    @patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://localhost:8084"}, clear=False)
    @patch("web.backend.routers.pipeline._stream_orchestrator_sse")
    @patch("web.backend.routers.pipeline._preflight_task_check", new_callable=AsyncMock)
    @patch("web.backend.routers.pipeline.get_task_state", return_value=None)
    def test_pipeline_events_falls_back_to_orchestrator_stream_when_redis_missing(
        self,
        _mock_get_task_state,
        mock_preflight,
        mock_stream,
    ):
        async def _stream():
            yield b'data: {"status":"running"}\n\n'

        mock_stream.return_value = _stream()

        with self.client.stream("GET", "/api/pipeline/events/task-orchestrator") as response:
            body = b"".join(response.iter_raw()).decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('"status":"running"', body)
        mock_preflight.assert_awaited_once()
        mock_stream.assert_called_once()

    @patch.dict(
        "os.environ",
        {
            "INTERNAL_ORCHESTRATOR_URL": "http://orchestrator:8084",
            "ORCHESTRATOR_URL": "http://localhost:8084",
        },
        clear=False,
    )
    @patch("web.backend.routers.pipeline._stream_orchestrator_sse")
    @patch("web.backend.routers.pipeline._preflight_task_check", new_callable=AsyncMock)
    @patch("web.backend.routers.pipeline.get_task_state", return_value=None)
    def test_pipeline_events_prefers_internal_orchestrator_url_inside_container(
        self,
        _mock_get_task_state,
        mock_preflight,
        mock_stream,
    ):
        async def _stream():
            yield b'data: {"status":"running"}\n\n'

        mock_stream.return_value = _stream()

        with self.client.stream("GET", "/api/pipeline/events/task-orchestrator") as response:
            body = b"".join(response.iter_raw()).decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn('"status":"running"', body)
        mock_preflight.assert_awaited_once_with("http://orchestrator:8084", "task-orchestrator")
        mock_stream.assert_called_once_with("http://orchestrator:8084", "task-orchestrator")

    @patch.dict("os.environ", {"ORCHESTRATOR_URL": "http://localhost:8084"}, clear=False)
    @patch("web.backend.routers.pipeline._preflight_task_check", new_callable=AsyncMock)
    @patch("web.backend.routers.pipeline.get_task_state", return_value=None)
    def test_pipeline_events_returns_404_when_orchestrator_task_is_missing(
        self,
        _mock_get_task_state,
        mock_preflight,
    ):
        from fastapi import HTTPException

        mock_preflight.side_effect = HTTPException(status_code=404, detail="Task not found")

        response = self.client.get("/api/pipeline/events/task-missing")

        self.assertEqual(response.status_code, 404)

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
                    repo.get_latest_resume_upload_for_hash.return_value = None
                    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post("/api/pipeline/upload-resume", files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["success"])
        self.assertIsNone(data["task_id"])
        self.assertTrue(data["upload_id"])
        self.assertIn("ready", data["message"].lower())

    @patch("web.backend.routers.pipeline._retry_resume_background")
    @patch("web.backend.routers.pipeline.get_redis_client")
    @patch("web.backend.routers.pipeline.set_task_state")
    def test_retry_resume_creates_new_upload_attempt(self, mock_set_task_state, mock_get_redis, _mock_retry_background):
        redis = MagicMock()
        mock_get_redis.return_value = redis

        with patch("web.backend.routers.pipeline.job_uow") as mock_uow:
            source_upload = SimpleNamespace(
                id="upload-old",
                status="failed_retryable",
                resume_hash="hash-1",
                resume_fingerprint="fp-1",
                original_filename="resume.pdf",
            )
            retry_upload = SimpleNamespace(id="upload-new")
            repo = MagicMock()
            repo.get_resume_upload.return_value = source_upload
            repo.get_structured_resume_by_fingerprint.return_value = object()
            repo.create_resume_upload.return_value = retry_upload
            mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
            mock_uow.return_value.__exit__ = MagicMock(return_value=False)

            response = self.client.post("/api/pipeline/retry-resume", json={"upload_id": "upload-old"})

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["upload_id"], "upload-new")
        self.assertEqual(data["status"], "in_progress")
        self.assertTrue(data["task_id"])
        repo.create_resume_upload.assert_called_once()
        self.assertGreaterEqual(repo.update_resume_upload.call_count, 1)
        mock_set_task_state.assert_called_once()
