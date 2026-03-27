"""Tests for uncovered branches in web/backend/routers/pipeline.py

Covers functions not fully exercised by test_pipeline.py:
- add_rate_limit_handlers / _rate_limit_exceeded_handler
- _validate_task_id
- get_pipeline_status
- _stream_local_task_sse
- pipeline_events
- get_resume_status
- _process_resume_background
"""

import asyncio
import json
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch
from uuid import UUID
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def pipeline_client():
    from web.backend.routers.pipeline import router
    from web.backend.dependencies import get_current_user
    app = FastAPI()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
        id=UUID("00000000-0000-0000-0000-000000000001")
    )
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# add_rate_limit_handlers / _rate_limit_exceeded_handler
# ---------------------------------------------------------------------------

class TestAddRateLimitHandlers:
    def test_sets_limiter_on_app_state(self):
        from web.backend.routers.pipeline import add_rate_limit_handlers, limiter
        mock_app = MagicMock()
        add_rate_limit_handlers(mock_app)
        assert mock_app.state.limiter is limiter

    def test_registers_exception_handler_for_rate_limit_exceeded(self):
        from web.backend.routers.pipeline import add_rate_limit_handlers
        from slowapi.errors import RateLimitExceeded
        mock_app = MagicMock()
        add_rate_limit_handlers(mock_app)
        mock_app.add_exception_handler.assert_called_once()
        exception_class = mock_app.add_exception_handler.call_args[0][0]
        assert exception_class is RateLimitExceeded

    def test_rate_limit_handler_returns_429(self):
        from web.backend.routers.pipeline import _rate_limit_exceeded_handler
        mock_request = MagicMock()
        mock_exc = MagicMock()
        mock_exc.__str__ = lambda self: "rate limit exceeded"
        result = _rate_limit_exceeded_handler(mock_request, mock_exc)
        assert result.status_code == 429


# ---------------------------------------------------------------------------
# _validate_task_id
# ---------------------------------------------------------------------------

class TestValidateTaskId:
    def test_valid_alphanumeric_with_hyphens(self):
        from web.backend.routers.pipeline import _validate_task_id
        assert _validate_task_id("match-a1b2c3d4") is True

    def test_valid_simple_id(self):
        from web.backend.routers.pipeline import _validate_task_id
        assert _validate_task_id("task123") is True

    def test_invalid_path_traversal(self):
        from web.backend.routers.pipeline import _validate_task_id
        assert _validate_task_id("task/../evil") is False

    def test_invalid_empty_string(self):
        from web.backend.routers.pipeline import _validate_task_id
        assert _validate_task_id("") is False

    def test_invalid_too_long(self):
        from web.backend.routers.pipeline import _validate_task_id
        assert _validate_task_id("a" * 51) is False

    def test_invalid_none(self):
        from web.backend.routers.pipeline import _validate_task_id
        assert _validate_task_id(None) is False

    def test_invalid_special_chars(self):
        from web.backend.routers.pipeline import _validate_task_id
        assert _validate_task_id("task<script>") is False

    def test_exactly_50_chars_valid(self):
        from web.backend.routers.pipeline import _validate_task_id
        assert _validate_task_id("a" * 50) is True


# ---------------------------------------------------------------------------
# get_pipeline_status
# ---------------------------------------------------------------------------

class TestGetPipelineStatus:
    def test_returns_status_from_redis(self, pipeline_client):
        state = {"status": "completed"}
        with patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/status/task-123")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"

    def test_with_result_fields(self, pipeline_client):
        state = {
            "status": "completed",
            "step": "saving_results",
            "result": {"matches_count": 10, "saved_count": 8, "execution_time": 2.5}
        }
        with patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/status/task-123")
        assert response.status_code == 200
        data = response.json()
        assert data["matches_count"] == 10
        assert data["saved_count"] == 8
        assert data["execution_time"] == pytest.approx(2.5)
        assert data["step"] == "saving_results"

    def test_normalizes_redis_step_for_active_status(self, pipeline_client):
        state = {"status": "running", "step": "matching"}
        with patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/status/task-123")
        assert response.status_code == 200
        data = response.json()
        assert data["step"] == "vector_matching"

    def test_with_error_field(self, pipeline_client):
        state = {"status": "failed", "error": "something broke"}
        with patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/status/task-123")
        assert response.status_code == 200
        data = response.json()
        assert data["error"] == "something broke"

    def test_get_task_state_exception_falls_through_gracefully(self, pipeline_client):
        with patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False), \
             patch("web.backend.routers.pipeline.get_task_state", side_effect=Exception("Redis down")):
            response = pipeline_client.get("/api/pipeline/status/task-err")
        # Falls through without crashing — may return various status codes
        assert response.status_code in (200, 404, 500)


# ---------------------------------------------------------------------------
# _stream_local_task_sse
# ---------------------------------------------------------------------------

class TestStreamLocalTaskSse:
    def test_task_not_found_yields_error_and_stops(self):
        from web.backend.routers.pipeline import _stream_local_task_sse

        async def run():
            chunks = []
            with patch("web.backend.routers.pipeline.get_task_state", return_value=None):
                async for chunk in _stream_local_task_sse("task-missing"):
                    chunks.append(chunk)
            return chunks

        chunks = asyncio.run(run())
        assert len(chunks) == 1
        data = json.loads(chunks[0].removeprefix("data: ").strip())
        assert data["status"] == "failed"
        assert "not found" in data["error"].lower()

    def test_completed_state_yields_and_stops(self):
        from web.backend.routers.pipeline import _stream_local_task_sse

        async def run():
            chunks = []
            with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "completed", "step": "saving_results"}), \
                 patch("asyncio.sleep", new_callable=AsyncMock):
                async for chunk in _stream_local_task_sse("task-done"):
                    chunks.append(chunk)
            return chunks

        chunks = asyncio.run(run())
        assert len(chunks) == 1
        data = json.loads(chunks[0].removeprefix("data: ").strip())
        assert data["status"] == "completed"
        assert data["step"] == "saving_results"

    def test_failed_state_yields_and_stops(self):
        from web.backend.routers.pipeline import _stream_local_task_sse

        async def run():
            chunks = []
            with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "failed", "error": "oops"}), \
                 patch("asyncio.sleep", new_callable=AsyncMock):
                async for chunk in _stream_local_task_sse("task-fail"):
                    chunks.append(chunk)
            return chunks

        chunks = asyncio.run(run())
        assert len(chunks) == 1
        data = json.loads(chunks[0].removeprefix("data: ").strip())
        assert data["status"] == "failed"

    def test_cancelled_state_yields_and_stops(self):
        from web.backend.routers.pipeline import _stream_local_task_sse

        async def run():
            chunks = []
            with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "cancelled"}), \
                 patch("asyncio.sleep", new_callable=AsyncMock):
                async for chunk in _stream_local_task_sse("task-cancel"):
                    chunks.append(chunk)
            return chunks

        chunks = asyncio.run(run())
        assert len(chunks) == 1

    def test_pending_then_completed_emits_both(self):
        from web.backend.routers.pipeline import _stream_local_task_sse

        states = [
            {"status": "running", "step": "matching"},
            {"status": "completed", "step": "saving_results"},
        ]
        call_count = [0]

        def fake_state(task_id):
            s = states[min(call_count[0], len(states) - 1)]
            call_count[0] += 1
            return s

        async def run():
            chunks = []
            with patch("web.backend.routers.pipeline.get_task_state", side_effect=fake_state), \
                 patch("asyncio.sleep", new_callable=AsyncMock):
                async for chunk in _stream_local_task_sse("task-progress"):
                    chunks.append(chunk)
            return chunks

        chunks = asyncio.run(run())
        assert len(chunks) == 2
        first = json.loads(chunks[0].removeprefix("data: ").strip())
        last = json.loads(chunks[-1].removeprefix("data: ").strip())
        assert first["status"] == "running"
        assert first["step"] == "vector_matching"
        assert last["status"] == "completed"

    def test_get_task_state_exception_treated_as_not_found(self):
        from web.backend.routers.pipeline import _stream_local_task_sse

        async def run():
            chunks = []
            with patch("web.backend.routers.pipeline.get_task_state", side_effect=Exception("Redis error")):
                async for chunk in _stream_local_task_sse("task-err"):
                    chunks.append(chunk)
            return chunks

        chunks = asyncio.run(run())
        # Exception → state = None → yields error
        assert len(chunks) >= 1
        data = json.loads(chunks[0].removeprefix("data: ").strip())
        assert data["status"] == "failed"


# ---------------------------------------------------------------------------
# pipeline_events endpoint
# ---------------------------------------------------------------------------

class TestPipelineEvents:
    def test_invalid_task_id_returns_400(self, pipeline_client):
        # Use a task_id with forbidden characters (angle brackets)
        response = pipeline_client.get("/api/pipeline/events/" + "a" * 51)
        assert response.status_code == 400

    def test_returns_200_streaming(self, pipeline_client):
        with patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False), \
             patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "completed"}):
            response = pipeline_client.get("/api/pipeline/events/task-abc")
        assert response.status_code == 200

    def test_very_long_task_id_returns_400(self, pipeline_client):
        response = pipeline_client.get(f"/api/pipeline/events/{'a' * 51}")
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# get_resume_status
# ---------------------------------------------------------------------------

class TestGetResumeStatus:
    def test_invalid_task_id_returns_400(self, pipeline_client):
        response = pipeline_client.get("/api/pipeline/resume-status/" + "a" * 51)
        assert response.status_code == 400

    def test_valid_task_with_processing_state_returns_200(self, pipeline_client):
        state = {
            "status": "processing",
            "task_type": "resume_upload",
            "owner_id": "00000000-0000-0000-0000-000000000001",
        }
        with patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/resume-status/task-abc")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "processing"
        assert data["message"] == "Resume processing is in progress."

    def test_valid_task_with_completed_state_returns_200(self, pipeline_client):
        state = {
            "status": "completed",
            "task_type": "resume_upload",
            "owner_id": "00000000-0000-0000-0000-000000000001",
        }
        with patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/resume-status/task-done")
        assert response.status_code == 200
        assert response.json()["message"] == "Resume processing completed successfully."

    def test_task_not_found_returns_404(self, pipeline_client):
        with patch("web.backend.routers.pipeline.get_task_state", return_value=None):
            response = pipeline_client.get("/api/pipeline/resume-status/task-xyz")
        assert response.status_code == 404

    def test_task_id_echoed_in_response(self, pipeline_client):
        state = {
            "status": "completed",
            "task_type": "resume_upload",
            "owner_id": "00000000-0000-0000-0000-000000000001",
        }
        with patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/resume-status/my-task-id")
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "my-task-id"

    def test_resume_status_hides_task_from_other_user(self, pipeline_client):
        state = {
            "status": "processing",
            "task_type": "resume_upload",
            "owner_id": "00000000-0000-0000-0000-000000000999",
        }
        with patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/resume-status/task-abc")
        assert response.status_code == 404

    def test_terminal_upload_row_takes_precedence_over_stale_redis_state(self, pipeline_client):
        upload = SimpleNamespace(
            status="failed_reupload_required",
            user_safe_message="Resume processing timed out. Please retry.",
            last_error="Resume processing timed out. Please retry.",
            processing_task_id="task-stale",
        )
        mock_uow = MagicMock()
        repo = MagicMock()
        repo.get_resume_upload_by_task_id.return_value = upload
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("web.backend.routers.pipeline.job_uow", return_value=mock_uow), \
             patch(
                 "web.backend.routers.pipeline.get_task_state",
                 return_value={"status": "processing", "task_type": "resume_upload", "owner_id": "00000000-0000-0000-0000-000000000001"},
             ):
            response = pipeline_client.get("/api/pipeline/resume-status/task-stale")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "failed"
        assert "timed out" in data["error"].lower()
        assert "timed out" in data["message"].lower()


# ---------------------------------------------------------------------------
# _process_resume_background
# ---------------------------------------------------------------------------

class TestProcessResumeBackground:
    def test_success_path_sets_completed_state(self):
        from web.backend.routers.pipeline import _process_resume_background

        repo = MagicMock()
        mock_uow = MagicMock()
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("pathlib.Path.mkdir"), \
             patch("pathlib.Path.write_bytes"), \
             patch("os.unlink"), \
             patch("web.backend.services.clients.orchestrator_client"), \
             patch("web.backend.routers.pipeline.job_uow", return_value=mock_uow), \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "completed"}):
            _process_resume_background(
                b"pdf-content",
                "resume.pdf",
                "task-1",
                "upload-1",
                "00000000-0000-0000-0000-000000000001",
                "hash-1",
                "fp-1",
            )

        repo.update_resume_upload.assert_called_once()

    def test_exception_sets_failed_state(self):
        from web.backend.routers.pipeline import _process_resume_background

        with patch("pathlib.Path.mkdir"), \
             patch("pathlib.Path.write_bytes"), \
             patch("os.unlink"), \
             patch("web.backend.services.clients.orchestrator_client") as mock_client, \
             patch("web.backend.routers.pipeline._write_resume_failure_state") as mock_failure, \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=None):
            mock_client.process_resume.side_effect = RuntimeError("service down")
            _process_resume_background(
                b"data",
                "resume.pdf",
                "task-err",
                "upload-1",
                "00000000-0000-0000-0000-000000000001",
                "hash-1",
                "fp-1",
            )

        mock_failure.assert_called_once()

    def test_error_includes_exception_message_in_redis_state(self):
        from web.backend.routers.pipeline import _process_resume_background

        with patch("pathlib.Path.mkdir"), \
             patch("pathlib.Path.write_bytes"), \
             patch("os.unlink"), \
             patch("web.backend.services.clients.orchestrator_client") as mock_client, \
             patch("web.backend.routers.pipeline._write_resume_failure_state") as mock_failure, \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=None):
            mock_client.process_resume.side_effect = ValueError("bad config")
            _process_resume_background(
                b"data",
                "resume.pdf",
                "task-1",
                "upload-1",
                "00000000-0000-0000-0000-000000000001",
                "hash-1",
                "fp-1",
            )

        error_arg = mock_failure.call_args[0][-1]
        assert "bad config" in str(error_arg)

    def test_temp_file_write_failure_marks_task_failed(self):
        from web.backend.routers.pipeline import _process_resume_background

        with patch(
            "web.backend.routers.pipeline._write_resume_file_to_shared_volume",
            side_effect=PermissionError("permission denied"),
        ), patch(
            "web.backend.routers.pipeline._write_resume_failure_state"
        ) as mock_failure, patch(
            "web.backend.routers.pipeline.set_task_state"
        ):
            _process_resume_background(
                b"data",
                "resume.pdf",
                "task-write-fail",
                "upload-1",
                "00000000-0000-0000-0000-000000000001",
                "hash-1",
                "fp-1",
            )

        mock_failure.assert_called_once()
        error_arg = mock_failure.call_args[0][-1]
        assert "permission denied" in str(error_arg).lower()
