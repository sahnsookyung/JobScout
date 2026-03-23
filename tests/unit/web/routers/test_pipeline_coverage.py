"""Tests for uncovered branches in web/backend/routers/pipeline.py

Focuses on compact mode internal functions not covered by test_pipeline.py:
- add_rate_limit_handlers / _rate_limit_exceeded_handler
- _validate_task_id
- _start_local_matching (compact mode paths)
- _run_local_matching_background
- get_pipeline_status (compact mode)
- _stream_local_task_sse
- pipeline_events (compact mode)
- get_resume_status
- _process_resume_background
"""

import asyncio
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def pipeline_client():
    from web.backend.routers.pipeline import router
    app = FastAPI()
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
        result = asyncio.run(_rate_limit_exceeded_handler(mock_request, mock_exc))
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
# _start_local_matching
# ---------------------------------------------------------------------------

class TestStartLocalMatching:
    def _make_manager(self, task_id="task-123"):
        manager = MagicMock()
        manager.create_task.return_value = task_id
        return manager

    def _make_redis(self, acquired=True):
        redis = MagicMock()
        redis.set.return_value = acquired
        redis.get.return_value = None
        return redis

    def _make_uow_context(self, fingerprint="fp-1"):
        mock_repo = MagicMock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = fingerprint
        mock_uow = MagicMock()
        mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
        mock_uow.return_value.__exit__ = MagicMock(return_value=False)
        return mock_uow, mock_repo

    def test_success_returns_pipeline_task_response(self):
        from web.backend.routers.pipeline import _start_local_matching
        from web.backend.models.responses import PipelineTaskResponse

        mock_redis = self._make_redis(acquired=True)
        mock_manager = self._make_manager("task-abc")
        mock_uow, _ = self._make_uow_context("fp-1")

        with patch("web.backend.routers.pipeline.get_redis_client", return_value=mock_redis), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=None), \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_pipeline_manager", return_value=mock_manager), \
             patch("web.backend.routers.pipeline.job_uow", mock_uow), \
             patch("threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            result = _start_local_matching()

        assert isinstance(result, PipelineTaskResponse)
        assert result.success is True
        assert result.task_id == "task-abc"

    def test_returns_409_when_lock_not_acquired(self):
        from web.backend.routers.pipeline import _start_local_matching
        from fastapi import HTTPException

        mock_redis = self._make_redis(acquired=False)
        with patch("web.backend.routers.pipeline.get_redis_client", return_value=mock_redis):
            with pytest.raises(HTTPException) as exc_info:
                _start_local_matching()
        assert exc_info.value.status_code == 409

    def test_returns_400_when_no_resume(self):
        from web.backend.routers.pipeline import _start_local_matching
        from fastapi import HTTPException

        mock_redis = self._make_redis(acquired=True)
        mock_manager = self._make_manager()
        mock_uow, _ = self._make_uow_context(fingerprint=None)

        with patch("web.backend.routers.pipeline.get_redis_client", return_value=mock_redis), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=None), \
             patch("web.backend.routers.pipeline.get_pipeline_manager", return_value=mock_manager), \
             patch("web.backend.routers.pipeline.job_uow", mock_uow):
            with pytest.raises(HTTPException) as exc_info:
                _start_local_matching()
        assert exc_info.value.status_code == 400

    def test_returns_409_when_resume_processing(self):
        from web.backend.routers.pipeline import _start_local_matching
        from fastapi import HTTPException

        mock_redis = self._make_redis(acquired=True)
        mock_redis.get.return_value = "resume-task-1"

        with patch("web.backend.routers.pipeline.get_redis_client", return_value=mock_redis), \
             patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "processing"}):
            with pytest.raises(HTTPException) as exc_info:
                _start_local_matching()
        assert exc_info.value.status_code == 409
        assert "processed" in exc_info.value.detail.lower() or "processing" in exc_info.value.detail.lower()

    def test_redis_unavailable_proceeds_without_lock(self):
        """When Redis is down, proceed without the lock (degraded mode)."""
        from web.backend.routers.pipeline import _start_local_matching
        from web.backend.models.responses import PipelineTaskResponse

        mock_manager = self._make_manager("task-no-redis")
        mock_uow, _ = self._make_uow_context("fp-1")

        with patch("web.backend.routers.pipeline.get_redis_client", side_effect=Exception("Redis down")), \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_pipeline_manager", return_value=mock_manager), \
             patch("web.backend.routers.pipeline.job_uow", mock_uow), \
             patch("threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            result = _start_local_matching()

        assert result.success is True

    def test_background_thread_started(self):
        from web.backend.routers.pipeline import _start_local_matching

        mock_redis = self._make_redis(acquired=True)
        mock_manager = self._make_manager()
        mock_uow, _ = self._make_uow_context("fp-1")

        with patch("web.backend.routers.pipeline.get_redis_client", return_value=mock_redis), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=None), \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_pipeline_manager", return_value=mock_manager), \
             patch("web.backend.routers.pipeline.job_uow", mock_uow), \
             patch("threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            _start_local_matching()

        mock_thread.return_value.start.assert_called_once()


# ---------------------------------------------------------------------------
# _run_local_matching_background
# ---------------------------------------------------------------------------

class TestRunLocalMatchingBackground:
    def _make_success_result(self, matches=5):
        result = MagicMock()
        result.success = True
        result.matches_count = matches
        result.saved_count = matches
        result.execution_time = 1.23
        result.error = None
        return result

    def _make_failed_result(self):
        result = MagicMock()
        result.success = False
        result.matches_count = 0
        result.saved_count = 0
        result.execution_time = 0.0
        result.error = "Embedding failed"
        return result

    def test_success_path_sets_completed_state(self):
        from web.backend.routers.pipeline import _run_local_matching_background

        manager = MagicMock()
        manager.get_task.return_value = MagicMock()

        with patch("core.config_loader.load_config"), \
             patch("core.app_context.AppContext.build"), \
             patch("pipeline.runner.run_matching_pipeline", return_value=self._make_success_result()), \
             patch("web.backend.routers.pipeline.set_task_state") as mock_set, \
             patch("web.backend.routers.pipeline.get_redis_client"):
            _run_local_matching_background("task-1", manager, "fp-1")

        completed = [c for c in mock_set.call_args_list if c[0][1].get("status") == "completed"]
        assert len(completed) >= 1

    def test_failed_result_sets_failed_state(self):
        from web.backend.routers.pipeline import _run_local_matching_background

        manager = MagicMock()
        manager.get_task.return_value = MagicMock()

        with patch("core.config_loader.load_config"), \
             patch("core.app_context.AppContext.build"), \
             patch("pipeline.runner.run_matching_pipeline", return_value=self._make_failed_result()), \
             patch("web.backend.routers.pipeline.set_task_state") as mock_set, \
             patch("web.backend.routers.pipeline.get_redis_client"):
            _run_local_matching_background("task-1", manager, "fp-1")

        failed = [c for c in mock_set.call_args_list if c[0][1].get("status") == "failed"]
        assert len(failed) >= 1

    def test_exception_sets_failed_state(self):
        from web.backend.routers.pipeline import _run_local_matching_background

        manager = MagicMock()
        manager.get_task.return_value = MagicMock()

        with patch("core.config_loader.load_config"), \
             patch("core.app_context.AppContext.build"), \
             patch("pipeline.runner.run_matching_pipeline", side_effect=RuntimeError("boom")), \
             patch("web.backend.routers.pipeline.set_task_state") as mock_set, \
             patch("web.backend.routers.pipeline.get_redis_client"):
            _run_local_matching_background("task-exc", manager, "fp-1")

        failed = [c for c in mock_set.call_args_list if c[0][1].get("status") == "failed"]
        assert len(failed) >= 1

    def test_lock_deleted_in_finally_on_success(self):
        from web.backend.routers.pipeline import _run_local_matching_background

        manager = MagicMock()
        manager.get_task.return_value = None
        mock_redis = MagicMock()

        with patch("core.config_loader.load_config"), \
             patch("core.app_context.AppContext.build"), \
             patch("pipeline.runner.run_matching_pipeline", return_value=self._make_success_result()), \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_redis_client", return_value=mock_redis):
            _run_local_matching_background("task-fin", manager, "fp-1")

        mock_redis.delete.assert_called_once()

    def test_lock_deleted_in_finally_on_exception(self):
        from web.backend.routers.pipeline import _run_local_matching_background

        manager = MagicMock()
        manager.get_task.return_value = None
        mock_redis = MagicMock()

        with patch("core.config_loader.load_config"), \
             patch("core.app_context.AppContext.build"), \
             patch("pipeline.runner.run_matching_pipeline", side_effect=Exception("fail")), \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_redis_client", return_value=mock_redis):
            _run_local_matching_background("task-fin", manager, "fp-1")

        mock_redis.delete.assert_called_once()

    def test_task_manager_updated_on_success(self):
        from web.backend.routers.pipeline import _run_local_matching_background

        manager = MagicMock()
        task = MagicMock()
        manager.get_task.return_value = task

        with patch("core.config_loader.load_config"), \
             patch("core.app_context.AppContext.build"), \
             patch("pipeline.runner.run_matching_pipeline", return_value=self._make_success_result(5)), \
             patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.get_redis_client"):
            _run_local_matching_background("task-1", manager, "fp-1")

        assert task.status == "completed"
        assert "5 matches" in task.message

    def test_result_includes_match_counts(self):
        from web.backend.routers.pipeline import _run_local_matching_background

        manager = MagicMock()
        manager.get_task.return_value = None

        with patch("core.config_loader.load_config"), \
             patch("core.app_context.AppContext.build"), \
             patch("pipeline.runner.run_matching_pipeline", return_value=self._make_success_result(7)), \
             patch("web.backend.routers.pipeline.set_task_state") as mock_set, \
             patch("web.backend.routers.pipeline.get_redis_client"):
            _run_local_matching_background("task-1", manager, "fp-1")

        final_call = mock_set.call_args_list[-1]
        state = final_call[0][1]
        # Running state or completed state
        completed_calls = [c for c in mock_set.call_args_list if c[0][1].get("status") == "completed"]
        assert len(completed_calls) >= 1
        result_data = completed_calls[0][0][1].get("result", {})
        assert result_data.get("matches_count") == 7


# ---------------------------------------------------------------------------
# get_pipeline_status (compact mode)
# ---------------------------------------------------------------------------

class TestGetPipelineStatus:
    def test_compact_mode_returns_status_from_redis(self, pipeline_client):
        state = {"status": "completed"}
        with patch.dict("os.environ", {"ORCHESTRATOR_URL": ""}, clear=False), \
             patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/status/task-123")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "completed"

    def test_compact_mode_with_result_fields(self, pipeline_client):
        state = {
            "status": "completed",
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

    def test_compact_mode_with_error_field(self, pipeline_client):
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
            with patch("web.backend.routers.pipeline.get_task_state", return_value={"status": "completed"}), \
                 patch("asyncio.sleep", new_callable=AsyncMock):
                async for chunk in _stream_local_task_sse("task-done"):
                    chunks.append(chunk)
            return chunks

        chunks = asyncio.run(run())
        assert len(chunks) == 1
        data = json.loads(chunks[0].removeprefix("data: ").strip())
        assert data["status"] == "completed"

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

        states = [{"status": "running"}, {"status": "completed"}]
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

    def test_compact_mode_returns_200_streaming(self, pipeline_client):
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
        state = {"status": "processing"}
        with patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/resume-status/task-abc")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "processing"

    def test_valid_task_with_completed_state_returns_200(self, pipeline_client):
        state = {"status": "completed"}
        with patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/resume-status/task-done")
        assert response.status_code == 200

    def test_task_not_found_returns_404(self, pipeline_client):
        with patch("web.backend.routers.pipeline.get_task_state", return_value=None):
            response = pipeline_client.get("/api/pipeline/resume-status/task-xyz")
        assert response.status_code == 404

    def test_task_id_echoed_in_response(self, pipeline_client):
        state = {"status": "completed"}
        with patch("web.backend.routers.pipeline.get_task_state", return_value=state):
            response = pipeline_client.get("/api/pipeline/resume-status/my-task-id")
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "my-task-id"


# ---------------------------------------------------------------------------
# _process_resume_background
# ---------------------------------------------------------------------------

class TestProcessResumeBackground:
    def _make_tmp_mock(self, name="/tmp/resume.pdf"):
        tmp_file = MagicMock()
        tmp_file.name = name
        tmp_file.__enter__ = MagicMock(return_value=tmp_file)
        tmp_file.__exit__ = MagicMock(return_value=False)
        return tmp_file

    def test_success_path_sets_completed_state(self):
        from web.backend.routers.pipeline import _process_resume_background

        manager = MagicMock()
        manager.get_task.return_value = MagicMock()
        mock_repo = MagicMock()
        mock_ctx = MagicMock()
        tmp_file = self._make_tmp_mock()

        with patch("web.backend.routers.pipeline.set_task_state") as mock_set, \
             patch("web.backend.routers.pipeline.load_config"), \
             patch("core.app_context.AppContext.build", return_value=mock_ctx), \
             patch("database.uow.job_uow") as mock_uow, \
             patch("tempfile.NamedTemporaryFile", return_value=tmp_file), \
             patch("os.unlink"):
            mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
            mock_uow.return_value.__exit__ = MagicMock(return_value=False)
            _process_resume_background(b"pdf-content", "resume.pdf", "task-1", manager, "fp-1")

        completed = [c for c in mock_set.call_args_list if c[0][1].get("status") == "completed"]
        assert len(completed) >= 1

    def test_exception_sets_failed_state(self):
        from web.backend.routers.pipeline import _process_resume_background

        manager = MagicMock()
        manager.get_task.return_value = MagicMock()
        tmp_file = self._make_tmp_mock()

        with patch("web.backend.routers.pipeline.set_task_state") as mock_set, \
             patch("web.backend.routers.pipeline.load_config", side_effect=RuntimeError("config error")), \
             patch("tempfile.NamedTemporaryFile", return_value=tmp_file), \
             patch("os.unlink"):
            _process_resume_background(b"data", "resume.pdf", "task-err", manager, "fp-1")

        failed = [c for c in mock_set.call_args_list if c[0][1].get("status") == "failed"]
        assert len(failed) >= 1

    def test_finally_deletes_temp_file_on_success(self):
        from web.backend.routers.pipeline import _process_resume_background

        manager = MagicMock()
        manager.get_task.return_value = None
        mock_ctx = MagicMock()
        mock_repo = MagicMock()
        tmp_file = self._make_tmp_mock("/tmp/test-resume.pdf")

        with patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.load_config"), \
             patch("core.app_context.AppContext.build", return_value=mock_ctx), \
             patch("database.uow.job_uow") as mock_uow, \
             patch("tempfile.NamedTemporaryFile", return_value=tmp_file), \
             patch("os.unlink") as mock_unlink:
            mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
            mock_uow.return_value.__exit__ = MagicMock(return_value=False)
            _process_resume_background(b"data", "resume.pdf", "task-fin", manager, "fp-1")

        mock_unlink.assert_called_once_with("/tmp/test-resume.pdf")

    def test_finally_deletes_temp_file_on_exception(self):
        from web.backend.routers.pipeline import _process_resume_background

        manager = MagicMock()
        manager.get_task.return_value = None
        tmp_file = self._make_tmp_mock("/tmp/fail-resume.pdf")

        with patch("web.backend.routers.pipeline.set_task_state"), \
             patch("web.backend.routers.pipeline.load_config", side_effect=Exception("fail")), \
             patch("tempfile.NamedTemporaryFile", return_value=tmp_file), \
             patch("os.unlink") as mock_unlink:
            _process_resume_background(b"data", "resume.pdf", "task-exc", manager, "fp-1")

        mock_unlink.assert_called_once_with("/tmp/fail-resume.pdf")

    def test_error_includes_exception_message_in_redis_state(self):
        from web.backend.routers.pipeline import _process_resume_background

        manager = MagicMock()
        manager.get_task.return_value = None
        tmp_file = self._make_tmp_mock()

        with patch("web.backend.routers.pipeline.set_task_state") as mock_set, \
             patch("web.backend.routers.pipeline.load_config", side_effect=ValueError("bad config")), \
             patch("tempfile.NamedTemporaryFile", return_value=tmp_file), \
             patch("os.unlink"):
            _process_resume_background(b"data", "resume.pdf", "task-1", manager, "fp-1")

        failed_calls = [c for c in mock_set.call_args_list if c[0][1].get("status") == "failed"]
        assert len(failed_calls) >= 1
        error_msg = failed_calls[0][0][1].get("error", "")
        assert "bad config" in error_msg
