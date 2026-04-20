"""
Unit Tests: Scorer-Matcher Service

Tests the scorer-matcher service functionality.

Usage:
    uv run pytest tests/unit/services/test_scorer_matcher.py -v
"""

import asyncio
import pytest
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch, AsyncMock, MagicMock

from fastapi.testclient import TestClient


class TestMatcherState:
    """Test MatcherState class."""

    def test_initialization(self):
        """Test MatcherState initializes correctly."""
        from services.scorer_matcher.main import MatcherState, MatcherConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=MatcherConsumer)
        state = MatcherState(mock_ctx, mock_consumer)

        assert state.ctx is mock_ctx
        assert state.consumer is mock_consumer
        assert isinstance(state.stop_event, type(threading.Event()))
        assert state.consumer_task is None

    def test_stop_event_initially_clear(self):
        """Stop event is initially clear."""
        from services.scorer_matcher.main import MatcherState, MatcherConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=MatcherConsumer)
        state = MatcherState(mock_ctx, mock_consumer)

        assert state.stop_event.is_set() is False

    def test_stop_event_can_be_set(self):
        """Stop event can be set."""
        from services.scorer_matcher.main import MatcherState, MatcherConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=MatcherConsumer)
        state = MatcherState(mock_ctx, mock_consumer)

        state.stop_event.set()
        assert state.stop_event.is_set() is True


class TestMatcherModels:
    """Test matcher Pydantic models."""

    def test_match_response_model(self):
        """Test MatchResponse model."""
        from services.scorer_matcher.main import MatchResponse

        response = MatchResponse(success=True, task_id="t-1", message="Done")
        assert response.success is True
        assert response.task_id == "t-1"
        assert response.message == "Done"
        assert response.matches == 0

    def test_match_resume_request(self):
        """Test MatchResumeRequest model."""
        from services.scorer_matcher.main import MatchResumeRequest

        req = MatchResumeRequest(resume_fingerprint="fp-123")
        assert req.resume_fingerprint == "fp-123"

    def test_match_job_request(self):
        """Test MatchJobRequest model."""
        from services.scorer_matcher.main import MatchJobRequest

        req = MatchJobRequest(job_ids=["job-1", "job-2"])
        assert req.job_ids == ["job-1", "job-2"]


class TestMatcherLogging:
    """Test matcher logging setup."""

    def test_setup_logging(self):
        """Test setup_logging configures logging."""
        from services.scorer_matcher.main import _setup_logging
        # Just verify it runs without error
        _setup_logging()


class TestMatcherWarmUp:
    def test_warm_up_cross_encoder_skips_when_env_flag_set(self, monkeypatch):
        from services.scorer_matcher.main import _warm_up_cross_encoder

        monkeypatch.setenv("MATCHER_SKIP_WARMUP", "true")

        with patch("services.scorer_matcher.main.logger") as mock_logger:
            _warm_up_cross_encoder(Mock())

        mock_logger.info.assert_called_once_with(
            "Cross-encoder warm-up skipped via MATCHER_SKIP_WARMUP"
        )


class TestMatcherHelpers:
    def test_serialize_task_state_coerces_non_json_values(self):
        from services.scorer_matcher.main import _serialize_task_state

        result = _serialize_task_state({"owner_id": object(), "count": 1})

        assert result["count"] == 1
        assert isinstance(result["owner_id"], str)

    def test_compute_stale_result_metadata_returns_empty_for_missing_identifiers(self):
        from services.scorer_matcher.main import _compute_stale_result_metadata

        assert _compute_stale_result_metadata(None, "upload-1") == {}
        assert _compute_stale_result_metadata("owner-1", None) == {}

    def test_compute_stale_result_metadata_returns_empty_for_invalid_owner(self):
        from services.scorer_matcher.main import _compute_stale_result_metadata

        assert _compute_stale_result_metadata("not-a-uuid", "upload-1") == {}

    def test_compute_stale_result_metadata_returns_empty_when_no_latest_upload(self):
        from services.scorer_matcher.main import _compute_stale_result_metadata

        repo = Mock()
        repo.get_latest_resume_upload.return_value = None
        mock_uow = MagicMock()
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("services.scorer_matcher.main.job_uow", return_value=mock_uow):
            result = _compute_stale_result_metadata(
                "00000000-0000-0000-0000-000000000001",
                "upload-1",
            )

        assert result == {}

    def test_compute_stale_result_metadata_marks_same_upload_fresh(self):
        from services.scorer_matcher.main import _compute_stale_result_metadata

        repo = Mock()
        repo.get_latest_resume_upload.return_value = SimpleNamespace(
            id="upload-1",
            resume_fingerprint="fp-1",
        )
        mock_uow = MagicMock()
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("services.scorer_matcher.main.job_uow", return_value=mock_uow):
            result = _compute_stale_result_metadata(
                "00000000-0000-0000-0000-000000000001",
                "upload-1",
            )

        assert result["stale_due_to_newer_upload"] is False

    def test_compute_stale_result_metadata_marks_newer_upload_stale(self):
        from services.scorer_matcher.main import _compute_stale_result_metadata

        repo = Mock()
        repo.get_latest_resume_upload.return_value = SimpleNamespace(
            id="upload-2",
            resume_fingerprint="fp-2",
        )
        mock_uow = MagicMock()
        mock_uow.__enter__.return_value = repo
        mock_uow.__exit__.return_value = False

        with patch("services.scorer_matcher.main.job_uow", return_value=mock_uow):
            result = _compute_stale_result_metadata(
                "00000000-0000-0000-0000-000000000001",
                "upload-1",
            )

        assert result["stale_due_to_newer_upload"] is True
        assert "latest resume" in result["stale_message"]

    def test_compute_stale_result_metadata_returns_empty_on_repository_error(self):
        from services.scorer_matcher.main import _compute_stale_result_metadata

        mock_uow = MagicMock()
        mock_uow.__enter__.side_effect = RuntimeError("db down")

        with patch("services.scorer_matcher.main.job_uow", return_value=mock_uow):
            result = _compute_stale_result_metadata(
                "00000000-0000-0000-0000-000000000001",
                "upload-1",
            )

        assert result == {}


class TestMatcherEndpoints:
    """Test matcher FastAPI endpoints."""

    @pytest.fixture
    def app_with_state(self):
        """Create app with mocked state."""
        from services.scorer_matcher.main import app, MatcherState, MatcherConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=MatcherConsumer)
        state = MatcherState(mock_ctx, mock_consumer)
        app.state.matcher = state

        return app, TestClient(app)

    def test_health(self, app_with_state):
        """Test /health endpoint."""
        app, client = app_with_state
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "healthy"
        assert data["service"] == "matcher"

    def test_metrics_endpoint_prometheus(self, app_with_state):
        """/metrics serves Prometheus text-format (replaces the deleted JSON liveness dict)."""
        app, client = app_with_state
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "text/plain" in r.headers.get("content-type", "")
        assert b"jobscout_scorer_route_total" in r.content

    def test_stop_sets_stop_event(self, app_with_state):
        """Test /match/stop sets stop event."""
        app, client = app_with_state
        r = client.post("/match/stop")
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert app.state.matcher.stop_event.is_set()

    def test_match_resume_success(self, app_with_state):
        """Test /match/resume endpoint."""
        app, client = app_with_state
        mock_result = Mock()
        mock_result.saved_count = 5

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=mock_result):
            r = client.post("/match/resume", json={"resume_fingerprint": "fp-123"})

        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["matches"] == 5

    def test_match_resume_no_matches(self, app_with_state):
        """Test /match/resume when matching returns 0 matches."""
        app, client = app_with_state
        mock_result = Mock()
        mock_result.saved_count = 0

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=mock_result):
            r = client.post("/match/resume", json={"resume_fingerprint": "fp-123"})

        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["matches"] == 0
        assert "No matches" in data["message"]

    def test_match_resume_none_result(self, app_with_state):
        """Test /match/resume when pipeline returns None."""
        app, client = app_with_state
        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=None):
            r = client.post("/match/resume", json={"resume_fingerprint": "fp-none"})

        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["matches"] == 0

    def test_match_resume_exception_returns_failure(self, app_with_state):
        """Test /match/resume returns failure when pipeline raises."""
        app, client = app_with_state
        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   side_effect=Exception("Pipeline error")):
            r = client.post("/match/resume", json={"resume_fingerprint": "fp-error"})

        assert r.status_code == 200
        data = r.json()
        assert data["success"] is False

    def test_match_jobs_empty(self, app_with_state):
        """Test /match/jobs with no job IDs."""
        app, client = app_with_state
        r = client.post("/match/jobs", json={"job_ids": []})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["matches"] == 0

    def test_match_jobs_not_implemented(self, app_with_state):
        """Test /match/jobs returns not implemented."""
        app, client = app_with_state
        r = client.post("/match/jobs", json={"job_ids": ["job-1"]})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is False
        assert "not yet implemented" in data["message"]


class TestMatcherConsumer:
    """Test MatcherConsumer class."""

    @pytest.mark.asyncio
    async def test_do_process_validates_fields(self):
        """_do_process validates required fields."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        success, result = await consumer._do_process("msg-1", {"task_id": "t-1"})
        assert success is False
        assert result["status"] == "failed"

    @pytest.mark.asyncio
    async def test_do_process_success(self):
        """_do_process returns success on completion."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        mock_result = Mock()
        mock_result.matches_count = 3
        mock_result.saved_count = 3
        mock_result.notified_count = 1
        mock_result.execution_time = 2.5
        mock_result.cancelled = False
        mock_result.error = None

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=mock_result), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=False), \
             patch("services.scorer_matcher.main._compute_stale_result_metadata", return_value={}), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state") as mock_set_state:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is True
        assert result["status"] == "completed"
        assert result["matches_count"] == 3
        assert mock_set_state.call_args_list[0].args[1] == {
            "status": "running",
            "step": "initializing",
            "task_type": "matching",
            "owner_id": None,
            "upload_id": None,
            "resume_fingerprint": "fp-123",
        }
        assert mock_set_state.call_args_list[-1].args[1] == {
            "status": "completed",
            "step": "initializing",
            "task_type": "matching",
            "owner_id": None,
            "upload_id": None,
            "resume_fingerprint": "fp-123",
            "result": {
                "matches_count": 3,
                "saved_count": 3,
                "notified_count": 1,
                "execution_time": 2.5,
            },
            "error": None,
        }

    @pytest.mark.asyncio
    async def test_do_process_no_result(self):
        """_do_process handles no result."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=None), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=False), \
             patch("services.scorer_matcher.main._compute_stale_result_metadata", return_value={}), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state"):
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is True
        assert result["status"] == "completed"
        assert result["matches_count"] == 0

    @pytest.mark.asyncio
    async def test_do_process_failure(self):
        """_do_process returns failure on error."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   side_effect=Exception("Pipeline failed")), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=False), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state") as mock_set_state:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is False
        assert result["status"] == "failed"
        assert "Pipeline failed" in result.get("error", "")
        assert mock_set_state.call_args_list[-1].args[1] == {
            "status": "failed",
            "step": "initializing",
            "task_type": "matching",
            "owner_id": None,
            "upload_id": None,
            "resume_fingerprint": "fp-123",
            "error": "Pipeline failed",
        }

    @pytest.mark.asyncio
    async def test_do_process_persists_matching_steps(self):
        """_do_process writes each matching stage to Redis as the runner advances."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        mock_result = Mock(matches_count=4, saved_count=2, notified_count=0, execution_time=1.25)
        mock_result.cancelled = False
        mock_result.error = None

        def fake_run(ctx, stop_event, resume_fingerprint, status_callback):
            status_callback("loading_resume")
            status_callback("vector_matching")
            status_callback("scoring")
            status_callback("saving_results")
            status_callback("notifying")
            return mock_result

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync", side_effect=fake_run), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=False), \
             patch("services.scorer_matcher.main._compute_stale_result_metadata", return_value={}), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state") as mock_set_state:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is True
        assert result["status"] == "completed"
        running_steps = [
            call.args[1]["step"]
            for call in mock_set_state.call_args_list
            if call.args[1]["status"] == "running"
        ]
        assert running_steps == [
            "initializing",
            "loading_resume",
            "vector_matching",
            "scoring",
            "saving_results",
            "notifying",
        ]

    @pytest.mark.asyncio
    async def test_do_process_includes_stale_result_metadata(self):
        """_do_process preserves stale-result metadata in terminal task state."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        mock_result = Mock(
            matches_count=2,
            saved_count=2,
            notified_count=0,
            execution_time=0.5,
            cancelled=False,
            error=None,
        )

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync", return_value=mock_result), \
             patch(
                 "services.scorer_matcher.main._compute_stale_result_metadata",
                 return_value={
                     "stale_due_to_newer_upload": True,
                     "latest_upload_id": "upload-new",
                     "latest_resume_fingerprint": "fp-new",
                     "stale_message": "These results were generated from an older resume upload.",
                 },
             ), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=False), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state") as mock_set_state:
            success, result = await consumer._do_process(
                "msg-1",
                {
                    "task_id": "t-1",
                    "resume_fingerprint": "fp-old",
                    "owner_id": "00000000-0000-0000-0000-000000000001",
                    "resume_upload_id": "upload-old",
                },
            )

        assert success is True
        assert result["status"] == "completed"
        assert mock_set_state.call_args_list[-1].args[1]["stale_due_to_newer_upload"] is True
        assert mock_set_state.call_args_list[-1].args[1]["latest_upload_id"] == "upload-new"

    @pytest.mark.asyncio
    async def test_do_process_marks_cancellation_requested_before_save_boundary(self):
        """Cancellation before saving uses cancellation_requested state."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        mock_result = Mock(
            matches_count=0,
            saved_count=0,
            notified_count=0,
            execution_time=0.1,
            cancelled=True,
            error="Cancelled by user",
        )

        def fake_run(_ctx, _stop_event, _resume_fingerprint, status_callback):
            status_callback("scoring")
            return mock_result

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync", side_effect=fake_run), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=True), \
             patch("services.scorer_matcher.main._compute_stale_result_metadata", return_value={}), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state") as mock_set_state:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"},
            )

        assert success is False
        assert result["status"] == "cancelled"
        running_states = [call.args[1] for call in mock_set_state.call_args_list[:-1]]
        assert any(state["status"] == "cancellation_requested" for state in running_states)

    @pytest.mark.asyncio
    async def test_do_process_marks_persisting_after_save_boundary(self):
        """Cancellation after save boundary uses persisting state."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        mock_result = Mock(
            matches_count=1,
            saved_count=1,
            notified_count=0,
            execution_time=0.2,
            cancelled=True,
            error="Cancelled during save",
        )

        def fake_run(_ctx, _stop_event, _resume_fingerprint, status_callback):
            status_callback("saving_results")
            return mock_result

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync", side_effect=fake_run), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=True), \
             patch("services.scorer_matcher.main._compute_stale_result_metadata", return_value={}), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state") as mock_set_state:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"},
            )

        assert success is False
        assert result["status"] == "cancelled"
        running_states = [call.args[1] for call in mock_set_state.call_args_list[:-1]]
        assert any(state["status"] == "persisting" for state in running_states)

    @pytest.mark.asyncio
    async def test_do_process_ignores_cancellation_lookup_failures(self):
        from services.scorer_matcher.main import MatcherConsumer

        consumer = MatcherConsumer(Mock())
        mock_result = Mock(
            matches_count=1,
            saved_count=1,
            notified_count=0,
            execution_time=0.1,
            cancelled=False,
            error=None,
        )

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync", return_value=mock_result), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", side_effect=RuntimeError("redis down")), \
             patch("services.scorer_matcher.main._compute_stale_result_metadata", return_value={}), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state") as mock_set_state:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"},
            )

        assert success is True
        assert result["status"] == "completed"
        assert mock_set_state.call_args_list[0].args[1]["status"] == "running"

    @pytest.mark.asyncio
    async def test_do_process_logs_when_completed_state_write_fails(self):
        from services.scorer_matcher.main import MatcherConsumer

        consumer = MatcherConsumer(Mock())
        mock_result = Mock(
            matches_count=1,
            saved_count=1,
            notified_count=0,
            execution_time=0.1,
            cancelled=False,
            error=None,
        )

        set_state = Mock(side_effect=[None, RuntimeError("write failed")])

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync", return_value=mock_result), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=False), \
             patch("services.scorer_matcher.main._compute_stale_result_metadata", return_value={}), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state", set_state), \
             patch("services.scorer_matcher.main.logger.warning") as mock_warning:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"},
            )

        assert success is True
        assert result["status"] == "completed"
        mock_warning.assert_called()

    @pytest.mark.asyncio
    async def test_do_process_logs_when_failed_state_write_fails(self):
        from services.scorer_matcher.main import MatcherConsumer

        consumer = MatcherConsumer(Mock())
        set_state = Mock(side_effect=[None, RuntimeError("write failed")])

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync", side_effect=Exception("Pipeline failed")), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=False), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state", set_state), \
             patch("services.scorer_matcher.main.logger.warning") as mock_warning:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"},
            )

        assert success is False
        assert result["status"] == "failed"
        mock_warning.assert_called()

    @pytest.mark.asyncio
    async def test_do_process_forwards_owner_context_when_present(self):
        from services.scorer_matcher.main import MatcherConsumer

        consumer = MatcherConsumer(Mock())
        mock_result = Mock(
            matches_count=1,
            saved_count=1,
            notified_count=1,
            execution_time=0.1,
            cancelled=False,
            error=None,
        )

        def fake_run(_ctx, _stop_event, _resume_fingerprint, _status_callback, owner_id=None, task_id=None):
            assert owner_id == "owner-123"
            assert task_id == "task-123"
            return mock_result

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync", side_effect=fake_run), \
             patch("services.scorer_matcher.main.is_task_cancellation_requested", return_value=False), \
             patch("services.scorer_matcher.main._compute_stale_result_metadata", return_value={}), \
             patch("services.scorer_matcher.main.clear_task_cancellation_requested"), \
             patch("services.scorer_matcher.main.set_task_state"):
            success, result = await consumer._do_process(
                "msg-1",
                {
                    "task_id": "task-123",
                    "resume_fingerprint": "fp-123",
                    "owner_id": "owner-123",
                },
            )

        assert success is True
        assert result["status"] == "completed"


class TestMatcherAppLifespan:
    """Test matcher app lifespan."""

    def test_startup_sets_state_and_logs(self):
        """App lifespan startup sets state."""
        from services.scorer_matcher.main import app, MatcherState, MatcherConsumer
        import logging

        # Simulate lifespan startup
        config_mock = Mock()
        with patch("services.scorer_matcher.main.load_config", return_value=config_mock):
            with patch("services.scorer_matcher.main.AppContext") as mock_ctx_class:
                mock_ctx = Mock()
                mock_ctx_class.build.return_value = mock_ctx

                # Manually test what lifespan does
                consumer = MatcherConsumer(mock_ctx)
                state = MatcherState(mock_ctx, consumer)

                assert state.ctx is mock_ctx
                assert state.consumer is consumer
                assert state.stop_event.is_set() is False

    def test_shutdown_cancels_consumer_task(self):
        """App lifespan shutdown cancels consumer task."""
        from services.scorer_matcher.main import MatcherState, MatcherConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=MatcherConsumer)
        state = MatcherState(mock_ctx, mock_consumer)

        mock_task = Mock()
        mock_task.cancel = Mock()
        state.consumer_task = mock_task

        # Simulate shutdown
        state.stop_event.set()
        if state.consumer_task:
            state.consumer_task.cancel()

        mock_task.cancel.assert_called_once()
        assert state.stop_event.is_set()

    def test_shutdown_closes_app_context(self):
        """App lifespan shutdown closes context."""
        from services.scorer_matcher.main import MatcherState, MatcherConsumer

        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        mock_consumer = Mock(spec=MatcherConsumer)
        state = MatcherState(mock_ctx, mock_consumer)

        # Simulate shutdown
        asyncio.run(state.ctx.aclose())

        mock_ctx.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_lifespan_starts_consumer_and_closes_async_context(self):
        from services.scorer_matcher.main import app, lifespan

        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        created_task = Mock()
        created_task.cancel = Mock()

        def _mock_create_task(coro):
            coro.close()
            return created_task

        with patch("services.scorer_matcher.main.init_db"), \
             patch("services.scorer_matcher.main.load_config", return_value=Mock()), \
             patch("services.scorer_matcher.main.AppContext.build", return_value=mock_ctx), \
             patch("services.scorer_matcher.main.asyncio.create_task", side_effect=_mock_create_task) as mock_create_task, \
             patch("services.scorer_matcher.main.asyncio.gather", new_callable=AsyncMock) as mock_gather:
            async with lifespan(app):
                assert app.state.matcher.ctx is mock_ctx
                mock_create_task.assert_called_once()

        created_task.cancel.assert_called_once()
        mock_gather.assert_awaited_once()
        mock_ctx.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lifespan_closes_sync_context_when_async_close_missing(self):
        from services.scorer_matcher.main import app, lifespan

        mock_ctx = SimpleNamespace(close=Mock())
        created_task = Mock()
        created_task.cancel = Mock()

        def _mock_create_task(coro):
            coro.close()
            return created_task

        with patch("services.scorer_matcher.main.init_db"), \
             patch("services.scorer_matcher.main.load_config", return_value=Mock()), \
             patch("services.scorer_matcher.main.AppContext.build", return_value=mock_ctx), \
             patch("services.scorer_matcher.main.asyncio.create_task", side_effect=_mock_create_task), \
             patch("services.scorer_matcher.main.asyncio.gather", new_callable=AsyncMock):
            async with lifespan(app):
                pass

        mock_ctx.close.assert_called_once()


class TestRunMatchingPipelineSync:
    """Test _run_matching_pipeline_sync function."""

    def test_run_matching_pipeline_sync_calls_runner(self):
        """_run_matching_pipeline_sync should call run_matching_pipeline."""
        from services.scorer_matcher.main import _run_matching_pipeline_sync
        import threading

        mock_ctx = Mock()
        stop_event = threading.Event()
        status_callback = Mock()

        mock_result = Mock()
        mock_result.saved_count = 5

        with patch("services.scorer_matcher.main.run_matching_pipeline",
                   return_value=mock_result) as mock_runner:
            result = _run_matching_pipeline_sync(
                mock_ctx,
                stop_event,
                "fp-123",
                status_callback,
            )

            mock_runner.assert_called_once_with(
                mock_ctx,
                stop_event,
                status_callback=status_callback,
                resume_fingerprint="fp-123",
            )
            assert result.saved_count == 5

    def test_run_matching_pipeline_sync_without_fingerprint(self):
        """_run_matching_pipeline_sync should work without fingerprint."""
        from services.scorer_matcher.main import _run_matching_pipeline_sync
        import threading

        mock_ctx = Mock()
        stop_event = threading.Event()

        with patch("services.scorer_matcher.main.run_matching_pipeline") as mock_runner:
            mock_runner.return_value = None
            result = _run_matching_pipeline_sync(mock_ctx, stop_event)

            mock_runner.assert_called_once_with(
                mock_ctx,
                stop_event,
                status_callback=None,
                resume_fingerprint=None,
            )
            assert result is None

    def test_run_matching_pipeline_sync_includes_owner_context_when_provided(self):
        """_run_matching_pipeline_sync forwards owner/task context only when provided."""
        from services.scorer_matcher.main import _run_matching_pipeline_sync
        import threading

        mock_ctx = Mock()
        stop_event = threading.Event()
        status_callback = Mock()

        with patch("services.scorer_matcher.main.run_matching_pipeline") as mock_runner:
            mock_runner.return_value = Mock(saved_count=1)

            _run_matching_pipeline_sync(
                mock_ctx,
                stop_event,
                "fp-123",
                status_callback,
                owner_id="owner-123",
                task_id="task-123",
            )

            mock_runner.assert_called_once_with(
                mock_ctx,
                stop_event,
                status_callback=status_callback,
                resume_fingerprint="fp-123",
                owner_id="owner-123",
                task_id="task-123",
            )


class TestWarmUpCrossEncoder:
    """`_warm_up_cross_encoder` decides strict-vs-lenient based on env."""

    def _config(self, *, enabled=True, batch_size=8, runtime="auto"):
        return SimpleNamespace(matching=SimpleNamespace(scorer=SimpleNamespace(
            semantic_fit=SimpleNamespace(cross_encoder=SimpleNamespace(local=SimpleNamespace(
                enabled=enabled,
                model_name="bge-test",
                model_cache_path=None,
                runtime=runtime,
                max_batch_size=batch_size,
                trust_remote_code=False,
            )))
        )))

    def test_skipped_when_local_provider_disabled(self, caplog):
        from services.scorer_matcher.main import _warm_up_cross_encoder
        with patch(
            "core.scorer.semantic_fit.LocalCrossEncoderProvider"
        ) as mock_provider_cls:
            _warm_up_cross_encoder(self._config(enabled=False))
        mock_provider_cls.assert_not_called()

    def test_skipped_when_max_batch_size_unparseable(self):
        from services.scorer_matcher.main import _warm_up_cross_encoder
        with patch(
            "core.scorer.semantic_fit.LocalCrossEncoderProvider"
        ) as mock_provider_cls:
            _warm_up_cross_encoder(self._config(batch_size="not-an-int"))
        mock_provider_cls.assert_not_called()

    def test_success_path_logs_diagnostics(self, monkeypatch, caplog):
        import logging as _logging
        from services.scorer_matcher.main import _warm_up_cross_encoder
        provider = Mock()
        provider.warm_up.return_value = {
            "provider_route": "local_native",
            "canary_score": 0.42,
        }
        with patch(
            "core.scorer.semantic_fit.LocalCrossEncoderProvider",
            return_value=provider,
        ), caplog.at_level(_logging.INFO, logger="services.scorer_matcher.main"):
            _warm_up_cross_encoder(self._config())
        provider.warm_up.assert_called_once()
        assert any("Cross-encoder warm-up succeeded" in r.getMessage() for r in caplog.records)

    def test_failure_strict_raises(self, monkeypatch):
        from services.scorer_matcher.main import _warm_up_cross_encoder
        monkeypatch.setenv("MATCHER_STRICT_WARMUP", "true")
        provider = Mock()
        provider.warm_up.side_effect = RuntimeError("boom")
        with patch(
            "core.scorer.semantic_fit.LocalCrossEncoderProvider",
            return_value=provider,
        ), pytest.raises(RuntimeError, match="boom"):
            _warm_up_cross_encoder(self._config())

    def test_failure_lenient_logs_warning_and_continues(self, monkeypatch, caplog):
        import logging as _logging
        from services.scorer_matcher.main import _warm_up_cross_encoder
        monkeypatch.setenv("MATCHER_STRICT_WARMUP", "false")
        provider = Mock()
        provider.warm_up.side_effect = RuntimeError("boom")
        with patch(
            "core.scorer.semantic_fit.LocalCrossEncoderProvider",
            return_value=provider,
        ), caplog.at_level(_logging.WARNING, logger="services.scorer_matcher.main"):
            _warm_up_cross_encoder(self._config())  # must not raise
        warned = [r for r in caplog.records if "MATCHER_STRICT_WARMUP=false" in r.getMessage()]
        assert warned


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
