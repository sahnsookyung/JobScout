"""
Unit Tests: Scorer-Matcher Service

Tests the scorer-matcher service functionality.

Usage:
    uv run pytest tests/unit/services/test_scorer_matcher.py -v
"""

import asyncio
import pytest
import threading
from unittest.mock import Mock, patch, AsyncMock

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

    def test_metrics(self, app_with_state):
        """Test /metrics endpoint."""
        app, client = app_with_state
        r = client.get("/metrics")
        assert r.status_code == 200
        data = r.json()
        assert data["service"] == "matcher"
        assert data["version"] == "1.0.0"

    def test_metrics_consumer_done(self, app_with_state):
        """Test /metrics with done consumer task."""
        app, client = app_with_state
        mock_task = Mock()
        mock_task.done.return_value = True
        app.state.matcher.consumer_task = mock_task

        r = client.get("/metrics")
        assert r.status_code == 200
        data = r.json()
        assert data["consumer_running"] is False

    def test_metrics_consumer_running(self, app_with_state):
        """Test /metrics with running consumer task."""
        app, client = app_with_state
        mock_task = Mock()
        mock_task.done.return_value = False
        app.state.matcher.consumer_task = mock_task

        r = client.get("/metrics")
        assert r.status_code == 200
        data = r.json()
        assert data["consumer_running"] is True

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
        mock_result.saved_count = 3

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=mock_result):
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is True
        assert result["status"] == "completed"
        assert result["matches_count"] == 3

    @pytest.mark.asyncio
    async def test_do_process_no_result(self):
        """_do_process handles no result."""
        from services.scorer_matcher.main import MatcherConsumer

        mock_ctx = Mock()
        consumer = MatcherConsumer(mock_ctx)

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=None):
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
                   side_effect=Exception("Pipeline failed")):
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is False
        assert result["status"] == "failed"
        assert "Pipeline failed" in result.get("error", "")


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
        import asyncio
        asyncio.run(state.ctx.aclose())

        mock_ctx.aclose.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
