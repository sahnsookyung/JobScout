"""
Unit Tests: Extraction Service

Tests the extraction service functionality without requiring
running services.

Usage:
    uv run pytest tests/unit/services/test_extraction.py -v
"""

import asyncio
import pytest
import threading
from unittest.mock import Mock, patch, AsyncMock

from fastapi.testclient import TestClient


class TestValidateResumePath:
    """Test _validate_resume_path function."""

    def test_valid_app_path(self):
        """Valid /app path is accepted."""
        from services.extraction.main import _validate_resume_path
        is_valid, path = _validate_resume_path("/app/resume.pdf")
        assert is_valid is True
        assert path.endswith("resume.pdf")

    def test_valid_data_path(self):
        """Valid /data path is accepted."""
        from services.extraction.main import _validate_resume_path
        is_valid, path = _validate_resume_path("/data/resume.pdf")
        assert is_valid is True

    def test_valid_cwd_path(self):
        """Valid CWD path is accepted."""
        from services.extraction.main import _validate_resume_path
        import os
        cwd_path = os.path.join(os.getcwd(), "resume.pdf")
        is_valid, path = _validate_resume_path(cwd_path)
        assert is_valid is True

    def test_invalid_path_rejected(self):
        """Invalid path is rejected."""
        from services.extraction.main import _validate_resume_path
        is_valid, error = _validate_resume_path("/etc/passwd")
        assert is_valid is False
        assert "Invalid" in error

    def test_path_traversal_rejected(self):
        """Path traversal is rejected."""
        from services.extraction.main import _validate_resume_path
        is_valid, error = _validate_resume_path("/app/../../../etc/passwd")
        assert is_valid is False


class TestExtractionState:
    """Test ExtractionState class."""

    def test_initialization(self):
        """Test ExtractionState initializes correctly."""
        from services.extraction.main import ExtractionState, ExtractionConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=ExtractionConsumer)
        state = ExtractionState(mock_ctx, mock_consumer)

        assert state.ctx is mock_ctx
        assert state.consumer is mock_consumer
        assert isinstance(state.stop_event, type(threading.Event()))
        assert state.consumer_task is None

    def test_stop_event_initially_clear(self):
        """Stop event is initially clear."""
        from services.extraction.main import ExtractionState, ExtractionConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=ExtractionConsumer)
        state = ExtractionState(mock_ctx, mock_consumer)

        assert state.stop_event.is_set() is False

    def test_stop_event_can_be_set(self):
        """Stop event can be set."""
        from services.extraction.main import ExtractionState, ExtractionConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=ExtractionConsumer)
        state = ExtractionState(mock_ctx, mock_consumer)

        state.stop_event.set()
        assert state.stop_event.is_set() is True

    def test_consumer_task_assignable(self):
        """Consumer task can be assigned."""
        from services.extraction.main import ExtractionState, ExtractionConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=ExtractionConsumer)
        state = ExtractionState(mock_ctx, mock_consumer)

        state.consumer_task = "dummy_task"
        assert state.consumer_task == "dummy_task"


class TestExtractionModels:
    """Test extraction Pydantic models."""

    def test_extract_job_request_default_limit(self):
        """ExtractJobRequest has default limit."""
        from services.extraction.main import ExtractJobRequest

        req = ExtractJobRequest()
        assert req.limit == 200

    def test_extract_resume_request_valid(self):
        """ExtractResumeRequest accepts resume_file."""
        from services.extraction.main import ExtractResumeRequest

        req = ExtractResumeRequest(resume_file="/app/resume.pdf")
        assert req.resume_file == "/app/resume.pdf"

    def test_extract_response_defaults(self):
        """ExtractResponse has default processed."""
        from services.extraction.main import ExtractResponse

        resp = ExtractResponse(success=True, message="Done")
        assert resp.success is True
        assert resp.processed == 0

    def test_extract_response_with_fingerprint(self):
        """ExtractResponse accepts fingerprint."""
        from services.extraction.main import ExtractResponse

        resp = ExtractResponse(success=True, message="Done", processed=1, fingerprint="fp-123")
        assert resp.fingerprint == "fp-123"


class TestExtractionLogging:
    """Test extraction logging setup."""

    def test_setup_logging(self):
        """Test setup_logging configures logging."""
        from services.extraction.main import _setup_logging
        # Just verify it runs without error
        _setup_logging()


class TestExtractionEndpoints:
    """Test extraction FastAPI endpoints."""

    @pytest.fixture
    def app_with_state(self):
        """Create app with mocked state."""
        from services.extraction.main import app, ExtractionState, ExtractionConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=ExtractionConsumer)
        state = ExtractionState(mock_ctx, mock_consumer)
        app.state.extraction = state

        return app, TestClient(app)

    def test_health(self, app_with_state):
        """Test /health endpoint."""
        app, client = app_with_state
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "healthy"

    def test_metrics_consumer_none(self, app_with_state):
        """Test /metrics with no consumer task."""
        app, client = app_with_state
        r = client.get("/metrics")
        assert r.status_code == 200

    def test_metrics_consumer_done(self, app_with_state):
        """Test /metrics with done consumer task."""
        app, client = app_with_state
        mock_task = Mock()
        mock_task.done.return_value = True
        app.state.extraction.consumer_task = mock_task

        r = client.get("/metrics")
        assert r.status_code == 200

    def test_metrics_consumer_running(self, app_with_state):
        """Test /metrics with running consumer task."""
        app, client = app_with_state
        mock_task = Mock()
        mock_task.done.return_value = False
        app.state.extraction.consumer_task = mock_task

        r = client.get("/metrics")
        assert r.status_code == 200

    def test_stop_sets_stop_event(self, app_with_state):
        """Test /extract/stop sets stop event."""
        app, client = app_with_state
        r = client.post("/extract/stop")
        assert r.status_code == 200
        assert app.state.extraction.stop_event.is_set()

    def test_extract_jobs(self, app_with_state):
        """Test /extract/jobs endpoint."""
        app, client = app_with_state
        with patch("services.extraction.main.run_job_extraction", return_value=5):
            r = client.post("/extract/jobs", json={"limit": 100})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["processed"] == 5

    def test_extract_resume_valid_path(self, app_with_state):
        """Test /extract/resume with valid path."""
        app, client = app_with_state
        with patch("services.extraction.main.extract_resume_file",
                   return_value=(True, "fp-abc")):
            r = client.post("/extract/resume", json={"resume_file": "/app/resume.pdf"})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["fingerprint"] == "fp-abc"

    def test_extract_resume_invalid_path(self, app_with_state):
        """Test /extract/resume with invalid path."""
        app, client = app_with_state
        r = client.post("/extract/resume", json={"resume_file": "/etc/passwd"})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is False
        assert "Invalid" in data["message"]


class TestExtractionConsumer:
    """Test ExtractionConsumer class."""

    @pytest.mark.asyncio
    async def test_do_process_validates_fields(self):
        """_do_process validates required fields."""
        from services.extraction.main import ExtractionConsumer

        mock_ctx = Mock()
        consumer = ExtractionConsumer(mock_ctx)

        success, result = await consumer._do_process("msg-1", {"task_id": "t-1"})
        assert success is False
        assert result["status"] == "failed"

    @pytest.mark.asyncio
    async def test_do_process_validates_path(self):
        """_do_process validates resume path."""
        from services.extraction.main import ExtractionConsumer

        mock_ctx = Mock()
        consumer = ExtractionConsumer(mock_ctx)

        success, result = await consumer._do_process(
            "msg-1",
            {"task_id": "t-1", "resume_file": "/etc/passwd"}
        )
        assert success is False
        assert result["status"] == "failed"
        assert "Invalid" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_do_process_success(self):
        """_do_process returns success on completion."""
        from services.extraction.main import ExtractionConsumer

        mock_ctx = Mock()
        consumer = ExtractionConsumer(mock_ctx)

        with patch("services.extraction.main.extract_resume_file",
                   return_value=(True, "fp-123")):
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_file": "/app/resume.pdf"}
            )

        assert success is True
        assert result["status"] == "completed"
        assert result["resume_fingerprint"] == "fp-123"

    @pytest.mark.asyncio
    async def test_do_process_skipped(self):
        """_do_process returns skipped when no changes."""
        from services.extraction.main import ExtractionConsumer

        mock_ctx = Mock()
        consumer = ExtractionConsumer(mock_ctx)

        with patch("services.extraction.main.extract_resume_file",
                   return_value=(False, "fp-123")):
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_file": "/app/resume.pdf"}
            )

        assert success is True
        assert result["status"] == "skipped"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
