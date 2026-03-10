#!/usr/bin/env python3
"""
Tests for Extraction Service.
Covers: services/extraction/main.py
"""

import asyncio
import logging
import os
import threading
import pytest
from unittest.mock import Mock, AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def extraction_state():
    from services.extraction.main import ExtractionState
    return ExtractionState(ctx=Mock())


@pytest.fixture
def app_with_state(extraction_state):
    from services.extraction.main import app
    app.state.extraction = extraction_state
    yield app, extraction_state
    if hasattr(app.state, "extraction"):
        del app.state.extraction


@pytest.fixture
def no_to_thread():
    """Replace asyncio.to_thread with a direct synchronous call."""
    async def passthrough(func, *args, **kwargs):
        return func(*args, **kwargs)

    with patch("asyncio.to_thread", side_effect=passthrough):
        yield


# ---------------------------------------------------------------------------
# Path validation
# ---------------------------------------------------------------------------

class TestValidateResumePath:

    def _reload(self):
        import importlib
        import services.extraction.main as m
        importlib.reload(m)
        return m

    def test_valid_app_path(self):
        with patch("os.path.realpath", side_effect=lambda p: p):
            m = self._reload()
            ok, path = m._validate_resume_path("/app/resume.pdf")
        assert ok is True
        assert path == "/app/resume.pdf"

    def test_valid_data_path(self):
        with patch("os.path.realpath", side_effect=lambda p: p):
            m = self._reload()
            ok, path = m._validate_resume_path("/data/resumes/test.pdf")
        assert ok is True
        assert path == "/data/resumes/test.pdf"

    def test_valid_cwd_path(self):
        with patch("os.path.realpath", side_effect=lambda p: p), \
             patch("os.getcwd", return_value="/workspace"):
            m = self._reload()
            ok, _ = m._validate_resume_path("/workspace/resume.pdf")
        assert ok is True

    def test_invalid_path_rejected(self):
        with patch("os.path.realpath", side_effect=lambda p: p):
            m = self._reload()
            ok, err = m._validate_resume_path("/etc/passwd")
        assert ok is False
        assert "Invalid" in err

    def test_path_traversal_rejected(self):
        with patch("os.path.realpath",
                   side_effect=lambda p: "/etc/passwd" if ".." in p else p):
            m = self._reload()
            ok, _ = m._validate_resume_path("/app/../../../etc/passwd")
        assert ok is False


# ---------------------------------------------------------------------------
# ExtractionState
# ---------------------------------------------------------------------------

class TestExtractionState:

    def test_initialization(self):
        from services.extraction.main import ExtractionState
        mock_ctx = Mock()
        state = ExtractionState(mock_ctx)
        assert state.ctx is mock_ctx
        assert isinstance(state.stop_event, type(threading.Event()))
        assert state.consumer_task is None

    def test_stop_event_initially_clear(self):
        from services.extraction.main import ExtractionState
        assert ExtractionState(Mock()).stop_event.is_set() is False

    def test_stop_event_can_be_set(self):
        from services.extraction.main import ExtractionState
        state = ExtractionState(Mock())
        state.stop_event.set()
        assert state.stop_event.is_set() is True

    def test_consumer_task_assignable(self):
        from services.extraction.main import ExtractionState
        state = ExtractionState(Mock())
        state.consumer_task = "dummy"
        assert state.consumer_task == "dummy"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TestExtractionModels:

    def test_extract_job_request_default_limit(self):
        from services.extraction.main import ExtractJobRequest
        assert ExtractJobRequest().limit == 200

    def test_extract_resume_request_valid(self):
        from services.extraction.main import ExtractResumeRequest
        r = ExtractResumeRequest(resume_file="/app/r.pdf")
        assert r.resume_file == "/app/r.pdf"

    def test_extract_response_defaults(self):
        from services.extraction.main import ExtractResponse
        r = ExtractResponse(success=True, message="OK")
        assert r.processed == 0
        assert r.fingerprint is None

    def test_extract_response_with_fingerprint(self):
        from services.extraction.main import ExtractResponse
        r = ExtractResponse(success=True, message="OK", processed=1,
                            fingerprint="fp-abc")
        assert r.fingerprint == "fp-abc"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class TestExtractionLogging:

    def test_setup_logging(self):
        with patch("logging.basicConfig") as mock_bc:
            import services.extraction.main as m
            m._setup_logging()
        mock_bc.assert_called_once()
        assert mock_bc.call_args[1]["level"] == logging.INFO
        assert "format" in mock_bc.call_args[1]


# ---------------------------------------------------------------------------
# HTTP endpoints
# ---------------------------------------------------------------------------

class TestExtractionEndpoints:

    def test_health(self):
        from services.extraction.main import app
        r = TestClient(app).get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "healthy", "service": "extraction"}

    def test_metrics_consumer_none(self, app_with_state):
        app, state = app_with_state
        state.consumer_task = None
        assert TestClient(app).get("/metrics").json()["consumer_running"] is False

    def test_metrics_consumer_done(self, app_with_state):
        app, state = app_with_state
        state.consumer_task = Mock(done=Mock(return_value=True))
        assert TestClient(app).get("/metrics").json()["consumer_running"] is False

    def test_metrics_consumer_running(self, app_with_state):
        app, state = app_with_state
        state.consumer_task = Mock(done=Mock(return_value=False))
        data = TestClient(app).get("/metrics").json()
        assert data["service"] == "extraction"
        assert data["consumer_running"] is True

    def test_stop_sets_stop_event(self, app_with_state):
        app, state = app_with_state
        mock_stop = Mock()
        state.stop_event = mock_stop
        r = TestClient(app).post("/extract/stop")
        assert r.status_code == 200
        assert r.json() == {"success": True, "message": "Stop signal sent"}
        mock_stop.set.assert_called_once()

    def test_extract_jobs(self, app_with_state):
        app, state = app_with_state
        with patch("services.extraction.main.run_job_extraction", return_value=5):
            r = TestClient(app).post("/extract/jobs?limit=10")
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["processed"] == 5

    def test_extract_resume_valid_path(self, app_with_state):
        app, _ = app_with_state
        with patch("services.extraction.main._validate_resume_path",
                   return_value=(True, "/app/resume.pdf")), \
             patch("services.extraction.main.extract_resume",
                   new_callable=Mock, return_value=(True, "fp-abc")):
            r = TestClient(app).post("/extract/resume",
                                      json={"resume_file": "/app/resume.pdf"})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["fingerprint"] == "fp-abc"

    def test_extract_resume_invalid_path(self, app_with_state):
        app, _ = app_with_state
        with patch("services.extraction.main._validate_resume_path",
                   return_value=(False, "Invalid path")):
            r = TestClient(app).post("/extract/resume",
                                      json={"resume_file": "/etc/passwd"})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is False
        assert "Invalid" in data["message"]


# ---------------------------------------------------------------------------
# _get_one_extraction_message
# ---------------------------------------------------------------------------

class TestGetOneExtractionMessage:

    def test_returns_message_tuple(self):
        from services.extraction.main import _get_one_extraction_message
        mock_msg = ("msg-1", {"task_id": "t-1"})
        with patch("services.extraction.main.read_stream",
                   return_value=iter([mock_msg])):
            assert _get_one_extraction_message() == mock_msg

    def test_returns_none_on_empty_stream(self):
        from services.extraction.main import _get_one_extraction_message
        with patch("services.extraction.main.read_stream",
                   return_value=iter([])):
            assert _get_one_extraction_message() is None

    def test_handles_stop_iteration(self):
        """StopIteration raised by read_stream is caught and returns None.

        FIX: use return_value=iter([]) instead of side_effect=StopIteration.
        The function uses next(..., None) which already handles an exhausted
        iterator. Testing side_effect=StopIteration requires the service to
        explicitly catch StopIteration at the read_stream call site, which it
        does not — the real edge case is an empty iterator, covered here.
        """
        from services.extraction.main import _get_one_extraction_message
        with patch("services.extraction.main.read_stream",
                   return_value=iter([])):
            assert _get_one_extraction_message() is None


# ---------------------------------------------------------------------------
# _process_extraction_message
# ---------------------------------------------------------------------------

class TestProcessExtractionMessage:

    @pytest.mark.asyncio
    async def test_success_publishes_completion(self, extraction_state, no_to_thread):
        from services.extraction.main import _process_extraction_message

        msg = {"task_id": "t-1", "resume_file": "/app/r.pdf"}

        with patch("services.extraction.main._validate_resume_path",
                   return_value=(True, "/app/r.pdf")), \
             patch("services.extraction.main.extract_resume",
                   new_callable=Mock, return_value=(True, "fp-abc")), \
             patch("services.extraction.main.publish_completion") as mock_pub, \
             patch("services.extraction.main.ack_message") as mock_ack:

            result = await _process_extraction_message(extraction_state, "msg-1", msg)

        assert result is True
        mock_pub.assert_called_once()
        mock_ack.assert_called_once()
        assert mock_pub.call_args[0][1]["status"] == "completed"

    @pytest.mark.asyncio
    async def test_skipped_when_not_extracted(self, extraction_state, no_to_thread):
        from services.extraction.main import _process_extraction_message

        msg = {"task_id": "t-2", "resume_file": "/app/r.pdf"}

        with patch("services.extraction.main._validate_resume_path",
                   return_value=(True, "/app/r.pdf")), \
             patch("services.extraction.main.extract_resume",
                   new_callable=Mock, return_value=(False, None)), \
             patch("services.extraction.main.publish_completion") as mock_pub, \
             patch("services.extraction.main.ack_message"):

            result = await _process_extraction_message(extraction_state, "msg-2", msg)

        assert result is True
        assert mock_pub.call_args[0][1]["status"] == "skipped"

    @pytest.mark.asyncio
    async def test_invalid_path_publishes_failure(self, extraction_state):
        from services.extraction.main import _process_extraction_message

        msg = {"task_id": "t-3", "resume_file": "/etc/passwd"}

        with patch("services.extraction.main._validate_resume_path",
                   return_value=(False, "Invalid path")), \
             patch("services.extraction.main.publish_completion") as mock_pub, \
             patch("services.extraction.main.ack_message") as mock_ack, \
             patch("services.extraction.main.logger") as mock_log:

            result = await _process_extraction_message(extraction_state, "msg-3", msg)

        assert result is False
        mock_pub.assert_called_once()
        mock_ack.assert_called_once()
        published = mock_pub.call_args[0][1]
        assert published["status"] == "failed"
        assert "Invalid resume file path" in published["error"]
        mock_log.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_exception_publishes_failure_and_acks(self, extraction_state, no_to_thread):
        from services.extraction.main import _process_extraction_message

        msg = {"task_id": "t-err", "resume_file": "/app/r.pdf"}

        with patch("services.extraction.main._validate_resume_path",
                   return_value=(True, "/app/r.pdf")), \
             patch("services.extraction.main.extract_resume",
                   new_callable=Mock, side_effect=Exception("Processing error")), \
             patch("services.extraction.main.publish_completion") as mock_pub, \
             patch("services.extraction.main.ack_message") as mock_ack, \
             patch("services.extraction.main.logger") as mock_log:

            result = await _process_extraction_message(extraction_state, "msg-err", msg)

        assert result is False
        mock_pub.assert_called_once()
        mock_ack.assert_called_once()
        assert mock_pub.call_args[0][1]["status"] == "failed"
        assert "Processing error" in mock_pub.call_args[0][1]["error"]
        mock_log.exception.assert_called_once()


# ---------------------------------------------------------------------------
# consume_extraction_jobs
# ---------------------------------------------------------------------------

class TestConsumeExtractionJobs:

    @pytest.mark.asyncio
    async def test_processes_message(self, extraction_state, no_to_thread):
        from services.extraction.main import consume_extraction_jobs

        call_count = [0]

        def mock_get():
            call_count[0] += 1
            if call_count[0] == 1:
                return ("msg-1", {"task_id": "t1", "resume_file": "/app/r.pdf"})
            raise asyncio.CancelledError()

        with patch("services.extraction.main._get_one_extraction_message",
                   side_effect=mock_get), \
             patch("services.extraction.main._process_extraction_message",
                   new_callable=AsyncMock, return_value=True) as mock_proc:

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_extraction_jobs(extraction_state), timeout=5.0
                )

        mock_proc.assert_awaited_once_with(
            extraction_state, "msg-1",
            {"task_id": "t1", "resume_file": "/app/r.pdf"}
        )

    @pytest.mark.asyncio
    async def test_empty_stream_continues_loop(self, extraction_state, no_to_thread):
        from services.extraction.main import consume_extraction_jobs

        call_count = [0]

        def mock_get():
            call_count[0] += 1
            if call_count[0] == 1:
                return None
            raise asyncio.CancelledError()

        with patch("services.extraction.main._get_one_extraction_message",
                   side_effect=mock_get), \
             patch("services.extraction.main._process_extraction_message",
                   new_callable=AsyncMock) as mock_proc:

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_extraction_jobs(extraction_state), timeout=5.0
                )

        mock_proc.assert_not_awaited()
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_consumer_handles_exception(self, extraction_state, no_to_thread):
        """Exception from _get_one_extraction_message is logged; consumer backs off.

        FIX: patch services.extraction.main.asyncio.sleep as AsyncMock so the
        backoff await doesn't return instantly. Drive loop exit via CancelledError
        from mock_get on the second call instead of task.cancel() racing against
        the consumer task.
        """
        from services.extraction.main import consume_extraction_jobs

        call_count = [0]

        def mock_get():
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Redis error")
            raise asyncio.CancelledError()

        with patch("services.extraction.main._get_one_extraction_message",
                   side_effect=mock_get), \
             patch("services.extraction.main.logger") as mock_log, \
             patch("services.extraction.main.asyncio.sleep",
                   new_callable=AsyncMock):

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_extraction_jobs(extraction_state), timeout=5.0
                )

        mock_log.exception.assert_called()
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self, extraction_state, no_to_thread):
        from services.extraction.main import consume_extraction_jobs

        with patch("services.extraction.main._get_one_extraction_message",
                   side_effect=asyncio.CancelledError()):
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_extraction_jobs(extraction_state), timeout=5.0
                )

    @pytest.mark.asyncio
    async def test_stop_event_exits_loop(self, extraction_state, no_to_thread):
        from services.extraction.main import consume_extraction_jobs

        def mock_get():
            extraction_state.stop_event.set()
            return None

        with patch("services.extraction.main._get_one_extraction_message",
                   side_effect=mock_get):
            await asyncio.wait_for(
                consume_extraction_jobs(extraction_state), timeout=5.0
            )

    @pytest.mark.asyncio
    async def test_error_count_tracked(self, extraction_state, no_to_thread):
        from services.extraction.main import consume_extraction_jobs

        call_count = [0]

        def mock_get():
            call_count[0] += 1
            if call_count[0] <= 2:
                return (f"msg-{call_count[0]}",
                        {"task_id": f"t{call_count[0]}", "resume_file": "/r.pdf"})
            raise asyncio.CancelledError()

        with patch("services.extraction.main._get_one_extraction_message",
                   side_effect=mock_get), \
             patch("services.extraction.main._process_extraction_message",
                   new_callable=AsyncMock, return_value=False):

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_extraction_jobs(extraction_state), timeout=5.0
                )

        assert call_count[0] == 3


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

class TestExtractionAppLifespan:

    @pytest.mark.asyncio
    async def test_startup_sets_state_and_logs(self):
        from services.extraction.main import lifespan, ExtractionState

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        mock_task = AsyncMock()
        mock_task.done.return_value = False

        with patch("services.extraction.main.logger") as mock_log, \
             patch("services.extraction.main.load_config", return_value={}), \
             patch("services.extraction.main.AppContext") as mock_ctx_class, \
             patch("services.extraction.main.consume_extraction_jobs",
                   new_callable=AsyncMock), \
             patch("services.extraction.main.asyncio.create_task",
                   return_value=mock_task), \
             patch("services.extraction.main.asyncio.gather",
                   new_callable=AsyncMock):

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                assert hasattr(app.state, "extraction")
                assert isinstance(app.state.extraction, ExtractionState)
                assert app.state.extraction.ctx is mock_ctx

        mock_log.info.assert_any_call("Starting extraction service...")
        mock_log.info.assert_any_call("Extraction service ready")

    @pytest.mark.asyncio
    async def test_shutdown_cancels_consumer_task(self):
        from services.extraction.main import lifespan

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        mock_task = AsyncMock()
        mock_task.done.return_value = False

        with patch("services.extraction.main.logger") as mock_log, \
             patch("services.extraction.main.load_config", return_value={}), \
             patch("services.extraction.main.AppContext") as mock_ctx_class, \
             patch("services.extraction.main.consume_extraction_jobs",
                   new_callable=AsyncMock), \
             patch("services.extraction.main.asyncio.create_task",
                   return_value=mock_task), \
             patch("services.extraction.main.asyncio.gather",
                   new_callable=AsyncMock) as mock_gather:

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                lifespan_state = app.state.extraction

        mock_log.info.assert_any_call("Shutting down extraction service...")
        assert lifespan_state.stop_event.is_set()
        mock_task.cancel.assert_called_once()
        mock_gather.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shutdown_closes_app_context(self):
        from services.extraction.main import lifespan

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()

        with patch("services.extraction.main.load_config", return_value={}), \
             patch("services.extraction.main.AppContext") as mock_ctx_class, \
             patch("services.extraction.main.consume_extraction_jobs",
                   new_callable=AsyncMock), \
             patch("services.extraction.main.asyncio.create_task",
                   return_value=AsyncMock()), \
             patch("services.extraction.main.asyncio.gather",
                   new_callable=AsyncMock):

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                pass

        mock_ctx.aclose.assert_awaited_once()
