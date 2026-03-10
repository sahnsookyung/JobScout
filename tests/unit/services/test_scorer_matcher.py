#!/usr/bin/env python3
"""
Tests for Scorer/Matcher Service.
Covers: services/scorer_matcher/main.py
"""

import asyncio
import logging
import threading
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def matcher_state():
    from services.scorer_matcher.main import MatcherState
    return MatcherState(ctx=Mock())


@pytest.fixture
def app_with_state(matcher_state):
    from services.scorer_matcher.main import app
    app.state.matcher = matcher_state
    yield app, matcher_state
    if hasattr(app.state, "matcher"):
        del app.state.matcher


@pytest.fixture
def no_to_thread():
    """Replace asyncio.to_thread with a direct synchronous call."""
    async def passthrough(func, *args, **kwargs):
        return func(*args, **kwargs)

    with patch("asyncio.to_thread", side_effect=passthrough):
        yield


# ---------------------------------------------------------------------------
# MatcherState
# ---------------------------------------------------------------------------

class TestMatcherState:

    def test_state_initialization(self):
        from services.scorer_matcher.main import MatcherState
        mock_ctx = Mock()
        state = MatcherState(mock_ctx)
        assert state.ctx is mock_ctx
        assert isinstance(state.stop_event, type(threading.Event()))
        assert state.consumer_task is None

    def test_state_can_hold_consumer_task(self):
        from services.scorer_matcher.main import MatcherState
        state = MatcherState(Mock())
        state.consumer_task = "dummy_task_reference"
        assert state.consumer_task == "dummy_task_reference"

    def test_stop_event_initially_not_set(self):
        from services.scorer_matcher.main import MatcherState
        assert MatcherState(Mock()).stop_event.is_set() is False

    def test_stop_event_can_be_set(self):
        from services.scorer_matcher.main import MatcherState
        state = MatcherState(Mock())
        state.stop_event.set()
        assert state.stop_event.is_set() is True


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TestMatcherModels:

    def test_match_response_valid(self):
        from services.scorer_matcher.main import MatchResponse
        r = MatchResponse(success=True, task_id="t-1", message="done")
        assert r.success is True
        assert r.task_id == "t-1"

    def test_match_response_with_matches(self):
        from services.scorer_matcher.main import MatchResponse
        r = MatchResponse(success=True, message="ok", matches=10, task_id="t-2")
        assert r.matches == 10

    def test_match_response_failure(self):
        from services.scorer_matcher.main import MatchResponse
        assert MatchResponse(success=False, message="fail").success is False


# ---------------------------------------------------------------------------
# Constants / env
# ---------------------------------------------------------------------------

class TestMatcherConstants:

    def test_stream_constants_defined(self):
        from services.scorer_matcher import main as m
        assert hasattr(m, "STREAM_MATCHING")
        assert hasattr(m, "CHANNEL_MATCHING_DONE")
        assert "matching" in m.STREAM_MATCHING.lower()
        assert "matching" in m.CHANNEL_MATCHING_DONE.lower()

    def test_consumer_group_default(self):
        import os, importlib
        import services.scorer_matcher.main as m
        backup = os.environ.pop("MATCHER_CONSUMER_GROUP", None)
        importlib.reload(m)
        assert m.CONSUMER_GROUP == "matcher-service"
        if backup:
            os.environ["MATCHER_CONSUMER_GROUP"] = backup

    def test_consumer_name_default(self):
        import os, importlib
        import services.scorer_matcher.main as m
        backup = os.environ.pop("HOSTNAME", None)
        importlib.reload(m)
        assert m.CONSUMER_NAME == "matcher-1"
        if backup:
            os.environ["HOSTNAME"] = backup


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

class TestMatcherLogging:

    def test_setup_logging_configures_logger(self):
        with patch("logging.basicConfig") as mock_bc:
            import services.scorer_matcher.main as m
            m._setup_logging()
        mock_bc.assert_called_once()
        assert mock_bc.call_args[1]["level"] == logging.INFO
        assert "format" in mock_bc.call_args[1]


# ---------------------------------------------------------------------------
# HTTP endpoints
# ---------------------------------------------------------------------------

class TestMatcherEndpoints:

    def test_health(self):
        from services.scorer_matcher.main import app
        r = TestClient(app).get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "healthy", "service": "matcher"}

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
        assert data["service"] == "matcher"
        assert data["consumer_running"] is True

    def test_stop_sets_stop_event(self, app_with_state):
        app, state = app_with_state
        mock_stop_event = Mock()
        state.stop_event = mock_stop_event
        r = TestClient(app).post("/match/stop")
        assert r.status_code == 200
        assert r.json() == {"success": True, "message": "Stop signal sent"}
        mock_stop_event.set.assert_called_once()

    def test_match_resume_success(self, app_with_state):
        app, state = app_with_state
        mock_result = Mock(saved_count=5)
        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=mock_result):
            r = TestClient(app).post("/match/resume",
                                     json={"resume_fingerprint": "fp-abc"})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["matches"] == 5

    def test_match_jobs_empty(self, app_with_state):
        app, _ = app_with_state
        r = TestClient(app).post("/match/jobs", json={"job_ids": []})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["matches"] == 0

    def test_match_jobs_not_implemented(self, app_with_state):
        app, _ = app_with_state
        r = TestClient(app).post("/match/jobs",
                                  json={"resume_fingerprint": "fp", "job_ids": ["1", "2"]})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is False
        assert "not yet implemented" in data["message"].lower()


# ---------------------------------------------------------------------------
# _process_matching_message
# ---------------------------------------------------------------------------

class TestProcessMatchingMessage:

    @pytest.mark.asyncio
    async def test_success_publishes_completion(self, matcher_state, no_to_thread):
        from services.scorer_matcher.main import _process_matching_message

        msg = {"task_id": "t-1", "resume_fingerprint": "fp-abc"}
        mock_result = Mock(saved_count=5)

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=mock_result), \
             patch("services.scorer_matcher.main.publish_completion") as mock_pub, \
             patch("services.scorer_matcher.main.ack_message") as mock_ack, \
             patch("services.scorer_matcher.main.logger") as mock_log:

            result = await _process_matching_message(matcher_state, "msg-1", msg)

        assert result is True
        mock_pub.assert_called_once()
        mock_ack.assert_called_once()
        published = mock_pub.call_args[0][1]
        assert published["status"] == "completed"
        assert published["matches_count"] == 5
        mock_log.info.assert_called()

    @pytest.mark.asyncio
    async def test_no_result_reports_zero_matches(self, matcher_state, no_to_thread):
        from services.scorer_matcher.main import _process_matching_message

        msg = {"task_id": "t-2", "resume_fingerprint": "fp-xyz"}

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   return_value=None), \
             patch("services.scorer_matcher.main.publish_completion") as mock_pub, \
             patch("services.scorer_matcher.main.ack_message"):

            result = await _process_matching_message(matcher_state, "msg-2", msg)

        assert result is True
        assert mock_pub.call_args[0][1]["matches_count"] == 0

    @pytest.mark.asyncio
    async def test_exception_publishes_failure_and_acks(self, matcher_state, no_to_thread):
        from services.scorer_matcher.main import _process_matching_message

        msg = {"task_id": "t-err", "resume_fingerprint": "fp-err"}

        with patch("services.scorer_matcher.main._run_matching_pipeline_sync",
                   side_effect=Exception("Model error")), \
             patch("services.scorer_matcher.main.publish_completion") as mock_pub, \
             patch("services.scorer_matcher.main.ack_message") as mock_ack, \
             patch("services.scorer_matcher.main.logger") as mock_log:

            result = await _process_matching_message(matcher_state, "msg-err", msg)

        assert result is False
        mock_pub.assert_called_once()
        mock_ack.assert_called_once()
        published = mock_pub.call_args[0][1]
        assert published["status"] == "failed"
        assert "Model error" in published["error"]
        mock_log.exception.assert_called_once()


# ---------------------------------------------------------------------------
# consume_matching_jobs
# ---------------------------------------------------------------------------

class TestConsumeMatchingJobs:

    @pytest.mark.asyncio
    async def test_processes_message_batch(self, matcher_state, no_to_thread):
        from services.scorer_matcher.main import consume_matching_jobs

        call_count = [0]

        def mock_read(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [("msg-1", {"task_id": "t1", "resume_fingerprint": "fp1"})]
            raise asyncio.CancelledError()

        with patch("services.scorer_matcher.main.read_stream", side_effect=mock_read), \
             patch("services.scorer_matcher.main._process_matching_message",
                   new_callable=AsyncMock, return_value=True) as mock_proc:

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_matching_jobs(matcher_state), timeout=5.0
                )

        mock_proc.assert_awaited_once_with(
            matcher_state, "msg-1", {"task_id": "t1", "resume_fingerprint": "fp1"}
        )

    @pytest.mark.asyncio
    async def test_empty_batch_continues_loop(self, matcher_state, no_to_thread):
        from services.scorer_matcher.main import consume_matching_jobs

        call_count = [0]

        def mock_read(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            raise asyncio.CancelledError()

        with patch("services.scorer_matcher.main.read_stream", side_effect=mock_read), \
             patch("services.scorer_matcher.main._process_matching_message",
                   new_callable=AsyncMock) as mock_proc:

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_matching_jobs(matcher_state), timeout=5.0
                )

        mock_proc.assert_not_awaited()
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_consumer_handles_exception(self, matcher_state, no_to_thread):
        """Exception from read_stream is logged; consumer backs off and retries."""
        from services.scorer_matcher.main import consume_matching_jobs

        call_count = [0]

        def mock_read(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Redis error")
            raise asyncio.CancelledError()  # clean exit on second call

        # FIX: patch asyncio.sleep as AsyncMock so the backoff await doesn't crash,
        # and drive loop exit via CancelledError from mock_read instead of task.cancel().
        with patch("services.scorer_matcher.main.read_stream", side_effect=mock_read), \
             patch("services.scorer_matcher.main.logger") as mock_log, \
             patch("services.scorer_matcher.main.asyncio.sleep", new_callable=AsyncMock):

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_matching_jobs(matcher_state), timeout=5.0
                )

        mock_log.exception.assert_called()
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates_cleanly(self, matcher_state, no_to_thread):
        from services.scorer_matcher.main import consume_matching_jobs

        with patch("services.scorer_matcher.main.read_stream",
                   side_effect=asyncio.CancelledError()):
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_matching_jobs(matcher_state), timeout=5.0
                )

    @pytest.mark.asyncio
    async def test_consumer_tracks_error_counts(self, matcher_state, no_to_thread):
        from services.scorer_matcher.main import consume_matching_jobs

        call_count = [0]

        def mock_read(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [("msg-1", {"task_id": "t1", "resume_fingerprint": "fp1"})]
            elif call_count[0] == 2:
                return [("msg-2", {"task_id": "t2", "resume_fingerprint": "fp2"})]
            raise asyncio.CancelledError()

        with patch("services.scorer_matcher.main.read_stream", side_effect=mock_read), \
             patch("services.scorer_matcher.main._process_matching_message",
                   new_callable=AsyncMock, return_value=False):

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_matching_jobs(matcher_state), timeout=5.0
                )

        assert call_count[0] == 3

    @pytest.mark.asyncio
    async def test_stop_event_exits_loop(self, matcher_state, no_to_thread):
        from services.scorer_matcher.main import consume_matching_jobs

        def mock_read(*args, **kwargs):
            matcher_state.stop_event.set()
            return []

        with patch("services.scorer_matcher.main.read_stream", side_effect=mock_read):
            await asyncio.wait_for(
                consume_matching_jobs(matcher_state), timeout=5.0
            )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

class TestMatcherAppLifespan:

    @pytest.mark.asyncio
    async def test_startup_sets_state_and_logs(self):
        from services.scorer_matcher.main import lifespan, MatcherState

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        mock_task = AsyncMock()
        mock_task.done.return_value = False

        with patch("services.scorer_matcher.main.logger") as mock_log, \
             patch("services.scorer_matcher.main.load_config", return_value={}), \
             patch("services.scorer_matcher.main.AppContext") as mock_ctx_class, \
             patch("services.scorer_matcher.main.consume_matching_jobs",
                   new_callable=AsyncMock), \
             patch("services.scorer_matcher.main.asyncio.create_task",
                   return_value=mock_task), \
             patch("services.scorer_matcher.main.asyncio.gather",
                   new_callable=AsyncMock):

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                assert hasattr(app.state, "matcher")
                assert isinstance(app.state.matcher, MatcherState)
                assert app.state.matcher.ctx is mock_ctx

        mock_log.info.assert_any_call("Starting matcher service...")
        mock_log.info.assert_any_call("Matcher service ready")

    @pytest.mark.asyncio
    async def test_shutdown_cancels_consumer_task(self):
        from services.scorer_matcher.main import lifespan

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        mock_task = AsyncMock()
        mock_task.done.return_value = False

        with patch("services.scorer_matcher.main.logger") as mock_log, \
             patch("services.scorer_matcher.main.load_config", return_value={}), \
             patch("services.scorer_matcher.main.AppContext") as mock_ctx_class, \
             patch("services.scorer_matcher.main.consume_matching_jobs",
                   new_callable=AsyncMock), \
             patch("services.scorer_matcher.main.asyncio.create_task",
                   return_value=mock_task), \
             patch("services.scorer_matcher.main.asyncio.gather",
                   new_callable=AsyncMock) as mock_gather:

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                lifespan_state = app.state.matcher

        mock_log.info.assert_any_call("Shutting down matcher service...")
        assert lifespan_state.stop_event.is_set()
        mock_task.cancel.assert_called_once()
        mock_gather.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shutdown_closes_app_context(self):
        from services.scorer_matcher.main import lifespan

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()

        with patch("services.scorer_matcher.main.load_config", return_value={}), \
             patch("services.scorer_matcher.main.AppContext") as mock_ctx_class, \
             patch("services.scorer_matcher.main.consume_matching_jobs",
                   new_callable=AsyncMock), \
             patch("services.scorer_matcher.main.asyncio.create_task",
                   return_value=AsyncMock()), \
             patch("services.scorer_matcher.main.asyncio.gather",
                   new_callable=AsyncMock):

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                pass

        mock_ctx.aclose.assert_awaited_once()
