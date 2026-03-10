#!/usr/bin/env python3
"""
Tests for Embeddings Service.
Covers: services/embeddings/main.py
"""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def embeddings_state():
    from services.embeddings.main import EmbeddingsState
    return EmbeddingsState(ctx=Mock())


@pytest.fixture
def app_with_state(embeddings_state):
    """Attach state to the real app singleton and clean up after each test."""
    from services.embeddings.main import app
    app.state.embeddings = embeddings_state
    yield app, embeddings_state
    if hasattr(app.state, "embeddings"):
        del app.state.embeddings


@pytest.fixture
def no_to_thread():
    """
    Replace asyncio.to_thread with a direct synchronous call.

    _process_embedding_message and consume_embeddings_jobs wrap every blocking
    dependency in asyncio.to_thread(func, *args). In that model the function is
    called synchronously inside a thread pool — it is never awaited. Patching
    dependencies as AsyncMock therefore silently fails: side_effect never fires
    and assert_awaited_once() always reads zero.

    This fixture removes the indirection so that plain Mock() patches behave
    exactly as expected throughout the test.
    """
    async def passthrough(func, *args, **kwargs):
        return func(*args, **kwargs)

    with patch("asyncio.to_thread", side_effect=passthrough):
        yield


# ---------------------------------------------------------------------------
# /embed/stop
# ---------------------------------------------------------------------------

class TestEmbeddingsStopEndpoint:

    def test_stop_sets_stop_event(self, app_with_state):
        app, state = app_with_state
        mock_stop_event = Mock()
        state.stop_event = mock_stop_event

        response = TestClient(app).post("/embed/stop")

        assert response.status_code == 200
        assert response.json() == {"success": True, "message": "Stop signal sent"}
        mock_stop_event.set.assert_called_once()

    def test_stop_without_embeddings_state_returns_server_error(self):
        """Endpoint fails gracefully when the service never finished starting."""
        from services.embeddings.main import app
        if hasattr(app.state, "embeddings"):
            del app.state.embeddings

        response = TestClient(app, raise_server_exceptions=False).post("/embed/stop")
        assert response.status_code in (500, 503)


# ---------------------------------------------------------------------------
# _process_embedding_message
# ---------------------------------------------------------------------------

class TestProcessEmbeddingMessage:

    @pytest.mark.asyncio
    async def test_success_returns_true_and_publishes_completion(
        self, embeddings_state, no_to_thread
    ):
        """Happy path: embedding generated, completion published, message acked."""
        from services.embeddings.main import _process_embedding_message

        msg = {"task_id": "task-1", "resume_fingerprint": "fp-abc"}

        with patch("services.embeddings.main.generate_resume_embedding") as mock_embed,              patch("services.embeddings.main.publish_completion") as mock_publish,              patch("services.embeddings.main.ack_message") as mock_ack:

            result = await _process_embedding_message(embeddings_state, "msg-1", msg)

        assert result is True
        mock_embed.assert_called_once_with(embeddings_state.ctx, "fp-abc")
        mock_publish.assert_called_once()
        published = mock_publish.call_args[0][1]
        assert published["task_id"] == "task-1"
        assert published["status"] == "completed"
        assert published["resume_fingerprint"] == "fp-abc"
        mock_ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_long_fingerprint_preserved_verbatim_in_payload(
        self, embeddings_state, no_to_thread
    ):
        """Fingerprint is not truncated in the published payload (only in log preview)."""
        from services.embeddings.main import _process_embedding_message

        long_fp = "fp_" + "x" * 100
        msg = {"task_id": "task-2", "resume_fingerprint": long_fp}

        with patch("services.embeddings.main.generate_resume_embedding"),              patch("services.embeddings.main.publish_completion") as mock_publish,              patch("services.embeddings.main.ack_message"):

            await _process_embedding_message(embeddings_state, "msg-2", msg)

        published = mock_publish.call_args[0][1]
        assert published["resume_fingerprint"] == long_fp

    @pytest.mark.asyncio
    async def test_exception_returns_false_and_publishes_failure(
        self, embeddings_state, no_to_thread
    ):
        """Exception causes result=False, failed status published, message still acked."""
        from services.embeddings.main import _process_embedding_message

        msg = {"task_id": "task-err", "resume_fingerprint": "fp-err"}

        with patch("services.embeddings.main.generate_resume_embedding",
                   side_effect=Exception("embed failed")),              patch("services.embeddings.main.publish_completion") as mock_publish,              patch("services.embeddings.main.ack_message") as mock_ack,              patch("services.embeddings.main.logger") as mock_logger:

            result = await _process_embedding_message(embeddings_state, "msg-err", msg)

        assert result is False
        mock_logger.exception.assert_called_once()
        mock_publish.assert_called_once()
        published = mock_publish.call_args[0][1]
        assert published["status"] == "failed"
        assert "embed failed" in published["error"]
        # Message must be acked even on failure to avoid redelivery loop
        mock_ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_task_id_does_not_crash(self, embeddings_state, no_to_thread):
        from services.embeddings.main import _process_embedding_message

        with patch("services.embeddings.main.generate_resume_embedding"),              patch("services.embeddings.main.publish_completion"),              patch("services.embeddings.main.ack_message"):

            result = await _process_embedding_message(
                embeddings_state, "msg-x", {"resume_fingerprint": "fp-only"}
            )
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_missing_fingerprint_does_not_crash(self, embeddings_state, no_to_thread):
        from services.embeddings.main import _process_embedding_message

        with patch("services.embeddings.main.generate_resume_embedding"),              patch("services.embeddings.main.publish_completion"),              patch("services.embeddings.main.ack_message"):

            result = await _process_embedding_message(
                embeddings_state, "msg-x", {"task_id": "task-no-fp"}
            )
        assert isinstance(result, bool)


# ---------------------------------------------------------------------------
# consume_embeddings_jobs
#
# read_stream is called inside a lambda passed to asyncio.to_thread:
#   asyncio.to_thread(lambda: list(read_stream(...)))
# With no_to_thread, the lambda executes directly so read_stream patches work.
#
# The consumer loop exits via CancelledError only. All tests drive it to a
# natural CancelledError exit on a controlled iteration rather than relying
# on external task.cancel() timing.
#
# asyncio.sleep (1s backoff) is also patched to keep tests instant.
# ---------------------------------------------------------------------------

class TestConsumeEmbeddingsJobs:

    @pytest.mark.asyncio
    async def test_processes_message_batch(self, embeddings_state, no_to_thread):
        """Consumer calls _process_embedding_message for each message in the batch."""
        from services.embeddings.main import consume_embeddings_jobs

        call_count = [0]

        def mock_read(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [("msg-1", {"task_id": "t1", "resume_fingerprint": "fp1"})]
            raise asyncio.CancelledError()

        with patch("services.embeddings.main.read_stream", side_effect=mock_read),              patch("services.embeddings.main._process_embedding_message",
                   new_callable=AsyncMock, return_value=True) as mock_process:

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_embeddings_jobs(embeddings_state), timeout=5.0
                )

        mock_process.assert_awaited_once_with(
            embeddings_state, "msg-1", {"task_id": "t1", "resume_fingerprint": "fp1"}
        )

    @pytest.mark.asyncio
    async def test_empty_batch_continues_loop(self, embeddings_state, no_to_thread):
        """Empty read result does not process anything and loops again."""
        from services.embeddings.main import consume_embeddings_jobs

        call_count = [0]

        def mock_read(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            raise asyncio.CancelledError()

        with patch("services.embeddings.main.read_stream", side_effect=mock_read),              patch("services.embeddings.main._process_embedding_message",
                   new_callable=AsyncMock) as mock_process:

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_embeddings_jobs(embeddings_state), timeout=5.0
                )

        mock_process.assert_not_awaited()
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_read_exception_is_logged_and_loop_continues(
        self, embeddings_state, no_to_thread
    ):
        """Generic exception from read_stream is logged; consumer backs off and retries."""
        from services.embeddings.main import consume_embeddings_jobs

        call_count = [0]

        def mock_read(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Redis connection lost")
            raise asyncio.CancelledError()

        with patch("services.embeddings.main.read_stream", side_effect=mock_read),              patch("services.embeddings.main.logger") as mock_logger,              patch("asyncio.sleep", new_callable=AsyncMock):

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_embeddings_jobs(embeddings_state), timeout=5.0
                )

        mock_logger.exception.assert_called()
        assert call_count[0] == 2

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates_cleanly(self, embeddings_state, no_to_thread):
        """CancelledError is not swallowed — it propagates out of the consumer."""
        from services.embeddings.main import consume_embeddings_jobs

        def mock_read(*args, **kwargs):
            raise asyncio.CancelledError()

        with patch("services.embeddings.main.read_stream", side_effect=mock_read):
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    consume_embeddings_jobs(embeddings_state), timeout=5.0
                )

    @pytest.mark.asyncio
    async def test_stop_event_exits_loop(self, embeddings_state, no_to_thread):
        """Consumer exits naturally when stop_event is set between iterations."""
        from services.embeddings.main import consume_embeddings_jobs

        def mock_read(*args, **kwargs):
            embeddings_state.stop_event.set()
            return []

        with patch("services.embeddings.main.read_stream", side_effect=mock_read):
            # Must return cleanly — no CancelledError, no timeout
            await asyncio.wait_for(
                consume_embeddings_jobs(embeddings_state), timeout=5.0
            )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

class TestEmbeddingsAppLifespan:

    @pytest.mark.asyncio
    async def test_startup_sets_state_and_logs(self):
        """Startup attaches EmbeddingsState to app and logs expected messages."""
        from services.embeddings.main import lifespan, EmbeddingsState

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        mock_task = AsyncMock()
        mock_task.done.return_value = False

        with patch("services.embeddings.main.logger") as mock_logger, \
             patch("services.embeddings.main.load_config", return_value={}), \
             patch("services.embeddings.main.AppContext") as mock_ctx_class, \
             patch("services.embeddings.main.consume_embeddings_jobs",
                   new_callable=AsyncMock), \
             patch("services.embeddings.main.asyncio.create_task",
                   return_value=mock_task), \
             patch("services.embeddings.main.asyncio.gather", new_callable=AsyncMock):

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                assert hasattr(app.state, "embeddings")
                assert isinstance(app.state.embeddings, EmbeddingsState)
                assert app.state.embeddings.ctx is mock_ctx

        mock_logger.info.assert_any_call("Starting embeddings service...")
        mock_logger.info.assert_any_call("Embeddings service ready")

    @pytest.mark.asyncio
    async def test_shutdown_cancels_consumer_task(self):
        """Shutdown signals stop_event, cancels the consumer task, and awaits it."""
        from services.embeddings.main import lifespan

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()
        mock_task = AsyncMock()
        mock_task.done.return_value = False

        with patch("services.embeddings.main.logger") as mock_logger, \
             patch("services.embeddings.main.load_config", return_value={}), \
             patch("services.embeddings.main.AppContext") as mock_ctx_class, \
             patch("services.embeddings.main.consume_embeddings_jobs",
                   new_callable=AsyncMock), \
             patch("services.embeddings.main.asyncio.create_task",
                   return_value=mock_task), \
             patch("services.embeddings.main.asyncio.gather",
                   new_callable=AsyncMock) as mock_gather:

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                lifespan_state = app.state.embeddings  # capture before teardown

        mock_logger.info.assert_any_call("Shutting down embeddings service...")
        assert lifespan_state.stop_event.is_set()  # lifespan sets stop_event before cancel
        mock_task.cancel.assert_called_once()
        mock_gather.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shutdown_closes_app_context(self):
        """Shutdown calls aclose() on the AppContext."""
        from services.embeddings.main import lifespan

        app = FastAPI(lifespan=lifespan)
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()

        with patch("services.embeddings.main.load_config", return_value={}), \
             patch("services.embeddings.main.AppContext") as mock_ctx_class, \
             patch("services.embeddings.main.consume_embeddings_jobs",
                   new_callable=AsyncMock), \
             patch("services.embeddings.main.asyncio.create_task",
                   return_value=AsyncMock()), \
             patch("services.embeddings.main.asyncio.gather", new_callable=AsyncMock):

            mock_ctx_class.build.return_value = mock_ctx

            async with lifespan(app):
                pass

        mock_ctx.aclose.assert_awaited_once()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TestEmbeddingsModels:

    def test_embed_job_request_default_limit(self):
        from services.embeddings.main import EmbedJobRequest
        assert EmbedJobRequest().limit == 100

    def test_embed_job_request_custom_limit(self):
        from services.embeddings.main import EmbedJobRequest
        assert EmbedJobRequest(limit=50).limit == 50

    def test_embed_resume_request_valid(self):
        from services.embeddings.main import EmbedResumeRequest
        assert EmbedResumeRequest(resume_fingerprint="abc123").resume_fingerprint == "abc123"

    def test_embed_resume_request_requires_fingerprint(self):
        from pydantic import ValidationError
        from services.embeddings.main import EmbedResumeRequest
        with pytest.raises(ValidationError):
            EmbedResumeRequest()

    def test_embed_response_default_processed(self):
        from services.embeddings.main import EmbedResponse
        assert EmbedResponse(success=True, message="OK").processed == 0

    def test_embed_response_custom_processed(self):
        from services.embeddings.main import EmbedResponse
        assert EmbedResponse(success=True, message="OK", processed=10).processed == 10


# ---------------------------------------------------------------------------
# /metrics
# ---------------------------------------------------------------------------

class TestEmbeddingsMetrics:

    def test_consumer_running_false_when_no_task(self, app_with_state):
        app, state = app_with_state
        state.consumer_task = None
        assert TestClient(app).get("/metrics").json()["consumer_running"] is False

    def test_consumer_running_false_when_task_done(self, app_with_state):
        app, state = app_with_state
        state.consumer_task = Mock(done=Mock(return_value=True))
        assert TestClient(app).get("/metrics").json()["consumer_running"] is False

    def test_consumer_running_true_when_task_active(self, app_with_state):
        app, state = app_with_state
        state.consumer_task = Mock(done=Mock(return_value=False))
        assert TestClient(app).get("/metrics").json()["consumer_running"] is True
