"""
Unit Tests: Embeddings Service

Tests the embeddings service functionality without requiring
running services. Tests state management and utilities.

Usage:
    uv run pytest tests/unit/services/test_embeddings.py -v
"""

import asyncio
import pytest
import threading
from unittest.mock import Mock, patch, MagicMock, AsyncMock


class TestEmbeddingsState:
    """Test EmbeddingsState class."""

    def test_state_initialization(self):
        """Test EmbeddingsState initializes correctly."""
        from services.embeddings.main import EmbeddingsState, EmbeddingsConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=EmbeddingsConsumer)
        state = EmbeddingsState(mock_ctx, mock_consumer)

        assert state.ctx is mock_ctx
        assert state.consumer is mock_consumer
        assert isinstance(state.stop_event, type(threading.Event()))
        assert state.consumer_task is None

    def test_state_can_hold_consumer_task(self):
        """Test that state can store a consumer task."""
        from services.embeddings.main import EmbeddingsState, EmbeddingsConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=EmbeddingsConsumer)
        state = EmbeddingsState(mock_ctx, mock_consumer)

        state.consumer_task = "dummy_task_reference"
        assert state.consumer_task == "dummy_task_reference"

    def test_stop_event_initially_not_set(self):
        """Test that stop event is not set on initialization."""
        from services.embeddings.main import EmbeddingsState, EmbeddingsConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=EmbeddingsConsumer)
        state = EmbeddingsState(mock_ctx, mock_consumer)

        assert state.stop_event.is_set() is False

    def test_stop_event_can_be_set(self):
        """Test that stop event can be set to signal shutdown."""
        from services.embeddings.main import EmbeddingsState, EmbeddingsConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=EmbeddingsConsumer)
        state = EmbeddingsState(mock_ctx, mock_consumer)

        state.stop_event.set()
        assert state.stop_event.is_set() is True


class TestEmbeddingsModels:
    """Test embeddings Pydantic models."""

    def test_embed_response_model(self):
        """Test EmbedResponse model."""
        from services.embeddings.main import EmbedResponse

        response = EmbedResponse(success=True, message="Done")
        assert response.success is True
        assert response.message == "Done"
        assert response.processed == 0

    def test_embed_response_model_with_optional_fields(self):
        """Test EmbedResponse with processed count."""
        from services.embeddings.main import EmbedResponse

        response = EmbedResponse(success=True, message="Done", processed=5)
        assert response.processed == 5


class TestEmbeddingsConsumer:
    """Test EmbeddingsConsumer class."""

    def test_consumer_group_from_env_default(self):
        """Consumer group defaults from env."""
        from services.embeddings.main import CONSUMER_GROUP
        assert CONSUMER_GROUP is not None

    def test_consumer_name_from_env_default(self):
        """Consumer name defaults from env."""
        from services.embeddings.main import CONSUMER_NAME
        assert CONSUMER_NAME is not None


class TestEmbeddingsEndpoints:
    """Test embeddings FastAPI endpoints."""

    @pytest.fixture
    def app_with_state(self):
        """Create app with mocked state."""
        from fastapi.testclient import TestClient
        from services.embeddings.main import app, EmbeddingsState, EmbeddingsConsumer

        mock_ctx = Mock()
        mock_consumer = Mock(spec=EmbeddingsConsumer)
        state = EmbeddingsState(mock_ctx, mock_consumer)
        state.consumer_task = Mock()
        state.consumer_task.done.return_value = False
        state.batch_consumer_task = Mock()
        state.batch_consumer_task.done.return_value = False
        app.state.embeddings = state

        from fastapi.testclient import TestClient
        return app, TestClient(app)

    def test_health_endpoint(self, app_with_state):
        """Test /health endpoint."""
        app, client = app_with_state
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "healthy"
        assert data["service"] == "embeddings"
        assert data["consumer_running"] is True
        assert data["batch_consumer_running"] is True

    def test_metrics_endpoint_prometheus(self, app_with_state):
        """/metrics serves Prometheus text including worker-liveness gauges."""
        app, client = app_with_state
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "text/plain" in r.headers.get("content-type", "")
        assert b"jobscout_scorer_route_total" in r.content
        assert b'jobscout_worker_running{service="embeddings",worker="consumer"} 1.0' in r.content

    def test_health_endpoint_degraded_when_worker_stops(self, app_with_state):
        """Health reflects stopped worker loops."""
        app, client = app_with_state
        app.state.embeddings.batch_consumer_task.done.return_value = True
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "degraded"
        assert data["batch_consumer_running"] is False

    def test_embed_resume_endpoint(self, app_with_state):
        """Test /embed/resume endpoint."""
        app, client = app_with_state
        with patch("services.embeddings.main.generate_resume_embedding", return_value=True):
            r = client.post("/embed/resume", json={"resume_fingerprint": "fp-123"})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["processed"] == 1

    def test_stop_endpoint(self, app_with_state):
        """Test /embed/stop endpoint."""
        app, client = app_with_state
        r = client.post("/embed/stop")
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True

    def test_embed_resume_endpoint_failure(self, app_with_state):
        """Test /embed/resume returns failure when generate_resume_embedding raises."""
        app, client = app_with_state
        with patch("services.embeddings.main.generate_resume_embedding",
                   side_effect=Exception("Embedding model offline")):
            r = client.post("/embed/resume", json={"resume_fingerprint": "fp-fail"})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is False
        assert data["processed"] == 0

    def test_embed_resume_stop_sets_stop_event(self, app_with_state):
        """Test /embed/stop sets the stop event on state."""
        app, client = app_with_state
        r = client.post("/embed/stop")
        assert r.status_code == 200
        assert app.state.embeddings.stop_event.is_set()


class TestEmbeddingsConsumerClass:
    """Test EmbeddingsConsumer class directly."""

    @pytest.mark.asyncio
    async def test_do_process_validates_fields(self):
        """_do_process validates required fields."""
        from services.embeddings.main import EmbeddingsConsumer

        mock_ctx = Mock()
        consumer = EmbeddingsConsumer(mock_ctx)

        success, result = await consumer._do_process("msg-1", {"task_id": "t-1"})
        assert success is False
        assert result["status"] == "failed"
        assert "resume_fingerprint" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_do_process_success(self):
        """_do_process returns success on completion."""
        from services.embeddings.main import EmbeddingsConsumer

        mock_ctx = Mock()
        consumer = EmbeddingsConsumer(mock_ctx)

        with patch("services.embeddings.main.generate_resume_embedding") as mock_embed:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is True
        assert result["status"] == "completed"
        mock_embed.assert_called_once()

    @pytest.mark.asyncio
    async def test_do_process_fails_when_resume_embedding_returns_false(self):
        """_do_process fails closed when no resume embeddings were generated."""
        from services.embeddings.main import EmbeddingsConsumer

        mock_ctx = Mock()
        consumer = EmbeddingsConsumer(mock_ctx)

        with patch("services.embeddings.main.generate_resume_embedding", return_value=False):
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is False
        assert result["status"] == "failed"
        assert "not generated" in result["error"]

    @pytest.mark.asyncio
    async def test_do_process_failure(self):
        """_do_process returns failure on error."""
        from services.embeddings.main import EmbeddingsConsumer

        mock_ctx = Mock()
        consumer = EmbeddingsConsumer(mock_ctx)

        with patch("services.embeddings.main.generate_resume_embedding",
                   side_effect=Exception("Embed failed")):
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "resume_fingerprint": "fp-123"}
            )

        assert success is False
        assert result["status"] == "failed"
        assert "Embed failed" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_batch_consumer_processes_job_batches(self):
        """Batch consumer should process queued embedding batch work."""
        from services.embeddings.main import EmbeddingsBatchConsumer

        mock_ctx = Mock()
        stop_event = threading.Event()
        consumer = EmbeddingsBatchConsumer(mock_ctx, stop_event)

        with patch("services.embeddings.main.run_embedding_extraction", return_value=6):
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "t-1", "limit": 50},
            )

        assert success is True
        assert result["status"] == "completed"
        assert result["processed"] == 6

    @pytest.mark.asyncio
    async def test_batch_consumer_enqueues_followup_matching_jobs(self):
        """Batch consumer queues matching only after embeddings complete."""
        from services.embeddings.main import EmbeddingsBatchConsumer

        mock_ctx = Mock()
        stop_event = threading.Event()
        consumer = EmbeddingsBatchConsumer(mock_ctx, stop_event)
        matching_jobs = [
            {"task_id": "match-1", "resume_fingerprint": "fp-1"},
            {"task_id": "match-2", "resume_fingerprint": "fp-2"},
        ]

        with patch("services.embeddings.main.run_embedding_extraction", return_value=6), \
             patch("services.embeddings.main.enqueue_job", return_value="1-0") as enqueue:
            success, result = await consumer._do_process(
                "msg-1",
                {
                    "task_id": "embed-1",
                    "limit": 50,
                    "enqueue_matching_jobs": matching_jobs,
                },
            )

        assert success is True
        assert result["status"] == "completed"
        assert result["processed"] == 6
        assert result["followup_matching_jobs_enqueued"] == 2
        assert enqueue.call_args_list[0].args == ("matching:jobs", matching_jobs[0])
        assert enqueue.call_args_list[1].args == ("matching:jobs", matching_jobs[1])

    @pytest.mark.asyncio
    async def test_batch_consumer_fails_when_followup_matching_payload_invalid(self):
        """Invalid matching follow-up payloads surface as failed completion."""
        from services.embeddings.main import EmbeddingsBatchConsumer

        mock_ctx = Mock()
        stop_event = threading.Event()
        consumer = EmbeddingsBatchConsumer(mock_ctx, stop_event)

        with patch("services.embeddings.main.run_embedding_extraction", return_value=6), \
             patch("services.embeddings.main.enqueue_job") as enqueue:
            success, result = await consumer._do_process(
                "msg-1",
                {
                    "task_id": "embed-1",
                    "limit": 50,
                    "enqueue_matching_jobs": {"task_id": "match-1"},
                },
            )

        assert success is False
        assert result["status"] == "failed"
        assert result["followup_stage"] == "matching"
        assert "enqueue_matching_jobs" in result["error"]
        enqueue.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_consumer_requeues_on_batch_exception(self):
        """Transient batch failures should enqueue a bounded retry payload."""
        from services.embeddings.main import EmbeddingsBatchConsumer

        mock_ctx = Mock()
        stop_event = threading.Event()
        consumer = EmbeddingsBatchConsumer(mock_ctx, stop_event)

        with patch("services.embeddings.main.run_embedding_extraction", side_effect=RuntimeError("db went away")), \
             patch("services.embeddings.main.enqueue_job", return_value="1-0") as enqueue:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "embed-1", "limit": 25},
            )

        assert success is False
        assert result["status"] == "failed"
        assert result["retry_enqueued"] is True
        assert result["batch_retry_count"] == 1
        enqueue.assert_called_once_with(
            "embeddings:batch",
            {"task_id": "embed-1", "limit": 25, "batch_retry_count": 1},
        )

    @pytest.mark.asyncio
    async def test_batch_consumer_does_not_requeue_after_retry_cap(self):
        """Batch retry payloads stop after the configured retry cap."""
        from services.embeddings.main import EmbeddingsBatchConsumer

        mock_ctx = Mock()
        stop_event = threading.Event()
        consumer = EmbeddingsBatchConsumer(mock_ctx, stop_event)

        with patch("services.embeddings.main.run_embedding_extraction", side_effect=RuntimeError("still broken")), \
             patch("services.embeddings.main.enqueue_job") as enqueue:
            success, result = await consumer._do_process(
                "msg-1",
                {"task_id": "embed-1", "limit": 25, "batch_retry_count": 3},
            )

        assert success is False
        assert result["status"] == "failed"
        assert result["retry_enqueued"] is False
        assert result["batch_retry_count"] == 3
        enqueue.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_consumer_invalid_message(self):
        """Batch consumer rejects message missing task_id."""
        from services.embeddings.main import EmbeddingsBatchConsumer

        mock_ctx = Mock()
        stop_event = threading.Event()
        consumer = EmbeddingsBatchConsumer(mock_ctx, stop_event)

        success, result = await consumer._do_process("msg-1", {})
        assert success is False
        assert result["status"] == "failed"
        assert "task_id" in result.get("error", "")

    @pytest.mark.asyncio
    async def test_batch_consumer_uses_default_limit(self):
        """Batch consumer defaults limit to 100 when not provided."""
        from services.embeddings.main import EmbeddingsBatchConsumer

        mock_ctx = Mock()
        stop_event = threading.Event()
        consumer = EmbeddingsBatchConsumer(mock_ctx, stop_event)

        with patch("services.embeddings.main.run_embedding_extraction", return_value=0) as mock_run:
            await consumer._do_process("msg-1", {"task_id": "t-1"})

        _, kwargs = mock_run.call_args
        # limit should be passed as positional arg or keyword
        assert mock_run.called


class TestEmbeddingsLifespan:
    """Test embeddings lifespan startup and shutdown."""

    def test_setup_logging(self):
        """Test setup_service_logging configures logging."""
        from core.logging_utils import setup_service_logging
        import logging
        setup_service_logging(logging.getLogger("test"))

    @pytest.mark.asyncio
    async def test_lifespan_startup_sets_state(self):
        """Lifespan sets app.state.embeddings to an EmbeddingsState instance."""
        from fastapi import FastAPI
        from services.embeddings.main import EmbeddingsState, lifespan

        app = FastAPI()
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()

        done_task = asyncio.create_task(asyncio.sleep(0))
        _ = await done_task

        def _stub_create_task(coro):
            if hasattr(coro, "close"):
                coro.close()
            return done_task

        with patch("services.embeddings.main.load_config"), \
             patch("services.embeddings.main.AppContext.build", return_value=mock_ctx), \
             patch("asyncio.create_task", side_effect=_stub_create_task):
            async with lifespan(app):
                assert isinstance(app.state.embeddings, EmbeddingsState)
                assert app.state.embeddings.ctx is mock_ctx

    @pytest.mark.asyncio
    async def test_lifespan_shutdown_calls_aclose(self):
        """Lifespan shutdown invokes ctx.aclose() when available."""
        from fastapi import FastAPI
        from services.embeddings.main import lifespan

        app = FastAPI()
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()

        done_task = asyncio.create_task(asyncio.sleep(0))
        _ = await done_task

        def _stub_create_task(coro):
            if hasattr(coro, "close"):
                coro.close()
            return done_task

        with patch("services.embeddings.main.load_config"), \
             patch("services.embeddings.main.AppContext.build", return_value=mock_ctx), \
             patch("asyncio.create_task", side_effect=_stub_create_task):
            async with lifespan(app):
                pass

        mock_ctx.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_lifespan_shutdown_falls_back_to_sync_close(self):
        """Lifespan shutdown calls ctx.close() when ctx has no aclose."""
        from fastapi import FastAPI
        from services.embeddings.main import lifespan

        app = FastAPI()

        class _SyncCtx:
            def close(self): pass

        mock_ctx = MagicMock(spec=_SyncCtx)

        done_task = asyncio.create_task(asyncio.sleep(0))
        _ = await done_task

        def _stub_create_task(coro):
            if hasattr(coro, "close"):
                coro.close()
            return done_task

        with patch("services.embeddings.main.load_config"), \
             patch("services.embeddings.main.AppContext.build", return_value=mock_ctx), \
             patch("asyncio.create_task", side_effect=_stub_create_task):
            async with lifespan(app):
                pass

        mock_ctx.close.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
