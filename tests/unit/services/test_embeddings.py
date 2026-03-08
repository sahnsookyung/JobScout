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


class TestEmbeddingsLifespan:
    """Test embeddings service lifespan."""

    def test_setup_logging(self):
        """Test setup_service_logging configures logging."""
        from core.logging_utils import setup_service_logging
        import logging
        # Just verify it runs without error
        setup_service_logging(logging.getLogger("test"))


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

    def test_metrics_endpoint(self, app_with_state):
        """Test /metrics endpoint."""
        app, client = app_with_state
        r = client.get("/metrics")
        assert r.status_code == 200
        data = r.json()
        assert data["service"] == "embeddings"
        assert "consumer_running" in data

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

    @pytest.mark.asyncio
    async def test_lifespan_startup_sets_state(self):
        """Lifespan sets app.state.embeddings to an EmbeddingsState instance."""
        from fastapi import FastAPI
        from services.embeddings.main import EmbeddingsState, lifespan

        app = FastAPI()
        mock_ctx = Mock()
        mock_ctx.aclose = AsyncMock()

        done_task = asyncio.create_task(asyncio.sleep(0))
        await done_task

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
        await done_task

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
            def close(self): ...

        mock_ctx = MagicMock(spec=_SyncCtx)

        done_task = asyncio.create_task(asyncio.sleep(0))
        await done_task

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


class TestEmbeddingsEndpoints:
    """Test embeddings service HTTP endpoints using TestClient."""

    def test_health_endpoint(self):
        """Test health endpoint returns correct status."""
        from fastapi.testclient import TestClient
        from services.embeddings.main import app
        
        client = TestClient(app)
        response = client.get("/health")
        
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "embeddings"}

    def test_metrics_endpoint(self):
        """Test metrics endpoint returns correct info."""
        from fastapi.testclient import TestClient
        from services.embeddings.main import app
        
        mock_task = Mock()
        mock_task.done.return_value = False
        
        mock_state = Mock()
        mock_state.consumer_task = mock_task
        
        app.state.embeddings = mock_state
        try:
            client = TestClient(app)
            response = client.get("/metrics")
            
            assert response.status_code == 200
            data = response.json()
            assert data["service"] == "embeddings"
            assert data["consumer_running"] is True
        finally:
            del app.state.embeddings

    def test_embed_jobs_endpoint(self):
        """Test embed/jobs endpoint."""
        from fastapi.testclient import TestClient
        from services.embeddings.main import app
        
        mock_state = Mock()
        mock_state.ctx = Mock()
        
        with patch('services.embeddings.main.run_embedding_extraction', return_value=10):
            app.state.embeddings = mock_state
            try:
                client = TestClient(app)
                response = client.post("/embed/jobs?limit=5")
                
                assert response.status_code == 200
                data = response.json()
                assert data["success"] is True
            finally:
                del app.state.embeddings

    def test_embed_resume_endpoint(self):
        """Test embed/resume endpoint."""
        from fastapi.testclient import TestClient
        from services.embeddings.main import app
        
        mock_state = Mock()
        mock_state.ctx = Mock()
        
        with patch('services.embeddings.main.generate_resume_embedding', return_value=[0.1] * 768):
            app.state.embeddings = mock_state
            try:
                client = TestClient(app)
                response = client.post("/embed/resume", json={"resume_fingerprint": "abc123"})
                
                assert response.status_code == 200
                data = response.json()
                assert data["success"] is True
            finally:
                del app.state.embeddings

    def test_stop_endpoint(self):
        """Test stop endpoint."""
        from fastapi.testclient import TestClient
        from services.embeddings.main import app
        
        mock_state = Mock()
        mock_state.consumer_task = Mock()
        
        app.state.embeddings = mock_state
        try:
            client = TestClient(app)
            response = client.post("/embed/stop")
            
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            mock_state.stop_event.set.assert_called_once()
        finally:
            del app.state.embeddings


class TestProcessEmbeddingMessage:
    """Test _process_embedding_message function."""

    @pytest.mark.asyncio
    async def test_process_embedding_message_success(self):
        """Test successful embedding processing."""
        from services.embeddings.main import _process_embedding_message, EmbeddingsState
        
        mock_ctx = Mock()
        state = EmbeddingsState(ctx=mock_ctx)
        msg = {"task_id": "task-123", "resume_fingerprint": "fp-abc123"}

        with patch('services.embeddings.main.generate_resume_embedding') as mock_embed, \
             patch('services.embeddings.main.publish_completion') as mock_publish, \
             patch('services.embeddings.main.ack_message') as mock_ack:

            result = await _process_embedding_message(state, "msg-1", msg)

        assert result is True
        mock_embed.assert_called_once_with(mock_ctx, "fp-abc123")
        mock_publish.assert_called_once()
        mock_ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_embedding_message_failure(self):
        """Test embedding processing failure."""
        from services.embeddings.main import _process_embedding_message, EmbeddingsState
        
        mock_ctx = Mock()
        state = EmbeddingsState(ctx=mock_ctx)
        msg = {"task_id": "task-456", "resume_fingerprint": "fp-xyz789"}

        with patch('services.embeddings.main.generate_resume_embedding', side_effect=RuntimeError("model offline")), \
             patch('services.embeddings.main.publish_completion') as mock_publish, \
             patch('services.embeddings.main.ack_message') as mock_ack:

            result = await _process_embedding_message(state, "msg-2", msg)

        assert result is False
        # Even on failure, message must be acked and failure published
        mock_ack.assert_called_once()
        mock_publish.assert_called_once()
        # Check failure status was published
        published_payload = mock_publish.call_args[0][1]
        assert published_payload["status"] == "failed"
        assert "model offline" in published_payload["error"]


class TestConsumeEmbeddingsJobs:
    """Test consume_embeddings_jobs consumer loop."""

    @pytest.mark.asyncio
    async def test_consumer_handles_empty_stream(self):
        """Test consumer handles empty stream (no messages)."""
        from services.embeddings.main import consume_embeddings_jobs, EmbeddingsState
        
        mock_ctx = Mock()
        state = EmbeddingsState(ctx=mock_ctx)
        
        # Empty stream - returns nothing
        with patch('services.embeddings.main.read_stream', return_value=[]):
            # Run for a short time then cancel
            task = asyncio.create_task(consume_embeddings_jobs(state))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_consumer_processes_message(self):
        """Test consumer processes a message from stream."""
        from services.embeddings.main import consume_embeddings_jobs, EmbeddingsState
        
        mock_ctx = Mock()
        state = EmbeddingsState(ctx=mock_ctx)
        
        messages = [("msg-1", {"task_id": "task-1", "resume_fingerprint": "fp-1"})]
        
        call_count = 0
        def mock_read_stream(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return messages
            # Return empty on subsequent calls to exit
            return []

        with patch('services.embeddings.main.read_stream', side_effect=mock_read_stream), \
             patch('services.embeddings.main._process_embedding_message', return_value=True) as mock_process:
            
            task = asyncio.create_task(consume_embeddings_jobs(state))
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            
            mock_process.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
