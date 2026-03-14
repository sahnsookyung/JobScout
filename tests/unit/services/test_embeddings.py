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
        """Test setup_logging configures logging."""
        from services.embeddings.main import _setup_logging
        # Just verify it runs without error
        _setup_logging()


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

    def test_embed_jobs_endpoint(self, app_with_state):
        """Test /embed/jobs endpoint."""
        app, client = app_with_state
        with patch("services.embeddings.main.run_embedding_extraction", return_value=5):
            r = client.post("/embed/jobs", json={"limit": 100})
        assert r.status_code == 200
        data = r.json()
        assert data["success"] is True
        assert data["processed"] == 5

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
