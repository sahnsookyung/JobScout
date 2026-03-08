#!/usr/bin/env python3
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
from unittest.mock import Mock, patch, MagicMock


class TestEmbeddingsState:
    """Test EmbeddingsState class."""

    def test_state_initialization(self):
        """Test EmbeddingsState initializes correctly."""
        from services.embeddings.main import EmbeddingsState
        
        mock_ctx = Mock()
        state = EmbeddingsState(mock_ctx)
        
        assert state.ctx is mock_ctx
        assert isinstance(state.stop_event, type(threading.Event()))
        assert state.consumer_task is None

    def test_state_can_hold_consumer_task(self):
        """Test that state can store a consumer task."""
        from services.embeddings.main import EmbeddingsState
        
        mock_ctx = Mock()
        state = EmbeddingsState(mock_ctx)
        
        # Just verify the attribute can be set (don't create actual tasks)
        state.consumer_task = "dummy_task_reference"
        
        assert state.consumer_task == "dummy_task_reference"

    def test_stop_event_initially_not_set(self):
        """Test that stop event is not set on initialization."""
        from services.embeddings.main import EmbeddingsState
        
        mock_ctx = Mock()
        state = EmbeddingsState(mock_ctx)
        
        assert state.stop_event.is_set() is False

    def test_stop_event_can_be_set(self):
        """Test that stop event can be set."""
        from services.embeddings.main import EmbeddingsState
        
        mock_ctx = Mock()
        state = EmbeddingsState(mock_ctx)
        
        state.stop_event.set()
        
        assert state.stop_event.is_set() is True


class TestEmbeddingsLifespan:
    """Test embeddings service lifespan management."""

    def test_setup_logging_configures_logger(self):
        """Test that _setup_logging configures logging correctly."""
        import logging
        
        with patch('logging.basicConfig') as mock_basic_config:
            import services.embeddings.main as embeddings_module
            embeddings_module._setup_logging()
            
            mock_basic_config.assert_called_once()
            call_kwargs = mock_basic_config.call_args[1]
            assert call_kwargs['level'] == logging.INFO
            assert 'format' in call_kwargs


class TestEmbeddingsModels:
    """Test pydantic models in embeddings service."""

    def test_embed_response_model(self):
        """Test EmbedResponse model validation."""
        from services.embeddings.main import EmbedResponse
        
        response = EmbedResponse(
            success=True,
            message="Embedding created"
        )
        
        assert response.success is True
        assert response.message == "Embedding created"

    def test_embed_response_model_with_optional_fields(self):
        """Test EmbedResponse model with optional fields."""
        from services.embeddings.main import EmbedResponse
        
        response = EmbedResponse(
            success=True,
            message="Processed",
            processed=5
        )
        
        assert response.success is True
        assert response.processed == 5


class TestEmbeddingsConsumer:
    """Test embeddings consumer logic."""

    def test_consumer_group_from_env_default(self):
        """Test default consumer group name."""
        with patch.dict('os.environ', {}, clear=False):
            # Remove env var if set
            import os
            env_backup = os.environ.get('EMBEDDINGS_CONSUMER_GROUP')
            if 'EMBEDDINGS_CONSUMER_GROUP' in os.environ:
                del os.environ['EMBEDDINGS_CONSUMER_GROUP']
            
            # Reload module to get default value
            import importlib
            import services.embeddings.main as embeddings_module
            importlib.reload(embeddings_module)
            
            assert embeddings_module.CONSUMER_GROUP == "embeddings-service"
            
            # Restore
            if env_backup:
                os.environ['EMBEDDINGS_CONSUMER_GROUP'] = env_backup

    def test_consumer_name_from_env_default(self):
        """Test default consumer name."""
        import os
        env_backup = os.environ.get('HOSTNAME')
        if 'HOSTNAME' in os.environ:
            del os.environ['HOSTNAME']
        
        import importlib
        import services.embeddings.main as embeddings_module
        importlib.reload(embeddings_module)
        
        assert embeddings_module.CONSUMER_NAME == "embeddings-1"
        
        if env_backup:
            os.environ['HOSTNAME'] = env_backup


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
