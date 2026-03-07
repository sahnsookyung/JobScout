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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
