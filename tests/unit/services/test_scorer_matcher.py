#!/usr/bin/env python3
"""
Unit Tests: Scorer/Matcher Service

Tests the scorer/matcher service functionality without requiring
running services. Tests state management and models.

Usage:
    uv run pytest tests/unit/services/test_scorer_matcher.py -v
"""

import asyncio
import pytest
import threading
from unittest.mock import Mock, patch, MagicMock


class TestMatcherState:
    """Test MatcherState class."""

    def test_state_initialization(self):
        """Test MatcherState initializes correctly."""
        from services.scorer_matcher.main import MatcherState
        
        mock_ctx = Mock()
        state = MatcherState(mock_ctx)
        
        assert state.ctx is mock_ctx
        assert isinstance(state.stop_event, type(threading.Event()))
        assert state.consumer_task is None

    def test_state_can_hold_consumer_task(self):
        """Test that state can store a consumer task."""
        from services.scorer_matcher.main import MatcherState
        
        mock_ctx = Mock()
        state = MatcherState(mock_ctx)
        
        # Just verify the attribute can be set (don't create actual tasks)
        state.consumer_task = "dummy_task_reference"
        
        assert state.consumer_task == "dummy_task_reference"

    def test_stop_event_initially_not_set(self):
        """Test that stop event is not set on initialization."""
        from services.scorer_matcher.main import MatcherState
        
        mock_ctx = Mock()
        state = MatcherState(mock_ctx)
        
        assert state.stop_event.is_set() is False

    def test_stop_event_can_be_set(self):
        """Test that stop event can be set."""
        from services.scorer_matcher.main import MatcherState
        
        mock_ctx = Mock()
        state = MatcherState(mock_ctx)
        
        state.stop_event.set()
        
        assert state.stop_event.is_set() is True


class TestMatcherLifespan:
    """Test scorer/matcher service lifespan management."""

    def test_setup_logging_configures_logger(self):
        """Test that _setup_logging configures logging correctly."""
        import logging
        
        with patch('logging.basicConfig') as mock_basic_config:
            import services.scorer_matcher.main as matcher_module
            matcher_module._setup_logging()
            
            mock_basic_config.assert_called_once()
            call_kwargs = mock_basic_config.call_args[1]
            assert call_kwargs['level'] == logging.INFO
            assert 'format' in call_kwargs


class TestMatcherModels:
    """Test pydantic models in scorer/matcher service."""

    def test_match_response_model(self):
        """Test MatchResponse model validation."""
        from services.scorer_matcher.main import MatchResponse
        
        response = MatchResponse(
            success=True,
            task_id="test-123",
            message="Matching complete"
        )
        
        assert response.success is True
        assert response.task_id == "test-123"
        assert response.message == "Matching complete"

    def test_match_response_model_with_matches(self):
        """Test MatchResponse model with matches count."""
        from services.scorer_matcher.main import MatchResponse
        
        response = MatchResponse(
            success=True,
            message="Found matches",
            matches=10,
            task_id="test-456"
        )
        
        assert response.success is True
        assert response.matches == 10
        assert response.task_id == "test-456"

    def test_match_response_model_failure(self):
        """Test MatchResponse model with failure."""
        from services.scorer_matcher.main import MatchResponse
        
        response = MatchResponse(
            success=False,
            message="Matching failed"
        )
        
        assert response.success is False


class TestMatcherConsumer:
    """Test scorer/matcher consumer configuration."""

    def test_consumer_group_from_env_default(self):
        """Test default consumer group name."""
        with patch.dict('os.environ', {}, clear=False):
            import os
            env_backup = os.environ.get('MATCHER_CONSUMER_GROUP')
            if 'MATCHER_CONSUMER_GROUP' in os.environ:
                del os.environ['MATCHER_CONSUMER_GROUP']
            
            import importlib
            import services.scorer_matcher.main as matcher_module
            importlib.reload(matcher_module)
            
            assert matcher_module.CONSUMER_GROUP == "matcher-service"
            
            if env_backup:
                os.environ['MATCHER_CONSUMER_GROUP'] = env_backup

    def test_consumer_name_from_env_default(self):
        """Test default consumer name."""
        import os
        env_backup = os.environ.get('HOSTNAME')
        if 'HOSTNAME' in os.environ:
            del os.environ['HOSTNAME']
        
        import importlib
        import services.scorer_matcher.main as matcher_module
        importlib.reload(matcher_module)
        
        assert matcher_module.CONSUMER_NAME == "matcher-1"
        
        if env_backup:
            os.environ['HOSTNAME'] = env_backup


class TestMatcherConstants:
    """Test constants used in matcher service."""

    def test_stream_constants_defined(self):
        """Test that stream constants are defined."""
        from services.scorer_matcher import main as matcher_module
        
        assert hasattr(matcher_module, 'STREAM_MATCHING')
        assert hasattr(matcher_module, 'CHANNEL_MATCHING_DONE')
        assert 'matching' in matcher_module.STREAM_MATCHING.lower()
        assert 'matching' in matcher_module.CHANNEL_MATCHING_DONE.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
