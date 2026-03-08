#!/usr/bin/env python3
"""
Unit Tests: Service State and Metrics

These tests verify the service state management and metrics endpoints
without requiring external services (Redis, database) to be running.

Usage:
    uv run pytest tests/integration/services/test_services_lifespan.py -v
"""

import pytest
from unittest.mock import Mock
from fastapi.testclient import TestClient


class TestEmbeddingsMetrics:
    """Test embeddings service metrics endpoint."""

    def test_metrics_reflects_consumer_state(self):
        """Test metrics endpoint reflects consumer running state."""
        from services.embeddings.main import app, EmbeddingsState
        
        mock_state = EmbeddingsState(ctx=Mock())
        mock_task = Mock()
        mock_task.done.return_value = False
        mock_state.consumer_task = mock_task
        
        # Set state BEFORE creating TestClient - don't use context manager
        # to avoid triggering lifespan which would require Redis
        app.state.embeddings = mock_state
        try:
            client = TestClient(app)  # No `with` - lifespan does NOT run
            response = client.get("/metrics")
            assert response.status_code == 200
            data = response.json()
            assert data["consumer_running"] is True
        finally:
            if hasattr(app.state, 'embeddings'):
                del app.state.embeddings

    def test_metrics_consumer_not_running(self):
        """Test metrics when consumer is not running."""
        from services.embeddings.main import app, EmbeddingsState
        
        mock_state = EmbeddingsState(ctx=Mock())
        mock_state.consumer_task = None
        
        app.state.embeddings = mock_state
        try:
            client = TestClient(app)
            response = client.get("/metrics")
            assert response.status_code == 200
            data = response.json()
            assert data["consumer_running"] is False
        finally:
            if hasattr(app.state, 'embeddings'):
                del app.state.embeddings


class TestExtractionMetrics:
    """Test extraction service metrics endpoint."""

    def test_metrics_reflects_consumer_state(self):
        """Test metrics endpoint reflects consumer running state."""
        from services.extraction.main import app, ExtractionState
        
        mock_state = ExtractionState(ctx=Mock())
        mock_task = Mock()
        mock_task.done.return_value = False
        mock_state.consumer_task = mock_task
        
        app.state.extraction = mock_state
        try:
            client = TestClient(app)  # No context manager
            response = client.get("/metrics")
            assert response.status_code == 200
            data = response.json()
            assert data["consumer_running"] is True
        finally:
            if hasattr(app.state, 'extraction'):
                del app.state.extraction

    def test_metrics_consumer_not_running(self):
        """Test metrics when consumer is not running."""
        from services.extraction.main import app, ExtractionState
        
        mock_state = ExtractionState(ctx=Mock())
        mock_state.consumer_task = None
        
        app.state.extraction = mock_state
        try:
            client = TestClient(app)
            response = client.get("/metrics")
            assert response.status_code == 200
            data = response.json()
            assert data["consumer_running"] is False
        finally:
            if hasattr(app.state, 'extraction'):
                del app.state.extraction


class TestMatcherMetrics:
    """Test scorer/matcher service metrics endpoint."""

    def test_metrics_reflects_consumer_state(self):
        """Test metrics endpoint reflects consumer running state."""
        from services.scorer_matcher.main import app, MatcherState
        
        mock_state = MatcherState(ctx=Mock())
        mock_task = Mock()
        mock_task.done.return_value = False
        mock_state.consumer_task = mock_task
        
        app.state.matcher = mock_state
        try:
            client = TestClient(app)  # No context manager
            response = client.get("/metrics")
            assert response.status_code == 200
            data = response.json()
            assert data["consumer_running"] is True
        finally:
            if hasattr(app.state, 'matcher'):
                del app.state.matcher

    def test_metrics_consumer_not_running(self):
        """Test metrics when consumer is not running."""
        from services.scorer_matcher.main import app, MatcherState
        
        mock_state = MatcherState(ctx=Mock())
        mock_state.consumer_task = None
        
        app.state.matcher = mock_state
        try:
            client = TestClient(app)
            response = client.get("/metrics")
            assert response.status_code == 200
            data = response.json()
            assert data["consumer_running"] is False
        finally:
            if hasattr(app.state, 'matcher'):
                del app.state.matcher


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
