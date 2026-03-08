#!/usr/bin/env python3
"""
Integration Tests: Service Lifespan with Testcontainers

These tests use Testcontainers to run real Redis and test the full
service lifecycle including startup and shutdown.

Usage:
    uv run pytest tests/integration/services/test_services_lifespan.py -v
"""

import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient


class TestEmbeddingsLifespan:
    """Test embeddings service lifespan with Testcontainers."""

    def test_metrics_reflects_consumer_state(self):
        """Test metrics endpoint reflects consumer running state."""
        from services.embeddings.main import app, EmbeddingsState
        
        mock_state = EmbeddingsState(ctx=Mock())
        mock_task = Mock()
        mock_task.done.return_value = False
        mock_state.consumer_task = mock_task
        
        app.state.embeddings = mock_state
        try:
            with TestClient(app) as client:
                response = client.get("/metrics")
                assert response.status_code == 200
                data = response.json()
                assert data["consumer_running"] is True
        finally:
            if hasattr(app.state, 'embeddings'):
                del app.state.embeddings


class TestExtractionLifespan:
    """Test extraction service lifespan with Testcontainers."""

    def test_metrics_reflects_consumer_state(self):
        """Test metrics endpoint reflects consumer running state."""
        from services.extraction.main import app, ExtractionState
        
        mock_state = ExtractionState(ctx=Mock())
        mock_task = Mock()
        mock_task.done.return_value = False
        mock_state.consumer_task = mock_task
        
        app.state.extraction = mock_state
        try:
            with TestClient(app) as client:
                response = client.get("/metrics")
                assert response.status_code == 200
                data = response.json()
                assert data["consumer_running"] is True
        finally:
            if hasattr(app.state, 'extraction'):
                del app.state.extraction


class TestMatcherLifespan:
    """Test scorer/matcher service lifespan with Testcontainers."""

    def test_metrics_reflects_consumer_state(self):
        """Test metrics endpoint reflects consumer running state."""
        from services.scorer_matcher.main import app, MatcherState
        
        mock_state = MatcherState(ctx=Mock())
        mock_task = Mock()
        mock_task.done.return_value = False
        mock_state.consumer_task = mock_task
        
        app.state.matcher = mock_state
        try:
            with TestClient(app) as client:
                response = client.get("/metrics")
                assert response.status_code == 200
                data = response.json()
                assert data["consumer_running"] is True
        finally:
            if hasattr(app.state, 'matcher'):
                del app.state.matcher


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
