"""
Tests for web/backend/routers/stats.py

Covers the /api/stats GET endpoint: total counts, hidden, below threshold,
active matches, score distribution buckets.
"""

import pytest
from unittest.mock import MagicMock, Mock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.backend.routers.stats import router
from web.backend.dependencies import get_current_user, get_db


def _make_query_mock(total=10, hidden=2, below_threshold=1, excellent=4, good=2, average=2, poor=2):
    """Build a mock DB session whose query chain returns the given counts."""
    mock_query = MagicMock()
    mock_query.count.return_value = total
    # filter().count() calls in order: hidden, below_threshold, excellent, good, average, poor
    mock_query.filter.return_value.count.side_effect = [
        hidden, below_threshold, excellent, good, average, poor
    ]
    mock_db = MagicMock()
    mock_db.query.return_value = mock_query
    return mock_db


def _make_policy_mock(min_fit=55.0):
    policy = Mock()
    policy.min_fit = min_fit
    policy_service = Mock()
    policy_service.get_current_policy.return_value = policy
    return policy_service


class TestGetStats:
    @pytest.fixture
    def app(self):
        app = FastAPI()
        app.include_router(router)
        return app

    @pytest.fixture
    def client(self, app):
        return TestClient(app, raise_server_exceptions=False)

    def _setup(self, app, mock_db, mock_policy_svc):
        app.dependency_overrides[get_db] = lambda: mock_db
        app.dependency_overrides[get_current_user] = lambda: Mock(id="test-user")
        return patch("web.backend.routers.stats.get_policy_service", return_value=mock_policy_svc)

    def test_returns_200(self, client, app):
        mock_db = _make_query_mock()
        with self._setup(app, mock_db, _make_policy_mock()):
            response = client.get("/api/stats")
        assert response.status_code == 200

    def test_success_flag(self, client, app):
        mock_db = _make_query_mock()
        with self._setup(app, mock_db, _make_policy_mock()):
            data = client.get("/api/stats").json()
        assert data["success"] is True

    def test_total_matches(self, client, app):
        mock_db = _make_query_mock(total=42, hidden=5, below_threshold=7)
        with self._setup(app, mock_db, _make_policy_mock()):
            data = client.get("/api/stats").json()
        assert data["stats"]["total_matches"] == 42

    def test_active_matches_calculated_correctly(self, client, app):
        # active = total - hidden - below_threshold = 20 - 3 - 5 = 12
        mock_db = _make_query_mock(total=20, hidden=3, below_threshold=5)
        with self._setup(app, mock_db, _make_policy_mock()):
            data = client.get("/api/stats").json()
        assert data["stats"]["active_matches"] == 12

    def test_hidden_count(self, client, app):
        mock_db = _make_query_mock(hidden=7)
        with self._setup(app, mock_db, _make_policy_mock()):
            data = client.get("/api/stats").json()
        assert data["stats"]["hidden_count"] == 7

    def test_below_threshold_count(self, client, app):
        mock_db = _make_query_mock(below_threshold=9)
        with self._setup(app, mock_db, _make_policy_mock()):
            data = client.get("/api/stats").json()
        assert data["stats"]["below_threshold_count"] == 9

    def test_min_fit_threshold_from_policy(self, client, app):
        mock_db = _make_query_mock()
        with self._setup(app, mock_db, _make_policy_mock(min_fit=70.0)):
            data = client.get("/api/stats").json()
        assert data["stats"]["min_fit_threshold"] == 70.0

    def test_score_distribution_buckets(self, client, app):
        mock_db = _make_query_mock(excellent=5, good=3, average=2, poor=1)
        with self._setup(app, mock_db, _make_policy_mock()):
            data = client.get("/api/stats").json()
        dist = data["stats"]["score_distribution"]
        assert dist["excellent"] == 5
        assert dist["good"] == 3
        assert dist["average"] == 2
        assert dist["poor"] == 1

    def test_empty_database(self, client, app):
        mock_db = _make_query_mock(total=0, hidden=0, below_threshold=0, excellent=0, good=0, average=0, poor=0)
        with self._setup(app, mock_db, _make_policy_mock()):
            data = client.get("/api/stats").json()
        assert data["stats"]["total_matches"] == 0
        assert data["stats"]["active_matches"] == 0

    def test_stats_key_present_in_response(self, client, app):
        mock_db = _make_query_mock()
        with self._setup(app, mock_db, _make_policy_mock()):
            data = client.get("/api/stats").json()
        expected_keys = {
            "total_matches", "active_matches", "hidden_count",
            "below_threshold_count", "min_fit_threshold", "score_distribution",
        }
        assert expected_keys.issubset(data["stats"].keys())
