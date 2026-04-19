"""
Tests for web/backend/routers/stats.py

Covers the /api/stats GET endpoint: total counts, hidden, below threshold,
active matches, score distribution buckets.
"""

from contextlib import contextmanager
from types import SimpleNamespace

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
    mock_query.join.return_value = mock_query
    mock_query.filter.return_value = mock_query
    # filter().count() calls in order: hidden, below_threshold, excellent, good, average, poor
    mock_query.count.side_effect = [
        total,
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

    def test_canonical_selection_run_populates_tier_counts(self, client, app):
        """With a canonical run, tier counts and excluded_by_reason come from
        match_selection repo, not from the DB-wide legacy query."""
        mock_db = _make_query_mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        match_selection_repo = Mock()
        match_selection_repo.count_items_for_run_by_tier.return_value = {
            "primary": 5, "excluded": 7,
        }
        match_selection_repo.count_excluded_items_by_reason.return_value = {
            "below_min_fit": 4, "beyond_top_k": 3,
        }
        # Provide one item with a ranking_snapshot carrying preference_status dict.
        item = SimpleNamespace(job_match=SimpleNamespace(
            ranking_snapshot={"preference_status": {"applied": True, "reason": "ok"}}
        ))
        match_selection_repo.get_items_for_run.return_value = [item]
        repo = SimpleNamespace(match_selection=match_selection_repo)

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock(min_fit=40.0)), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch(
                 "web.backend.routers.stats.resolve_canonical_resume_selection",
                 return_value=canonical,
             ):
            data = client.get("/api/stats").json()
        stats = data["stats"]
        assert stats["primary_count"] == 5
        assert stats["excluded_count"] == 7
        assert stats["total_scored"] == 12
        assert stats["excluded_by_reason"] == {"below_min_fit": 4, "beyond_top_k": 3}
        assert stats["preference_status"] == {"applied": True, "reason": "ok"}

    def test_canonical_selection_failure_falls_back_to_zero_tier_counts(self, client, app):
        """If the UoW fails, stats must still return — canonical counts drop to 0."""
        mock_db = _make_query_mock()

        @contextmanager
        def exploding_uow():
            raise RuntimeError("db down")
            yield  # pragma: no cover

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", exploding_uow):
            data = client.get("/api/stats").json()
        stats = data["stats"]
        assert stats["total_scored"] == 0
        assert stats["primary_count"] == 0
        assert stats["excluded_count"] == 0
        assert stats["excluded_by_reason"] == {}
        assert stats["preference_status"] is None

    def test_no_canonical_run_leaves_tier_counts_zero(self, client, app):
        mock_db = _make_query_mock()

        @contextmanager
        def fake_uow():
            yield SimpleNamespace()

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch(
                 "web.backend.routers.stats.resolve_canonical_resume_selection",
                 return_value=None,
            ):
            data = client.get("/api/stats").json()
        assert data["stats"]["primary_count"] == 0
        assert data["stats"]["excluded_count"] == 0

    def test_owner_scoped_legacy_counts_join_structured_resume(self, client, app):
        mock_db = _make_query_mock()
        with self._setup(app, mock_db, _make_policy_mock()):
            response = client.get("/api/stats")

        assert response.status_code == 200
        mock_db.query.return_value.join.assert_called_once()
