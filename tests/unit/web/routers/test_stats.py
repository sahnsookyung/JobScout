"""
Tests for web/backend/routers/stats.py

Covers the /api/stats GET endpoint: total counts, hidden, below threshold,
active matches, score distribution buckets.
"""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from unittest.mock import Mock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.backend.routers.stats import router
from web.backend.dependencies import get_current_user, get_db


def _make_item(
    *,
    fit_score: float | None,
    tier: str = "primary",
    hidden: bool = False,
    preference_status: dict | None = None,
):
    ranking_snapshot = (
        {"preference_status": preference_status}
        if preference_status is not None
        else None
    )
    return SimpleNamespace(
        selection_tier=tier,
        fit_score_at_selection=fit_score,
        job_match=SimpleNamespace(
            is_hidden=hidden,
            ranking_snapshot=ranking_snapshot,
        ),
    )


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

    @staticmethod
    def _fake_repo(*, primary_count=0, excluded_count=0, excluded_by_reason=None, items=None):
        match_selection_repo = Mock()
        match_selection_repo.count_items_for_run_by_tier.return_value = {
            "primary": primary_count,
            "excluded": excluded_count,
        }
        match_selection_repo.count_excluded_items_by_reason.return_value = excluded_by_reason or {}
        match_selection_repo.get_items_for_run.return_value = items or []
        return SimpleNamespace(match_selection=match_selection_repo)

    def test_returns_200(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(items=[])

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            response = client.get("/api/stats")
        assert response.status_code == 200

    def test_success_flag(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(items=[])

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        assert data["success"] is True

    def test_total_matches(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(
            primary_count=5,
            excluded_count=7,
            items=[_make_item(fit_score=75.0) for _ in range(12)],
        )

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        assert data["stats"]["total_matches"] == 12

    def test_active_matches_calculated_correctly(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(
            primary_count=5,
            excluded_count=2,
            excluded_by_reason={"below_min_fit": 2},
            items=[
                _make_item(fit_score=85.0, hidden=True),
                _make_item(fit_score=82.0),
                _make_item(fit_score=78.0),
            ],
        )

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        assert data["stats"]["active_matches"] == 4

    def test_hidden_count(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(
            primary_count=4,
            items=[
                _make_item(fit_score=90.0, hidden=True),
                _make_item(fit_score=80.0, hidden=True),
                _make_item(fit_score=70.0),
            ],
        )

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        assert data["stats"]["hidden_count"] == 2

    def test_below_threshold_count(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(
            excluded_by_reason={"below_min_fit": 9},
            items=[],
        )

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        assert data["stats"]["below_threshold_count"] == 9

    def test_min_fit_threshold_from_policy(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(items=[])

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock(min_fit=70.0)), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        assert data["stats"]["min_fit_threshold"] == 70.0

    def test_score_distribution_buckets(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(
            primary_count=11,
            items=(
                [_make_item(fit_score=85.0) for _ in range(5)]
                + [_make_item(fit_score=65.0) for _ in range(3)]
                + [_make_item(fit_score=45.0) for _ in range(2)]
                + [_make_item(fit_score=20.0)]
            ),
        )

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        dist = data["stats"]["score_distribution"]
        assert dist["excellent"] == 5
        assert dist["good"] == 3
        assert dist["average"] == 2
        assert dist["poor"] == 1

    def test_empty_database(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(items=[])

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        assert data["stats"]["total_matches"] == 0
        assert data["stats"]["active_matches"] == 0

    def test_stats_key_present_in_response(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(items=[])

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        expected_keys = {
            "total_matches", "active_matches", "hidden_count",
            "below_threshold_count", "min_fit_threshold", "score_distribution",
        }
        assert expected_keys.issubset(data["stats"].keys())

    def test_canonical_selection_run_populates_tier_counts(self, client, app):
        """With a canonical run, tier counts and excluded_by_reason come from
        match_selection repo, not from the DB-wide legacy query."""
        canonical = SimpleNamespace(selection_run_id="run-1")
        mock_db = Mock()
        repo = self._fake_repo(
            primary_count=5,
            excluded_count=7,
            excluded_by_reason={"below_min_fit": 4, "beyond_top_k": 3},
            items=[
                _make_item(fit_score=85.0, hidden=True),
                _make_item(fit_score=72.0, preference_status={"applied": True, "reason": "ok"}),
                _make_item(fit_score=30.0, tier="excluded"),
            ],
        )

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
        assert stats["total_matches"] == 12
        assert stats["hidden_count"] == 1
        assert stats["active_matches"] == 4
        assert stats["excluded_by_reason"] == {"below_min_fit": 4, "beyond_top_k": 3}
        assert stats["preference_status"] == {"applied": True, "reason": "ok"}

    def test_canonical_selection_failure_falls_back_to_zero_tier_counts(self, client, app):
        """If the UoW fails, stats must still return — canonical counts drop to 0."""
        mock_db = Mock()

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
        assert stats["total_matches"] == 0

    def test_no_canonical_run_leaves_tier_counts_zero(self, client, app):
        mock_db = Mock()

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
        assert data["stats"]["total_matches"] == 0
