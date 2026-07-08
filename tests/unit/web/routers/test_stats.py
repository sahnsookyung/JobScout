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
from web.backend.services.stats_service import (
    _canonical_stats_payload,
    _count_excluded_items_by_reason,
    _count_items_for_run_by_tier,
    _job_processing_stats,
    _preference_status_for_selection_run,
    _selection_run_item_stats,
)
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


def _make_policy_mock(min_fit=55.0, top_k=50):
    policy = Mock()
    policy.min_fit = min_fit
    policy.top_k = top_k
    policy_service = Mock()
    policy_service.get_current_policy.return_value = policy
    return policy_service

class _FakeJobStatsQuery:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row

class _FakeJobStatsDb:
    def __init__(self, row):
        self._row = row

    def query(self, *args):
        return _FakeJobStatsQuery(self._row)


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
                _make_item(fit_score=68.0),
                _make_item(fit_score=58.0),
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
            items=[
                _make_item(fit_score=54.0),
                _make_item(fit_score=20.0, tier="excluded"),
                _make_item(fit_score=None, tier="excluded"),
            ],
        )

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats").json()
        assert data["stats"]["below_threshold_count"] == 3

    def test_policy_query_params_drive_live_bucket_counts(self, client, app):
        mock_db = Mock()
        canonical = SimpleNamespace(selection_run_id="run-1")
        repo = self._fake_repo(
            primary_count=4,
            items=[
                _make_item(fit_score=92.0),
                _make_item(fit_score=80.0),
                _make_item(fit_score=75.0, hidden=True),
                _make_item(fit_score=45.0),
            ],
        )

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock(min_fit=55.0, top_k=50)), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch("web.backend.routers.stats.resolve_canonical_resume_selection", return_value=canonical):
            data = client.get("/api/stats", params={"min_fit": 70, "top_k": 1}).json()
        stats = data["stats"]
        assert stats["min_fit_threshold"] == 70.0
        assert stats["policy_top_k"] == 1
        assert stats["active_matches"] == 1
        assert stats["beyond_top_k_count"] == 1
        assert stats["hidden_count"] == 1
        assert stats["below_threshold_count"] == 1
        assert stats["qualifying_count"] == 3

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
            "beyond_top_k_count", "qualifying_count", "policy_top_k",
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
        assert stats["active_matches"] == 1
        assert stats["below_threshold_count"] == 1
        assert stats["beyond_top_k_count"] == 0
        assert stats["qualifying_count"] == 2
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

    def test_tenant_header_scopes_canonical_selection(self, client, app):
        canonical = SimpleNamespace(selection_run_id="run-tenant")
        mock_db = Mock()
        repo = self._fake_repo(primary_count=1, items=[_make_item(fit_score=90.0)])
        tenant_id = "00000000-0000-4000-8000-000000000201"

        @contextmanager
        def fake_uow():
            yield repo

        with self._setup(app, mock_db, _make_policy_mock()), \
             patch("web.backend.routers.stats.job_uow", fake_uow), \
             patch(
                 "web.backend.routers.stats.resolve_canonical_resume_selection",
                 return_value=canonical,
             ) as resolve_selection:
            response = client.get("/api/stats", headers={"X-Tenant-Id": tenant_id})

        assert response.status_code == 200
        assert str(resolve_selection.call_args.kwargs["tenant_id"]) == tenant_id

    def test_invalid_tenant_header_returns_400(self, client, app):
        mock_db = Mock()

        with self._setup(app, mock_db, _make_policy_mock()):
            response = client.get("/api/stats", headers={"X-Tenant-Id": "not-a-uuid"})

        assert response.status_code == 400
        assert response.json()["detail"] == "X-Tenant-Id must be a UUID."

    def test_job_processing_stats_are_serialized(self):
        row = SimpleNamespace(
            job_post_total=1459,
            active_job_posts=1050,
            inactive_job_posts=409,
            expired_job_posts=5,
            extracted_job_posts=352,
            embedded_job_posts=1129,
            ready_to_score_job_posts=352,
            active_extracted_job_posts=300,
            active_embedded_job_posts=940,
            active_ready_to_score_job_posts=300,
            pending_extraction_job_posts=1103,
            processing_extraction_job_posts=2,
            retryable_extraction_job_posts=4,
            failed_extraction_job_posts=3,
            active_pending_extraction_job_posts=22,
            active_retryable_extraction_job_posts=3,
            inactive_pending_extraction_job_posts=1082,
            ready_for_extraction_job_posts=1103,
            active_ready_for_extraction_job_posts=22,
            pending_embedding_job_posts=330,
            processing_embedding_job_posts=5,
            retryable_embedding_job_posts=7,
            failed_embedding_job_posts=11,
            active_pending_embedding_job_posts=9,
            active_retryable_embedding_job_posts=1,
            inactive_pending_embedding_job_posts=327,
            missing_description_job_posts=304,
            active_missing_description_job_posts=12,
            inactive_missing_description_job_posts=292,
            description_recovery_queued_job_posts=8,
            description_recovery_retryable_job_posts=3,
            description_recovery_unavailable_job_posts=4,
            oldest_missing_description_first_seen_at=None,
        )
        repo = SimpleNamespace(db=_FakeJobStatsDb(row))

        stats = _job_processing_stats(repo)

        assert stats["job_post_total"] == 1459
        assert stats["expired_job_posts"] == 5
        assert stats["embedded_job_posts"] == 1129
        assert stats["active_ready_to_score_job_posts"] == 300
        assert stats["pending_extraction_job_posts"] == 1103
        assert stats["active_pending_extraction_job_posts"] == 22
        assert stats["inactive_pending_extraction_job_posts"] == 1082
        assert stats["ready_for_extraction_job_posts"] == 1103
        assert stats["active_ready_for_extraction_job_posts"] == 22
        assert stats["missing_description_job_posts"] == 304
        assert stats["inactive_missing_description_job_posts"] == 292
        assert stats["description_recovery_queued_job_posts"] == 8
        assert stats["description_recovery_retryable_job_posts"] == 3
        assert stats["description_recovery_unavailable_job_posts"] == 4
        assert stats["processing_extraction_job_posts"] == 2
        assert stats["retryable_extraction_job_posts"] == 4
        assert stats["failed_extraction_job_posts"] == 3
        assert stats["processing_embedding_job_posts"] == 5
        assert stats["retryable_embedding_job_posts"] == 7
        assert stats["failed_embedding_job_posts"] == 11


class _QueryChain:
    def __init__(self, *, rows=None, row=None):
        self.rows = rows or []
        self.row = row
        self.join_calls = 0
        self.filter_calls = 0

    def join(self, *args, **kwargs):
        self.join_calls += 1
        return self

    def filter(self, *args, **kwargs):
        self.filter_calls += 1
        return self

    def group_by(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def select_from(self, *args, **kwargs):
        return self

    def all(self):
        return self.rows

    def one(self):
        return self.row


def test_canonical_stats_payload_records_job_and_canonical_degradation():
    reasons = []
    repo = SimpleNamespace()

    with patch(
        "web.backend.services.stats_service._job_processing_stats",
        side_effect=RuntimeError("job stats down"),
    ), patch("web.backend.services.stats_service.set_job_inventory_metrics"):
        stats = _canonical_stats_payload(
            repo,
            "owner-1",
            min_fit=55.0,
            top_k=10,
            tenant_id=None,
            degraded_reasons=reasons,
            canonical_resolver=Mock(side_effect=RuntimeError("canonical down")),
        )

    assert stats["total_matches"] == 0
    assert [reason["code"] for reason in reasons] == [
        "job_processing_stats_unavailable",
        "canonical_selection_unavailable",
    ]


def test_canonical_stats_payload_uses_fallback_counts_and_item_stats():
    canonical = SimpleNamespace(selection_run_id="run-1")
    repo = SimpleNamespace(match_selection=Mock())
    repo.match_selection.count_items_for_run_by_tier.side_effect = RuntimeError("tier fallback down")
    repo.match_selection.count_excluded_items_by_reason.side_effect = RuntimeError("reason fallback down")
    repo.match_selection.get_items_for_run.return_value = [
        _make_item(fit_score=82.0, preference_status={"applied": True}),
        _make_item(fit_score=20.0, tier="excluded"),
    ]
    reasons = []

    with patch(
        "web.backend.services.stats_service._job_processing_stats",
        return_value={"job_post_total": 1},
    ), patch("web.backend.services.stats_service.set_job_inventory_metrics"), patch(
        "web.backend.services.stats_service._count_items_for_run_by_tier",
        side_effect=RuntimeError("tenant tier down"),
    ), patch(
        "web.backend.services.stats_service._count_excluded_items_by_reason",
        side_effect=RuntimeError("tenant reason down"),
    ), patch(
        "web.backend.services.stats_service._selection_run_item_stats",
        side_effect=RuntimeError("tenant stats down"),
    ):
        stats = _canonical_stats_payload(
            repo,
            "owner-1",
            min_fit=55.0,
            top_k=10,
            tenant_id="tenant-1",
            degraded_reasons=reasons,
            canonical_resolver=Mock(return_value=canonical),
        )

    assert stats["job_post_total"] == 1
    assert stats["active_matches"] == 1
    assert stats["below_threshold_count"] == 1
    assert stats["preference_status"] == {"applied": True}
    assert [reason["code"] for reason in reasons] == [
        "tenant_scoped_tier_counts_unavailable",
        "tier_counts_unavailable",
        "tenant_scoped_excluded_reason_counts_unavailable",
        "excluded_reason_counts_unavailable",
        "tenant_scoped_selection_item_stats_unavailable",
    ]


def test_canonical_stats_payload_handles_item_stats_fallback_failure():
    canonical = SimpleNamespace(selection_run_id="run-1")
    repo = SimpleNamespace(match_selection=Mock())
    repo.match_selection.count_items_for_run_by_tier.return_value = {"primary": 2}
    repo.match_selection.count_excluded_items_by_reason.return_value = {}
    repo.match_selection.get_items_for_run.side_effect = RuntimeError("items down")
    reasons = []

    with patch(
        "web.backend.services.stats_service._job_processing_stats",
        return_value={},
    ), patch("web.backend.services.stats_service.set_job_inventory_metrics"), patch(
        "web.backend.services.stats_service._selection_run_item_stats",
        side_effect=RuntimeError("tenant stats down"),
    ):
        stats = _canonical_stats_payload(
            repo,
            "owner-1",
            min_fit=55.0,
            top_k=10,
            tenant_id=None,
            degraded_reasons=reasons,
            canonical_resolver=Mock(return_value=canonical),
        )

    assert stats["active_matches"] == 0
    assert reasons[-1]["code"] == "selection_item_stats_unavailable"


def test_tenant_scoped_count_helpers_apply_tenant_joins():
    tier_query = _QueryChain(rows=[(None, 2), ("excluded", 3)])
    reason_query = _QueryChain(rows=[(None, 1), ("below_min_fit", 4)])
    db = Mock()
    db.query.side_effect = [tier_query, reason_query]
    repo = SimpleNamespace(db=db)

    tier_counts = _count_items_for_run_by_tier(repo, "run-1", tenant_id="tenant-1")
    reason_counts = _count_excluded_items_by_reason(repo, "run-1", tenant_id="tenant-1")

    assert tier_counts == {"primary": 2, "excluded": 3}
    assert reason_counts == {"unknown": 1, "below_min_fit": 4}
    assert tier_query.join_calls >= 2
    assert reason_query.join_calls >= 2


def test_preference_status_and_selection_item_stats_query_helpers():
    preference_query = _QueryChain(rows=[("not-json",), ({"preference_status": {"applied": True}},)])
    stats_row = SimpleNamespace(
        below_threshold_count=1,
        hidden_count=2,
        visible_qualifying_count=5,
        excellent_count=3,
        good_count=2,
        average_count=1,
        poor_count=0,
    )
    stats_query = _QueryChain(row=stats_row)
    db = Mock()
    db.query.side_effect = [preference_query, stats_query, preference_query]
    repo = SimpleNamespace(db=db)

    assert _preference_status_for_selection_run(repo, "run-1", tenant_id="tenant-1") == {"applied": True}
    stats = _selection_run_item_stats(
        repo,
        "run-1",
        min_fit=55.0,
        top_k=3,
        tenant_id="tenant-1",
    )

    assert stats["active_matches"] == 3
    assert stats["beyond_top_k_count"] == 2
    assert stats["qualifying_count"] == 7
    assert stats["score_distribution"]["excellent"] == 3
