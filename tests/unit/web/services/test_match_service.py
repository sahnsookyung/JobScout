#!/usr/bin/env python3
"""
Tests for Match Service
Covers: web/backend/services/match_service.py

Key invariants verified:
  - Stage 1: DB query is scoped to the canonical resume, not a fit-biased SQL shortlist
  - Stage 2: rank_matches() is called on the full retrieved pool
  - Stage 3: top_k truncation happens AFTER ranking (pool[:effective_k])
  - ranking_mode parameter is forwarded to the ranking engine
  - preference_score=None is preserved (distinct from 0.0)
"""

import pytest
import uuid
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    return Mock(spec=Session)


@pytest.fixture
def service(mock_db):
    from web.backend.services.match_service import MatchService
    instance = MatchService(mock_db)
    instance._resolve_canonical_selection = Mock(
        return_value=SimpleNamespace(
            resume_fingerprint="fp-123",
            selection_run_id="run-1",
        )
    )
    instance._load_rankable_pool = Mock(return_value=[])
    return instance


@pytest.fixture
def real_service(mock_db):
    from web.backend.services.match_service import MatchService
    return MatchService(mock_db)


def _make_match(
    match_id="match-1",
    fit_score=0.85,
    preference_score=None,
    job_id="job-1",
    title="Developer",
    company="TechCorp",
    location_text="Remote",
    is_remote=True,
):
    """Return a minimal mock JobMatch with all fields _to_match_summary touches."""
    m = Mock()
    m.id = match_id
    m.fit_score = fit_score
    m.preference_score = preference_score
    m.job_similarity = 0.7
    m.penalties = 0.05
    m.required_coverage = 0.90
    m.preferred_requirement_coverage = 0.60
    m.match_type = "requirements_only"
    m.is_hidden = False
    m.created_at = datetime.now(timezone.utc)
    m.calculated_at = datetime.now(timezone.utc)
    m.selection_tier = "primary"
    m.excluded_reason = None
    job = Mock()
    job.id = job_id
    job.title = title
    job.company = company
    job.location_text = location_text
    job.is_remote = is_remote
    m.job_post = job
    # ranking_explanation is attached by rank_matches(); not present by default
    del m.ranking_explanation  # ensure getattr returns AttributeError → None fallback
    return m


def _wire_query(mock_db, matches):
    """Wire mock_db.query() chain to return *matches* from .all()."""
    q = MagicMock()
    mock_db.query.return_value = q
    q.filter.return_value = q
    q.options.return_value = q
    q.order_by.return_value = q
    q.join.return_value = q
    q.limit.return_value = q
    q.all.return_value = list(matches)
    return q

def _wire_rankable_pool(service, matches):
    service._load_rankable_pool = Mock(return_value=list(matches))


# ---------------------------------------------------------------------------
# Three-stage pipeline correctness
# ---------------------------------------------------------------------------

class TestThreeStagePipeline:
    """
    The canonical correctness contract:
      1. DB query scopes to the canonical resume and does not SQL-limit by fit.
      2. rank_matches() receives the FULL retrieved pool.
      3. Truncation to effective_top_k happens AFTER ranking.
    """

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_loads_selection_run_pool_before_reranking(
        self, mock_store, mock_rank, mock_db, service
    ):
        """The web read path should use the selection-run pool, not a SQL shortlist."""
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(balanced_w_pref=0.6, balanced_w_fit=0.4, max_ranking_candidates=500)
        mock_store.return_value.get_current_config.return_value = cfg

        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda pool, ctx: pool  # passthrough

        service.get_matches(top_k=5)

        service._resolve_canonical_selection.assert_called_once_with(
            owner_id=None,
            tenant_id=None,
        )
        service._load_rankable_pool.assert_called_once()
        mock_db.query.assert_not_called()

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    @patch("web.backend.services.match_service.get_result_policy_store")
    @patch("web.backend.services.match_service.MatchLlmEvaluationService")
    def test_llm_rerank_applies_before_top_k_truncation(
        self,
        mock_judge_service_cls,
        mock_llm_policy_store,
        mock_store,
        mock_rank,
        mock_db,
        service,
    ):
        from core.ranking.policy import RankingConfig

        cfg = RankingConfig(
            balanced_w_pref=0.6,
            balanced_w_fit=0.4,
            max_ranking_candidates=500,
            default_top_k=1,
            max_top_k=100,
        )
        mock_store.return_value.get_current_config.return_value = cfg
        mock_llm_policy_store.return_value.get_llm_judge_policy.return_value = SimpleNamespace(
            enabled=True,
            available=True,
            top_n=2,
            unavailable_reason="available",
        )
        mock_judge_service = mock_judge_service_cls.return_value
        mock_judge_service.evaluation_effectiveness.return_value = {
            "effective_for_rerank": True,
            "ignored_for_rerank_reason": None,
            "stale_status": "current",
            "input_truncation": {},
        }

        low_llm = _make_match("m1", fit_score=95)
        high_llm = _make_match("m2", fit_score=90)
        now = datetime.now(timezone.utc)
        low_llm.llm_evaluation = SimpleNamespace(
            id="eval-1",
            status="succeeded",
            llm_score=55,
            confidence=0.9,
            completed_at=now,
        )
        high_llm.llm_evaluation = SimpleNamespace(
            id="eval-2",
            status="succeeded",
            llm_score=97,
            confidence=0.8,
            completed_at=now,
        )
        _wire_rankable_pool(service, [low_llm, high_llm])
        mock_rank.side_effect = lambda p, ctx: p

        results = service.get_matches(owner_id="owner-1", top_k=1)

        assert [item.match_id for item in results] == ["m2"]
        assert service.last_llm_rerank_metadata["applied"] is True
        assert service.last_llm_rerank_metadata["eligible_count"] == 2

    @patch("web.backend.services.match_service.record_match_query_degraded")
    @patch("web.backend.services.match_service.job_uow")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_canonical_selection_failure_returns_degraded_metadata(
        self,
        mock_store,
        mock_uow,
        mock_record_degraded,
        mock_db,
        real_service,
    ):
        from core.ranking.policy import RankingConfig

        cfg = RankingConfig(
            balanced_w_pref=0.6,
            balanced_w_fit=0.4,
            max_ranking_candidates=500,
        )
        mock_store.return_value.get_current_config.return_value = cfg
        mock_uow.side_effect = RuntimeError("database unavailable")

        results = real_service.get_matches(owner_id="owner-1")

        assert results == []
        assert real_service.last_degraded_reasons == [
            {"code": "canonical_selection_unavailable", "detail": "RuntimeError"}
        ]
        mock_record_degraded.assert_called_once_with("canonical_selection_unavailable")

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_rank_matches_receives_full_pool_before_truncation(
        self, mock_store, mock_rank, mock_db, service
    ):
        """rank_matches is called with all pool items; truncation happens after."""
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4,
            max_ranking_candidates=500, default_top_k=2, max_top_k=100,
        )
        mock_store.return_value.get_current_config.return_value = cfg

        pool = [_make_match(f"m{i}", fit_score=float(i)) for i in range(5)]
        _wire_rankable_pool(service, pool)

        captured_pool = []

        def capture(p, ctx):
            captured_pool.extend(p)
            return p

        mock_rank.side_effect = capture

        results = service.get_matches(top_k=2)

        # rank_matches received all 5 items (full pool)
        assert len(captured_pool) == 5
        # result is truncated to 2
        assert len(results) == 2

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_truncation_respects_max_top_k(
        self, mock_store, mock_rank, mock_db, service
    ):
        """top_k > max_top_k is silently capped to max_top_k."""
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4,
            max_ranking_candidates=500, default_top_k=25, max_top_k=3,
        )
        mock_store.return_value.get_current_config.return_value = cfg

        pool = [_make_match(f"m{i}") for i in range(10)]
        _wire_rankable_pool(service, pool)
        mock_rank.side_effect = lambda p, ctx: p

        results = service.get_matches(top_k=100)

        assert len(results) == 3

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_default_top_k_used_when_none_requested(
        self, mock_store, mock_rank, mock_db, service
    ):
        """When top_k is not specified, config.default_top_k applies."""
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4,
            max_ranking_candidates=500, default_top_k=3, max_top_k=100,
        )
        mock_store.return_value.get_current_config.return_value = cfg

        pool = [_make_match(f"m{i}") for i in range(8)]
        _wire_rankable_pool(service, pool)
        mock_rank.side_effect = lambda p, ctx: p

        results = service.get_matches()  # top_k=None

        assert len(results) == 3

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_tier_all_without_explicit_top_k_returns_primary_and_excluded(
        self, mock_store, mock_rank, mock_db, service
    ):
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4,
            max_ranking_candidates=500, default_top_k=2, max_top_k=100,
        )
        mock_store.return_value.get_current_config.return_value = cfg

        primary = [_make_match(f"p{i}") for i in range(2)]
        excluded = [_make_match(f"e{i}") for i in range(3)]
        for match in excluded:
            match.selection_tier = "excluded"
        _wire_rankable_pool(service, primary + excluded)
        mock_rank.side_effect = lambda p, ctx: p

        results = service.get_matches(tier="all", top_k=None)

        assert len(results) == 5
        assert service.last_matches_limit == 100

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_tier_all_without_limit_is_server_paged(
        self, mock_store, mock_rank, mock_db, service
    ):
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4,
            max_ranking_candidates=500, default_top_k=50, max_top_k=500,
        )
        mock_store.return_value.get_current_config.return_value = cfg

        primary = [_make_match(f"p{i}") for i in range(120)]
        _wire_rankable_pool(service, primary)
        mock_rank.side_effect = lambda p, ctx: p

        results = service.get_matches(tier="all", top_k=None, limit=None)

        assert len(results) == 100
        assert service.last_matches_total == 120
        assert service.last_matches_limit == 100

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_tier_all_with_explicit_top_k_caps_final_combined_results(
        self, mock_store, mock_rank, mock_db, service
    ):
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4,
            max_ranking_candidates=500, default_top_k=2, max_top_k=4,
        )
        mock_store.return_value.get_current_config.return_value = cfg

        primary = [_make_match(f"p{i}") for i in range(2)]
        excluded = [_make_match(f"e{i}") for i in range(3)]
        for match in excluded:
            match.selection_tier = "excluded"
        _wire_rankable_pool(service, primary + excluded)
        mock_rank.side_effect = lambda p, ctx: p

        results = service.get_matches(tier="all", top_k=4)

        assert len(results) == 4

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_limit_and_offset_page_after_ranking(
        self, mock_store, mock_rank, mock_db, service
    ):
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4,
            max_ranking_candidates=500, default_top_k=2, max_top_k=100,
        )
        mock_store.return_value.get_current_config.return_value = cfg

        primary = [_make_match(f"p{i}") for i in range(4)]
        _wire_rankable_pool(service, primary)
        mock_rank.side_effect = lambda p, ctx: p

        results = service.get_matches(tier="all", top_k=None, limit=2, offset=1)

        assert [match.match_id for match in results] == ["p1", "p2"]
        assert service.last_matches_total == 4
        assert service.last_matches_limit == 2
        assert service.last_matches_offset == 1

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_missing_canonical_resume_returns_empty_list(
        self, mock_store, mock_rank, mock_db, service
    ):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        service._resolve_canonical_selection.return_value = None

        assert service.get_matches() == []
        mock_rank.assert_not_called()

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_ranking_mode_forwarded_to_engine(
        self, mock_store, mock_rank, mock_db, service
    ):
        """ranking_mode string is resolved and passed as RankingContext.mode."""
        from core.ranking.policy import RankingConfig
        from core.ranking.engine import RankingMode
        cfg = RankingConfig(balanced_w_pref=0.6, balanced_w_fit=0.4)
        mock_store.return_value.get_current_config.return_value = cfg

        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(ranking_mode="preference_first")

        assert mock_rank.called
        ctx = mock_rank.call_args[0][1]
        assert ctx.mode == RankingMode.PREFERENCE_FIRST

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_invalid_ranking_mode_falls_back_to_default(
        self, mock_store, mock_rank, mock_db, service
    ):
        """An unrecognised ranking_mode falls back to config.active_default_mode."""
        from core.ranking.policy import RankingConfig
        from core.ranking.engine import RankingMode
        cfg = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4, active_default_mode="fit_first"
        )
        mock_store.return_value.get_current_config.return_value = cfg

        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(ranking_mode="not_a_valid_mode")

        ctx = mock_rank.call_args[0][1]
        assert ctx.mode == RankingMode.FIT_FIRST

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_min_fit_filter_applied_to_query(
        self, mock_store, mock_rank, mock_db, service
    ):
        """When min_fit is set the query includes a fit_score filter."""
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(min_fit=50.0)

        assert service._load_rankable_pool.call_args.kwargs["min_fit"] == 50.0

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_empty_pool_returns_empty_list(
        self, mock_store, mock_rank, mock_db, service
    ):
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(balanced_w_pref=0.6, balanced_w_fit=0.4)
        mock_store.return_value.get_current_config.return_value = cfg

        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        assert service.get_matches() == []

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    @patch("web.backend.services.match_service.job_uow")
    def test_selection_run_pool_uses_snapshot_scores_not_sql_query(
        self,
        mock_uow,
        mock_store,
        mock_rank,
        mock_db,
        real_service,
    ):
        from core.ranking.policy import RankingConfig

        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        real_service._resolve_canonical_selection = Mock(
            return_value=SimpleNamespace(
                resume_fingerprint="fp-123",
                selection_run_id="run-1",
            )
        )
        repo = MagicMock()
        repo.match_selection.get_items_for_run.return_value = [
            SimpleNamespace(
                fit_score_at_selection=82.0,
                preference_score_at_selection=0.7,
                job_similarity_at_selection=0.8,
                required_coverage_at_selection=0.9,
                job_match=SimpleNamespace(
                    id="match-1",
                    job_post_id="job-1",
                    penalties=0.0,
                    preferred_requirement_coverage=0.5,
                    match_type="requirements_only",
                    is_hidden=False,
                    created_at=datetime.now(timezone.utc),
                    calculated_at=datetime.now(timezone.utc),
                    status="active",
                    job_post=SimpleNamespace(
                        id="job-1",
                        title="Engineer",
                        company="Acme",
                        location_text="Remote",
                        is_remote=True,
                    ),
                ),
            ),
        ]
        mock_uow.return_value = MagicMock(
            __enter__=Mock(return_value=repo),
            __exit__=Mock(return_value=False),
        )
        mock_rank.side_effect = lambda p, ctx: p

        results = real_service.get_matches(owner_id="user-1")

        assert len(results) == 1
        assert results[0].fit_score == pytest.approx(82.0)
        mock_db.query.assert_not_called()

    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_cursor_mode_uses_bounded_projection_instead_of_full_pool(
        self,
        mock_store,
        mock_db,
        service,
    ):
        from core.ranking.policy import RankingConfig

        cfg = RankingConfig(
            balanced_w_pref=0.6,
            balanced_w_fit=0.4,
            max_ranking_candidates=500,
            default_top_k=10,
            max_top_k=100,
        )
        mock_store.return_value.get_current_config.return_value = cfg
        service._resolve_canonical_selection.return_value = SimpleNamespace(
            resume_fingerprint="fp-123",
            selection_run_id=uuid.uuid4(),
            ranking_mode_used="balanced",
        )

        def row(rank: int):
            return SimpleNamespace(
                selection_item_id=uuid.uuid4(),
                rank_position=rank,
                fit_score_at_selection=90 - rank,
                preference_score_at_selection=0.7,
                job_similarity_at_selection=0.8,
                required_coverage_at_selection=0.9,
                selection_tier="primary",
                excluded_reason=None,
                match_id=uuid.uuid4(),
                job_post_id=uuid.uuid4(),
                penalties=0.0,
                preferred_requirement_coverage=0.5,
                match_type="requirements_only",
                is_hidden=False,
                created_at=datetime.now(timezone.utc),
                calculated_at=datetime.now(timezone.utc),
                resume_fingerprint="fp-123",
                job_content_hash="hash",
                title=f"Engineer {rank}",
                company="Acme",
                location_text="Remote",
                is_remote=True,
            )

        count_result = Mock()
        count_result.scalar_one.return_value = 3
        rows_result = Mock()
        rows_result.all.return_value = [row(1), row(2), row(3)]
        mock_db.execute.side_effect = [count_result, rows_result]

        results = service.get_matches(
            owner_id="owner-1",
            page_mode="cursor",
            view="compact",
            include="",
            limit=2,
            top_k=10,
        )

        assert [item.title for item in results] == ["Engineer 1", "Engineer 2"]
        service._load_rankable_pool.assert_not_called()
        assert service.last_matches_page_mode == "cursor"
        assert service.last_matches_view == "compact"
        assert service.last_matches_limit == 2
        assert service.last_matches_next_cursor is not None
        assert service.last_matches_has_more is True
        assert mock_db.execute.call_count == 2

    @patch("web.backend.services.match_service.get_result_policy_store")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_cursor_mode_expands_sql_window_to_llm_top_n_before_page_slice(
        self,
        mock_ranking_store,
        mock_policy_store,
        mock_db,
        service,
    ):
        from core.ranking.policy import RankingConfig

        cfg = RankingConfig(
            balanced_w_pref=0.6,
            balanced_w_fit=0.4,
            max_ranking_candidates=500,
            default_top_k=2,
            max_top_k=100,
        )
        mock_ranking_store.return_value.get_current_config.return_value = cfg
        mock_policy_store.return_value.get_llm_judge_policy.return_value = SimpleNamespace(
            enabled=True,
            available=True,
            top_n=4,
            revision=9,
            unavailable_reason="available",
        )
        service._resolve_canonical_selection.return_value = SimpleNamespace(
            resume_fingerprint="fp-123",
            selection_run_id=uuid.uuid4(),
            ranking_mode_used="balanced",
        )
        service._attach_latest_evaluations = Mock()
        service._apply_llm_rerank = Mock(
            side_effect=lambda candidates, **_: {
                "enabled": True,
                "available": True,
                "applied": False,
                "top_n": 4,
                "window_size": len(candidates),
                "eligible_count": len(candidates),
                "reranked_count": 0,
                "policy_revision": 9,
                "reason": "no_effective_evaluations",
            }
        )

        def row(rank: int):
            return SimpleNamespace(
                selection_item_id=uuid.uuid4(),
                rank_position=rank,
                fit_score_at_selection=90 - rank,
                preference_score_at_selection=0.7,
                job_similarity_at_selection=0.8,
                required_coverage_at_selection=0.9,
                selection_tier="primary",
                excluded_reason=None,
                match_id=uuid.uuid4(),
                job_post_id=uuid.uuid4(),
                penalties=0.0,
                preferred_requirement_coverage=0.5,
                match_type="requirements_only",
                is_hidden=False,
                created_at=datetime.now(timezone.utc),
                calculated_at=datetime.now(timezone.utc),
                resume_fingerprint="fp-123",
                job_content_hash="hash",
                title=f"Engineer {rank}",
                company="Acme",
                location_text="Remote",
                is_remote=True,
            )

        count_result = Mock()
        count_result.scalar_one.return_value = 4
        rows_result = Mock()
        rows_result.all.return_value = [row(1), row(2), row(3), row(4)]
        mock_db.execute.side_effect = [count_result, rows_result]

        results = service.get_matches(
            owner_id="owner-1",
            page_mode="cursor",
            view="compact",
            limit=2,
            top_k=2,
        )

        assert [item.title for item in results] == [
            "Engineer 1",
            "Engineer 2",
            "Engineer 3",
            "Engineer 4",
        ]
        count_stmt = mock_db.execute.call_args_list[0].args[0]
        assert 4 in count_stmt.compile().params.values()
        assert service.last_matches_limit == 4
        service._apply_llm_rerank.assert_called_once()
        assert len(service._apply_llm_rerank.call_args.args[0]) == 4

    @patch("web.backend.services.match_service.MatchLlmEvaluationService")
    @patch("web.backend.services.match_service.get_result_policy_store")
    def test_llm_rerank_window_uses_policy_top_n_and_revision(
        self,
        mock_llm_policy_store,
        mock_judge_service_cls,
        service,
    ):
        mock_llm_policy_store.return_value.get_llm_judge_policy.return_value = SimpleNamespace(
            enabled=True,
            available=True,
            top_n=3,
            revision=7,
            unavailable_reason="available",
        )
        mock_judge_service = mock_judge_service_cls.return_value
        mock_judge_service.evaluation_effectiveness.return_value = {
            "effective_for_rerank": True,
            "ignored_for_rerank_reason": None,
            "stale_status": "current",
        }
        now = datetime.now(timezone.utc)
        candidates = [_make_match(f"m{i}", fit_score=100 - i) for i in range(5)]
        for index, candidate in enumerate(candidates, start=1):
            candidate.llm_original_rank = index
            if index <= 3:
                candidate.llm_evaluation = SimpleNamespace(
                    id=f"eval-{index}",
                    status="succeeded",
                    llm_score=80 + index,
                    confidence=0.9,
                    completed_at=now,
                )

        metadata = service._apply_llm_rerank(
            candidates,
            owner_id="owner-1",
            tenant_id=None,
        )

        assert metadata["top_n"] == 3
        assert metadata["window_size"] == 3
        assert metadata["policy_revision"] == 7
        assert mock_judge_service.evaluation_effectiveness.call_count == 3
        assert candidates[3].llm_reranked_rank == 4


class TestRankablePoolHelpers:
    @patch("web.backend.services.match_service.job_uow")
    def test_resolve_canonical_selection_returns_none_when_resolution_fails(
        self,
        mock_uow,
        real_service,
    ):
        mock_uow.side_effect = RuntimeError("database unavailable")

        assert real_service._resolve_canonical_selection(owner_id="user-1") is None

    @patch("web.backend.services.match_service.job_uow")
    def test_load_rankable_pool_skips_items_that_do_not_pass_filters(
        self,
        mock_uow,
        real_service,
    ):
        repo = MagicMock()
        hidden_match = SimpleNamespace(
            status="active",
            is_hidden=True,
            job_post=SimpleNamespace(is_remote=True),
        )
        repo.match_selection.get_items_for_run.return_value = [
            SimpleNamespace(
                fit_score_at_selection=90.0,
                job_match=hidden_match,
            ),
        ]
        mock_uow.return_value = MagicMock(
            __enter__=Mock(return_value=repo),
            __exit__=Mock(return_value=False),
        )

        assert real_service._load_rankable_pool(
            SimpleNamespace(selection_run_id="run-1"),
            status="active",
            min_fit=None,
            remote_only=False,
            show_hidden=False,
        ) == []

    def test_selection_item_filter_rejects_status_min_fit_hidden_and_non_remote(self, real_service):
        active_match = SimpleNamespace(status="active", is_hidden=False)
        hidden_match = SimpleNamespace(status="active", is_hidden=True)
        inactive_match = SimpleNamespace(status="stale", is_hidden=False)
        remote_job = SimpleNamespace(is_remote=True)
        onsite_job = SimpleNamespace(is_remote=False)

        assert real_service._selection_item_passes_filters(
            active_match,
            remote_job,
            80.0,
            status="active",
            min_fit=70.0,
            remote_only=True,
            show_hidden=False,
        )
        assert not real_service._selection_item_passes_filters(
            inactive_match,
            remote_job,
            80.0,
            status="active",
            min_fit=None,
            remote_only=False,
            show_hidden=True,
        )
        assert not real_service._selection_item_passes_filters(
            active_match,
            remote_job,
            None,
            status="active",
            min_fit=70.0,
            remote_only=False,
            show_hidden=True,
        )
        assert not real_service._selection_item_passes_filters(
            hidden_match,
            remote_job,
            80.0,
            status="active",
            min_fit=None,
            remote_only=False,
            show_hidden=False,
        )
        assert not real_service._selection_item_passes_filters(
            active_match,
            onsite_job,
            80.0,
            status="all",
            min_fit=None,
            remote_only=True,
            show_hidden=True,
        )

    def test_selection_item_to_summary_candidate_uses_unknown_job_fallbacks(self, real_service):
        item = SimpleNamespace(
            preference_score_at_selection=None,
            job_similarity_at_selection=0.45,
            required_coverage_at_selection=0.5,
        )
        match = SimpleNamespace(
            id="match-1",
            job_post_id="job-1",
            penalties=None,
            preferred_requirement_coverage=None,
            match_type=None,
            is_hidden=False,
            created_at=datetime.now(timezone.utc),
            calculated_at=datetime.now(timezone.utc),
        )

        candidate = real_service._selection_item_to_summary_candidate(
            item,
            match,
            job=None,
            fit_score=None,
        )

        assert candidate.title == "Unknown"
        assert candidate.company == "Unknown"
        assert candidate.fit_score is None
        assert candidate.preference_score is None

    def test_get_match_for_owner_raises_when_missing(self, real_service, mock_db):
        query = _wire_query(mock_db, [])
        query.one_or_none.return_value = None

        from web.backend.services.match_service import MatchNotFoundException

        with pytest.raises(MatchNotFoundException):
            real_service._get_match_for_owner("missing-match", owner_id="user-1")

    def test_fit_components_rejects_non_dict_payload(self, real_service):
        assert real_service._fit_components(["not", "a", "dict"]) is None


# ---------------------------------------------------------------------------
# Filter wiring
# ---------------------------------------------------------------------------

class TestGetMatchesFilters:

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_status_filter_applied(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(status="active")

        assert service._load_rankable_pool.call_args.kwargs["status"] == "active"

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_status_all_skips_status_filter(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(status="all")

        assert service._load_rankable_pool.call_args.kwargs["status"] == "all"

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_remote_only_joins_job_post(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(remote_only=True)

        assert service._load_rankable_pool.call_args.kwargs["remote_only"] is True

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_show_hidden_false_filters_hidden(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(show_hidden=False)

        assert service._load_rankable_pool.call_args.kwargs["show_hidden"] is False

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_show_hidden_true_skips_hidden_filter(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        _wire_rankable_pool(service, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(show_hidden=True)

        assert service._load_rankable_pool.call_args.kwargs["show_hidden"] is True


# ---------------------------------------------------------------------------
# MatchSummary field mapping (preference_score NULL preservation)
# ---------------------------------------------------------------------------

class TestToMatchSummary:

    def test_preference_score_none_preserved(self, service):
        """preference_score=None (evaluator not run) must remain None in output."""
        m = _make_match(preference_score=None)
        result = service._to_match_summary(m)
        assert result.preference_score is None
        assert result.preferred_requirement_coverage == pytest.approx(0.60)

    def test_preference_score_zero_preserved(self, service):
        """preference_score=0.0 (scored poorly) must remain 0.0, not collapsed to None."""
        m = _make_match(preference_score=0.0)
        result = service._to_match_summary(m)
        assert result.preference_score == pytest.approx(0.0)

    def test_ranking_explanation_fields_populated(self, service):
        """When ranking_explanation is present the fields are forwarded."""
        from core.ranking.explainability import RankingExplanation
        m = _make_match(preference_score=0.7)
        m.ranking_explanation = RankingExplanation(
            ranking_mode_used="balanced",
            config_version="1.0.0",
            preference_score=0.7,
            fit_score=0.85,
            similarity_score=0.7,
            balanced_primary_score=0.76,
            dominant_reason_code="balanced_blend",
            explanation_label="Balanced blend of preference and fit",
            missing_scores=[],
        )
        result = service._to_match_summary(m)
        assert result.ranking_mode_used == "balanced"
        assert result.dominant_reason_code == "balanced_blend"
        assert result.balanced_primary_score == pytest.approx(0.76)
        assert result.missing_scores == []

    def test_ranking_explanation_absent_returns_none_fields(self, service):
        """When ranking_explanation is not attached, explanation fields are None/empty."""
        m = _make_match()
        result = service._to_match_summary(m)
        assert result.ranking_mode_used is None
        assert result.dominant_reason_code is None
        assert result.missing_scores == []

    def test_basic_fields_mapped(self, service):
        m = _make_match(
            match_id="m-1", job_id="j-1", title="SWE",
            company="ACME", location_text="Tokyo", is_remote=False,
            fit_score=72.5,
        )
        result = service._to_match_summary(m)
        assert result.match_id == "m-1"
        assert result.job_id == "j-1"
        assert result.title == "SWE"
        assert result.company == "ACME"
        assert result.location == "Tokyo"
        assert result.is_remote is False
        assert result.fit_score == pytest.approx(72.5)

    def test_llm_marker_fields_mapped(self, service):
        completed_at = datetime.now(timezone.utc)
        m = _make_match(match_id="m-llm")
        m.llm_evaluation = SimpleNamespace(
            id="eval-1",
            status="succeeded",
            llm_score=91.5,
            confidence=0.875,
            completed_at=completed_at,
        )

        result = service._to_match_summary(m)

        assert result.llm_evaluation_status == "succeeded"
        assert result.llm_evaluation_id == "eval-1"
        assert result.llm_score == pytest.approx(91.5)
        assert result.llm_confidence == pytest.approx(0.875)
        assert result.llm_judged_at == completed_at.isoformat()

    def test_job_access_error_falls_back_to_unknown(self, service):
        from unittest.mock import PropertyMock
        m = _make_match()
        mock_job = Mock()
        type(mock_job).id = PropertyMock(side_effect=RuntimeError("db gone"))
        m.job_post = mock_job
        result = service._to_match_summary(m)
        assert result.title == "Unknown"
        assert result.company == "Unknown"
        assert result.job_id is None

    def test_null_job_post_falls_back_gracefully(self, service):
        m = _make_match()
        m.job_post = None
        result = service._to_match_summary(m)
        assert result.title == "Unknown"
        assert result.job_id is None


# ---------------------------------------------------------------------------
# get_match_detail
# ---------------------------------------------------------------------------

class TestGetMatchDetail:

    def _make_full_match(self):
        m = Mock()
        m.id = "match-1"
        m.job_post_id = "job-1"
        m.resume_fingerprint = "fp-123"
        m.fit_score = 0.85
        m.preference_score = 0.7
        m.penalty_details = None
        m.base_score = 0.70
        m.penalties = 0.05
        m.required_coverage = 0.90
        m.preferred_requirement_coverage = 0.60
        m.total_requirements = 10
        m.matched_requirements_count = 8
        m.match_type = "exact"
        m.status = "active"
        m.created_at = datetime.now(timezone.utc)
        m.calculated_at = datetime.now(timezone.utc)
        m.fit_components = {
            "fit_confidence": 0.78,
            "fit_scorer": {"name": "threshold_semantic_fit", "version": "1"},
            "fit_explanation": {"summary": "8/10 covered."},
        }
        m.preference_components = {
            "preference_mode_used": "semantic_rerank",
            "preference_reason_codes": ["tech_stack_match"],
        }
        return m

    def test_success(self, service, mock_db):
        mock_match = self._make_full_match()
        mock_job = Mock(
            id="job-1", title="Developer", description="desc",
            company="TC", location_text="Remote", is_remote=True,
            salary_min=100_000, salary_max=150_000, currency="USD",
            min_years_experience=5, requires_degree=True,
            security_clearance=None, job_level="mid",
        )
        req = Mock(
            job_requirement_unit_id="req-1",
            requirement=Mock(text="Python"),
            evidence_text="5y Python", evidence_section="skills",
            similarity_score=0.9, is_covered=True, req_type="required",
        )
        service._get_match_for_owner = Mock(return_value=mock_match)
        mock_db.query.return_value.get.return_value = mock_job
        mock_db.query.return_value.options.return_value.filter.return_value.all.return_value = [req]

        result = service.get_match_detail("match-1")

        assert result.success is True
        assert result.match.match_id == "match-1"
        assert result.match.fit_confidence == 0.78
        assert result.match.preference_components["preference_mode_used"] == "semantic_rerank"
        assert result.job.title == "Developer"
        assert len(result.requirements) == 1

    def test_preference_components_require_dedicated_field(self, service):
        match = self._make_full_match()
        match.preference_components = None
        match.fit_components.update(
            {
                "preference_mode_used": "semantic_rerank",
                "preference_reason_codes": ["tech_stack_match"],
            }
        )

        detail = service._to_match_detail(match, {}, owner_id=None, tenant_id=None)

        assert detail.preference_components is None
        assert "preference_mode_used" not in (detail.fit_components or {})

    def test_fit_components_exclude_preference_payload(self, service):
        match = self._make_full_match()
        match.fit_components = {
            "fit_confidence": 0.78,
            "preference_mode_used": "semantic_rerank",
        }
        match.preferred_requirement_coverage = 0.42
        match.preference_components = None

        detail = service._to_match_detail(match, {}, owner_id=None, tenant_id=None)

        assert "preferred_requirement_coverage" not in detail.fit_components
        assert detail.preferred_requirement_coverage == pytest.approx(0.42)
        assert "preference_mode_used" not in detail.fit_components

    def test_preference_score_nullable_in_detail(self, service, mock_db):
        """preference_score=None in the ORM model is preserved in MatchDetail."""
        mock_match = self._make_full_match()
        mock_match.preference_score = None
        mock_job = Mock(
            id="job-1", title="Dev", description="d", company="C",
            location_text="R", is_remote=True, salary_min=None, salary_max=None,
            currency=None, min_years_experience=None, requires_degree=None,
            security_clearance=None, job_level=None,
        )
        service._get_match_for_owner = Mock(return_value=mock_match)
        mock_db.query.return_value.get.return_value = mock_job
        mock_db.query.return_value.options.return_value.filter.return_value.all.return_value = []

        result = service.get_match_detail("match-1")

        assert result.match.preference_score is None

    def test_llm_marker_fields_mapped_in_detail(self, service):
        match = self._make_full_match()
        service._latest_evaluation_for_match = Mock(
            return_value=SimpleNamespace(
                id="eval-detail",
                status="failed",
                llm_score=None,
                confidence=None,
                completed_at=None,
            )
        )

        detail = service._to_match_detail(
            match,
            {},
            owner_id="owner-1",
            tenant_id=None,
        )

        assert detail.llm_evaluation_status == "failed"
        assert detail.llm_evaluation_id == "eval-detail"
        assert detail.llm_score is None
        assert detail.llm_confidence is None

    def test_not_found_raises(self, service, mock_db):
        from web.backend.exceptions import MatchNotFoundException
        service._get_match_for_owner = Mock(side_effect=MatchNotFoundException("not found"))
        with pytest.raises(MatchNotFoundException, match="not found"):
            service.get_match_detail("nonexistent")

    def test_no_job_returns_null_job_fields(self, service, mock_db):
        mock_match = self._make_full_match()
        service._get_match_for_owner = Mock(return_value=mock_match)
        mock_db.query.return_value.get.return_value = None
        mock_db.query.return_value.options.return_value.filter.return_value.all.return_value = []
        result = service.get_match_detail("match-1")
        assert result.job.job_id is None

    def test_db_error_is_reraised(self, service, mock_db):
        mock_match = self._make_full_match()
        service._get_match_for_owner = Mock(return_value=mock_match)
        mock_db.query.return_value.get.side_effect = RuntimeError("DB gone")
        with pytest.raises(RuntimeError, match="DB gone"):
            service.get_match_detail("match-1")


# ---------------------------------------------------------------------------
# toggle_hidden
# ---------------------------------------------------------------------------

class TestToggleHidden:

    def test_unhidden_to_hidden(self, service, mock_db):
        mock_match = Mock(id="m-1", is_hidden=False)
        with patch("database.repositories.match.MatchRepository") as Repo:
            repo = Mock()
            repo.get_match_by_id.return_value = mock_match
            Repo.return_value = repo
            result = service.toggle_hidden("m-1")
        assert result is True
        assert mock_match.is_hidden is True
        mock_db.commit.assert_called_once()

    def test_hidden_to_unhidden(self, service, mock_db):
        mock_match = Mock(id="m-1", is_hidden=True)
        with patch("database.repositories.match.MatchRepository") as Repo:
            repo = Mock()
            repo.get_match_by_id.return_value = mock_match
            Repo.return_value = repo
            result = service.toggle_hidden("m-1")
        assert result is False

    def test_not_found_raises(self, service, mock_db):
        from web.backend.exceptions import MatchNotFoundException
        with patch("database.repositories.match.MatchRepository") as Repo:
            repo = Mock()
            repo.get_match_by_id.return_value = None
            Repo.return_value = repo
            with pytest.raises(MatchNotFoundException):
                service.toggle_hidden("nonexistent")


# ---------------------------------------------------------------------------
# get_match_explanation
# ---------------------------------------------------------------------------

class TestGetMatchExplanation:

    def test_success(self, service, mock_db):
        m = Mock(id="m-1", fit_components={"fit_explanation": {"summary": "ok"}})
        service._get_match_for_owner = Mock(return_value=m)
        result = service.get_match_explanation("m-1")
        assert result["success"] is True
        assert result["explanation"] == {"summary": "ok"}

    def test_no_explanation_key(self, service, mock_db):
        m = Mock(id="m-1", fit_components={})
        service._get_match_for_owner = Mock(return_value=m)
        result = service.get_match_explanation("m-1")
        assert result["explanation"] is None

    def test_null_fit_components(self, service, mock_db):
        m = Mock(id="m-1", fit_components=None)
        service._get_match_for_owner = Mock(return_value=m)
        result = service.get_match_explanation("m-1")
        assert result["explanation"] is None

    def test_not_found_raises(self, service, mock_db):
        from web.backend.exceptions import MatchNotFoundException
        service._get_match_for_owner = Mock(side_effect=MatchNotFoundException("bad-id"))
        with pytest.raises(MatchNotFoundException):
            service.get_match_explanation("bad-id")


# ---------------------------------------------------------------------------
# Helper: _parse_penalty_details
# ---------------------------------------------------------------------------

class TestParsePenaltyDetails:

    def test_dict_passthrough(self, service):
        d = {"missing_skill": "Python"}
        assert service._parse_penalty_details(d) == d

    def test_valid_json_string(self, service):
        assert service._parse_penalty_details('{"k": 1}') == {"k": 1}

    def test_invalid_json_returns_empty(self, service):
        assert service._parse_penalty_details("not-json") == {}

    def test_none_returns_empty(self, service):
        assert service._parse_penalty_details(None) == {}

    def test_unexpected_type_returns_empty(self, service):
        assert service._parse_penalty_details(42) == {}


# ---------------------------------------------------------------------------
# Helper: _to_job_details
# ---------------------------------------------------------------------------

class TestFitComponentHelpers:
    """Cover the non-dict guard branches in _fit_confidence/_fit_explanation/_fit_scorer."""

    def test_fit_confidence_returns_none_for_non_dict(self, service):
        from web.backend.services.match_service import MatchService
        assert MatchService._fit_confidence("not-a-dict") is None
        assert MatchService._fit_confidence(None) is None

    def test_fit_explanation_returns_none_for_non_dict(self, service):
        from web.backend.services.match_service import MatchService
        assert MatchService._fit_explanation(42) is None

    def test_fit_scorer_returns_none_for_non_dict(self, service):
        from web.backend.services.match_service import MatchService
        assert MatchService._fit_scorer([]) is None

    def test_fit_components_strip_preference_keys(self, service):
        from web.backend.services.match_service import MatchService

        normalized = MatchService._fit_components(
            {
                "preferred_requirement_coverage": 0.35,
                "preference_mode_used": "semantic_rerank",
            }
        )

        assert normalized["preferred_requirement_coverage"] == pytest.approx(0.35)
        assert "preference_mode_used" not in normalized


class TestCanonicalResumeSelection:
    def test_resolves_current_committed_selection_run(self, real_service):
        repo = Mock()
        repo.match_selection.get_latest_current_run_for_owner.return_value = Mock(
            id="run-1",
            resume_fingerprint="fp-current",
        )

        job_uow_cm = MagicMock()
        job_uow_cm.__enter__.return_value = repo
        job_uow_cm.__exit__.return_value = False

        with patch("web.backend.services.match_service.job_uow", return_value=job_uow_cm):
            result = real_service._resolve_canonical_selection(owner_id="user-1")

        assert result.resume_fingerprint == "fp-current"
        assert result.selection_run_id == "run-1"

    def test_returns_none_without_committed_selection_run(self, real_service):
        repo = Mock()
        repo.match_selection.get_latest_current_run_for_owner.return_value = None
        job_uow_cm = MagicMock()
        job_uow_cm.__enter__.return_value = repo
        job_uow_cm.__exit__.return_value = False

        with patch("web.backend.services.match_service.job_uow", return_value=job_uow_cm):
            assert real_service._resolve_canonical_selection(owner_id="user-1") is None


class TestToJobDetails:

    def test_full_job(self, service):
        job = Mock(
            id="j-1", title="SWE", company="C", location_text="NYC",
            is_remote=False, description="desc", salary_min=80_000,
            salary_max=120_000, currency="USD", min_years_experience=3,
            requires_degree=True, security_clearance=None, job_level="mid",
        )
        result = service._to_job_details(job)
        assert result.job_id == "j-1"
        assert result.salary_min == pytest.approx(80_000)

    def test_null_job(self, service):
        result = service._to_job_details(None)
        assert result.job_id is None
        assert result.title is None


# ---------------------------------------------------------------------------
# Helper: _to_requirement_detail
# ---------------------------------------------------------------------------

class TestToRequirementDetail:

    def test_maps_fields(self, service):
        req = Mock(
            job_requirement_unit_id="req-1",
            requirement=Mock(text="Python"),
            evidence_text="5y", evidence_section="skills",
            similarity_score=0.9, is_covered=True, req_type="required",
        )
        result = service._to_requirement_detail(req)
        assert result.requirement_id == "req-1"
        assert result.requirement_text == "Python"
        assert result.is_covered is True


class TestScoringDegradedReason:
    """The /api/matches banner derives a compact code from fit_components."""

    def test_returns_none_for_non_dict_components(self, real_service):
        assert real_service._scoring_degraded_reason(None) is None
        assert real_service._scoring_degraded_reason("string") is None

    def test_returns_none_when_no_fallback_reason(self, real_service):
        assert real_service._scoring_degraded_reason({"other": 1}) is None

    def test_remote_keyword_maps_to_remote_unavailable(self, real_service):
        assert real_service._scoring_degraded_reason(
            {"semantic_fit_fallback_reason": "REMOTE host down"}
        ) == "remote_unavailable"

    def test_local_disabled_maps_to_local_unavailable(self, real_service):
        assert real_service._scoring_degraded_reason(
            {"semantic_fit_fallback_reason": "local provider disabled"}
        ) == "local_unavailable"

    def test_local_no_provider_maps_to_local_unavailable(self, real_service):
        assert real_service._scoring_degraded_reason(
            {"semantic_fit_fallback_reason": "local: no provider configured"}
        ) == "local_unavailable"

    def test_provider_disabled_falls_to_provider_disabled(self, real_service):
        assert real_service._scoring_degraded_reason(
            {"semantic_fit_fallback_reason": "scoring disabled at runtime"}
        ) == "provider_disabled"

    def test_unrecognized_reason_falls_to_degraded(self, real_service):
        assert real_service._scoring_degraded_reason(
            {"semantic_fit_fallback_reason": "weird new reason"}
        ) == "degraded"


class TestPreferenceStatusHelper:
    def test_returns_ranking_snapshot_status_when_present(self, real_service):
        match = SimpleNamespace(
            ranking_snapshot={"preference_status": {"applied": True, "reason": "ok"}},
            preference_components=None,
        )
        assert real_service._preference_status(match) == {"applied": True, "reason": "ok"}

    def test_returns_fallback_reason_from_components(self, real_service):
        match = SimpleNamespace(
            ranking_snapshot=None,
            preference_components={
                "preference_fallback_reason": "preference_reranking_failed:RuntimeError",
                "preference_mode_used": "fit_only_fallback",
            },
        )
        result = real_service._preference_status(match)
        assert result == {
            "applied": False,
            "reason": "preference_reranking_failed:RuntimeError",
            "effective_mode": "fit_only_fallback",
        }

    def test_returns_applied_when_only_mode_used_present(self, real_service):
        match = SimpleNamespace(
            ranking_snapshot=None,
            preference_components={"preference_mode_used": "semantic_rerank"},
        )
        assert real_service._preference_status(match) == {
            "applied": True,
            "effective_mode": "semantic_rerank",
        }

    def test_returns_none_when_components_missing(self, real_service):
        match = SimpleNamespace(ranking_snapshot=None, preference_components=None)
        assert real_service._preference_status(match) is None

    def test_returns_none_when_components_have_no_mode_or_fallback(self, real_service):
        match = SimpleNamespace(ranking_snapshot=None, preference_components={})
        assert real_service._preference_status(match) is None


class TestSelectionItemFiltersByTier:
    def test_excluded_tier_skips_status_and_hidden_filters(self, real_service):
        excluded_match = SimpleNamespace(status="stale", is_hidden=True)
        remote_job = SimpleNamespace(is_remote=True)
        # Excluded tier ignores status/hidden — only min_fit + remote_only apply.
        assert real_service._selection_item_passes_filters(
            excluded_match,
            remote_job,
            45.0,
            status="active",
            min_fit=40.0,
            remote_only=True,
            show_hidden=False,
            tier="excluded",
        )

    def test_excluded_tier_still_respects_min_fit(self, real_service):
        excluded_match = SimpleNamespace(status="active", is_hidden=False)
        remote_job = SimpleNamespace(is_remote=True)
        assert not real_service._selection_item_passes_filters(
            excluded_match,
            remote_job,
            10.0,
            status="all",
            min_fit=40.0,
            remote_only=False,
            show_hidden=True,
            tier="excluded",
        )

    def test_excluded_tier_passes_when_read_time_min_fit_is_relaxed(self, real_service):
        excluded_match = SimpleNamespace(status="active", is_hidden=False)
        remote_job = SimpleNamespace(is_remote=True)
        assert real_service._selection_item_passes_filters(
            excluded_match,
            remote_job,
            10.0,
            status="all",
            min_fit=0.0,
            remote_only=False,
            show_hidden=True,
            tier="excluded",
        )


class TestCoverageFocusedHelpers:
    def test_page_limit_and_ranked_match_helpers(self, real_service):
        matches = [1, 2, 3, 4]

        assert real_service._normalize_page_limit(tier="all", limit=None) == 100
        assert real_service._normalize_page_limit(tier="primary", limit=None) is None
        assert real_service._normalize_page_limit(tier="primary", limit=999) == 500
        assert real_service._page_ranked_matches(matches, limit=None, offset=2) == [3, 4]
        assert real_service._page_ranked_matches(matches, limit=2, offset=-5) == [1, 2]
        assert real_service._page_ranked_matches(matches, limit=0, offset=1) == []

    def test_attach_latest_evaluations_handles_empty_exception_non_iterable_and_success(
        self,
        real_service,
        mock_db,
    ):
        real_service._has_real_query_session = Mock(return_value=True)
        real_service._attach_latest_evaluations([], owner_id="owner-1", tenant_id=None)
        real_service._attach_latest_evaluations(
            [SimpleNamespace(id=None)],
            owner_id="owner-1",
            tenant_id=None,
        )

        query = MagicMock()
        mock_db.query.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        query.all.side_effect = RuntimeError("db down")
        with patch("web.backend.services.match_service.record_match_query_degraded") as record:
            real_service._attach_latest_evaluations(
                [SimpleNamespace(id="match-1")],
                owner_id="owner-1",
                tenant_id=None,
            )
        record.assert_called_once_with("llm_evaluation_lookup_unavailable")

        query.all.side_effect = None
        query.all.return_value = object()
        real_service._attach_latest_evaluations(
            [SimpleNamespace(id="match-1")],
            owner_id="owner-1",
            tenant_id=None,
        )

        evaluation = SimpleNamespace(job_match_id="match-1")
        match = SimpleNamespace(id="match-1")
        query.all.return_value = [evaluation]
        real_service._attach_latest_evaluations(
            [match],
            owner_id="owner-1",
            tenant_id="tenant-1",
        )
        assert match.llm_evaluation is evaluation

    def test_llm_policy_metadata_and_apply_rerank_early_exits(self, real_service):
        assert real_service._llm_policy_metadata(owner_id=None)["reason"] == "owner_missing"

        with patch(
            "web.backend.services.match_service.get_result_policy_store",
            side_effect=RuntimeError("policy down"),
        ), patch("web.backend.services.match_service.record_match_query_degraded") as record:
            metadata = real_service._llm_policy_metadata(owner_id="owner-1")

        assert metadata["reason"] == "policy_unavailable"
        record.assert_called_once_with("policy_unavailable")

        candidate = SimpleNamespace(id="m1")
        assert real_service._apply_llm_rerank([], owner_id="owner-1", tenant_id=None)["reason"] == "empty_primary_pool"
        assert (
            real_service._apply_llm_rerank(
                [candidate],
                owner_id="owner-1",
                tenant_id=None,
                policy_metadata={"available": False, "unavailable_reason": "quota"},
            )["reason"]
            == "quota"
        )
        assert (
            real_service._apply_llm_rerank(
                [candidate],
                owner_id="owner-1",
                tenant_id=None,
                policy_metadata={"available": True, "enabled": False, "top_n": 1},
            )["reason"]
            == "disabled"
        )
        assert (
            real_service._apply_llm_rerank(
                [candidate],
                owner_id="owner-1",
                tenant_id=None,
                policy_metadata={"available": True, "enabled": True, "top_n": 0},
            )["reason"]
            == "top_n_zero"
        )

    def test_apply_llm_rerank_marks_no_eligible_and_sorts_eligible_window(self, real_service):
        no_eval = SimpleNamespace(id="m1", llm_original_rank=1, llm_evaluation=None)
        with patch("web.backend.services.match_service.MatchLlmEvaluationService") as judge_cls:
            judge_cls.return_value.evaluation_effectiveness.return_value = {
                "effective_for_rerank": False,
                "ignored_for_rerank_reason": "stale",
                "stale_status": "stale",
            }
            metadata = real_service._apply_llm_rerank(
                [no_eval],
                owner_id="owner-1",
                tenant_id=None,
                policy_metadata={"available": True, "enabled": True, "top_n": 1},
            )
        assert metadata["reason"] == "no_current_successful_evaluations"
        assert no_eval.llm_reranked_rank == 1

        low = SimpleNamespace(
            id="low",
            llm_original_rank=1,
            llm_evaluation=SimpleNamespace(llm_score=10, confidence=0.9),
        )
        high = SimpleNamespace(
            id="high",
            llm_original_rank=2,
            llm_evaluation=SimpleNamespace(llm_score=99, confidence=0.7),
        )
        with patch("web.backend.services.match_service.MatchLlmEvaluationService") as judge_cls:
            judge_cls.return_value.evaluation_effectiveness.return_value = {
                "effective_for_rerank": True,
                "ignored_for_rerank_reason": None,
                "stale_status": "current",
            }
            pool = [low, high]
            metadata = real_service._apply_llm_rerank(
                pool,
                owner_id="owner-1",
                tenant_id=None,
                policy_metadata={"available": True, "enabled": True, "top_n": 2},
            )

        assert metadata["reason"] == "applied"
        assert [candidate.id for candidate in pool] == ["high", "low"]
        assert high.llm_reranked_rank == 1

    def test_real_query_session_detection_and_owner_lookup_branches(self, real_service, mock_db):
        real_service.db = SimpleNamespace(query=None)
        assert real_service._has_real_query_session() is False
        real_service.db = mock_db
        assert real_service._has_real_query_session() is False

        query = MagicMock()
        mock_db.query.return_value = query
        query.join.return_value = query
        query.filter.return_value = query
        query.one_or_none.return_value = None

        with pytest.raises(Exception, match="not found"):
            real_service._get_match_for_owner(
                "match-1",
                owner_id="owner-1",
                tenant_id="tenant-1",
            )

        query.one_or_none.return_value = SimpleNamespace(id="match-1")
        assert real_service._get_match_for_owner("match-1").id == "match-1"

    def test_toggle_hidden_rejects_tenant_mismatch_and_excluded_owner_match(
        self,
        real_service,
        mock_db,
    ):
        match = SimpleNamespace(
            id="match-1",
            job_post_id="job-1",
            job_post=SimpleNamespace(tenant_id="tenant-a"),
            is_hidden=False,
        )
        with patch("database.repositories.match.MatchRepository") as repo_cls:
            repo_cls.return_value.get_match_by_id_for_owner.return_value = match
            with pytest.raises(Exception, match="not found"):
                real_service.toggle_hidden(
                    "match-1",
                    owner_id="owner-1",
                    tenant_id="tenant-b",
                )

        with patch("database.repositories.match.MatchRepository") as repo_cls:
            repo_cls.return_value.get_match_by_id_for_owner.return_value = match
            real_service._selection_tier_for_current_owner_run = Mock(return_value="excluded")
            with pytest.raises(Exception, match="Excluded matches"):
                real_service.toggle_hidden("match-1", owner_id="owner-1")

    def test_optional_attr_latest_eval_marker_and_job_detail_helpers(self, real_service, mock_db):
        assert real_service._optional_int_attr(SimpleNamespace(value=True), "value") is None
        assert real_service._optional_int_attr(SimpleNamespace(value="bad"), "value") is None
        assert real_service._optional_str_attr(SimpleNamespace(value=123), "value") is None

        query = MagicMock()
        mock_db.query.return_value = query
        query.filter.return_value = query
        query.order_by.return_value = query
        evaluation = SimpleNamespace(id="eval-1")
        query.first.return_value = evaluation
        match = SimpleNamespace(id="match-1", resume_fingerprint="fp")

        assert real_service._latest_evaluation_for_match(
            match,
            owner_id="owner-1",
            tenant_id=None,
        ) is evaluation
        assert real_service._latest_evaluation_for_match(
            match,
            owner_id="owner-1",
            tenant_id="tenant-1",
        ) is evaluation

        assert real_service._llm_marker_fields(None)["llm_evaluation_status"] is None
        judged_at = datetime.now(timezone.utc)
        fields = real_service._llm_marker_fields(
            SimpleNamespace(
                id="eval-1",
                status="succeeded",
                llm_score="95.5",
                confidence="0.8",
                completed_at=judged_at,
                llm_effectiveness={"effective_for_rerank": True, "stale_status": "current"},
            )
        )
        assert fields["llm_score"] == 95.5
        assert fields["llm_effective_for_rerank"] is True

        job = SimpleNamespace(
            id="job-1",
            title="Backend Engineer",
            company="Example",
            location_text="Remote",
            is_remote=True,
            description=None,
            description_source=None,
            description_completeness=None,
            description_warning_code=None,
            salary_min=None,
            salary_max=None,
            currency="USD",
            min_years_experience=None,
            requires_degree=False,
            security_clearance=None,
            job_level="senior",
        )
        details = real_service._to_job_details(job)
        assert details.description_completeness == "missing"
