#!/usr/bin/env python3
"""
Tests for Match Service
Covers: web/backend/services/match_service.py

Key invariants verified:
  - Stage 1: DB query is bounded by max_ranking_candidates (not top_k)
  - Stage 2: rank_matches() is called on the full retrieved pool
  - Stage 3: top_k truncation happens AFTER ranking (pool[:effective_k])
  - ranking_mode parameter is forwarded to the ranking engine
  - preference_score=None is preserved (distinct from 0.0)
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy.orm import Session
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    return Mock(spec=Session)


@pytest.fixture
def service(mock_db):
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
    m.preferred_coverage = 0.60
    m.match_type = "requirements_only"
    m.is_hidden = False
    m.created_at = datetime.now(timezone.utc)
    m.calculated_at = datetime.now(timezone.utc)
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


# ---------------------------------------------------------------------------
# Three-stage pipeline correctness
# ---------------------------------------------------------------------------

class TestThreeStagePipeline:
    """
    The canonical correctness contract:
      1. DB query is LIMIT max_ranking_candidates (config-driven), NOT top_k.
      2. rank_matches() receives the FULL retrieved pool.
      3. Truncation to effective_top_k happens AFTER ranking.
    """

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_db_limit_uses_max_ranking_candidates_not_top_k(
        self, mock_store, mock_rank, mock_db, service
    ):
        """query.limit() receives max_ranking_candidates (500), not top_k (5)."""
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(balanced_w_pref=0.6, balanced_w_fit=0.4, max_ranking_candidates=500)
        mock_store.return_value.get_current_config.return_value = cfg

        q = _wire_query(mock_db, [])
        mock_rank.side_effect = lambda pool, ctx: pool  # passthrough

        service.get_matches(top_k=5)

        q.limit.assert_called_once_with(500)

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
        _wire_query(mock_db, pool)

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
        _wire_query(mock_db, pool)
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
        _wire_query(mock_db, pool)
        mock_rank.side_effect = lambda p, ctx: p

        results = service.get_matches()  # top_k=None

        assert len(results) == 3

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

        _wire_query(mock_db, [])
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

        _wire_query(mock_db, [])
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
        q = _wire_query(mock_db, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(min_fit=50.0)

        # filter must have been called (min_fit guard + hidden guard)
        assert q.filter.called

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_empty_pool_returns_empty_list(
        self, mock_store, mock_rank, mock_db, service
    ):
        from core.ranking.policy import RankingConfig
        cfg = RankingConfig(balanced_w_pref=0.6, balanced_w_fit=0.4)
        mock_store.return_value.get_current_config.return_value = cfg

        _wire_query(mock_db, [])
        mock_rank.side_effect = lambda p, ctx: p

        assert service.get_matches() == []


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
        q = _wire_query(mock_db, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(status="active")

        q.filter.assert_called()

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_status_all_skips_status_filter(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        q = _wire_query(mock_db, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(status="all")

        assert q.order_by.called

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_remote_only_joins_job_post(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        q = _wire_query(mock_db, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(remote_only=True)

        q.join.assert_called()

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_show_hidden_false_filters_hidden(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        q = _wire_query(mock_db, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(show_hidden=False)

        q.filter.assert_called()

    @patch("web.backend.services.match_service.rank_matches")
    @patch("web.backend.services.match_service.get_ranking_policy_store")
    def test_show_hidden_true_skips_hidden_filter(self, mock_store, mock_rank, mock_db, service):
        from core.ranking.policy import RankingConfig
        mock_store.return_value.get_current_config.return_value = RankingConfig(
            balanced_w_pref=0.6, balanced_w_fit=0.4
        )
        q = _wire_query(mock_db, [])
        mock_rank.side_effect = lambda p, ctx: p

        service.get_matches(show_hidden=True)

        filter_calls = [str(c) for c in q.filter.call_args_list]
        assert not any("is_hidden" in c for c in filter_calls)


# ---------------------------------------------------------------------------
# MatchSummary field mapping (preference_score NULL preservation)
# ---------------------------------------------------------------------------

class TestToMatchSummary:

    def test_preference_score_none_preserved(self, service):
        """preference_score=None (evaluator not run) must remain None in output."""
        m = _make_match(preference_score=None)
        result = service._to_match_summary(m)
        assert result.preference_score is None

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
        m.preferred_coverage = 0.60
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
        mock_db.query.return_value.get.side_effect = [mock_match, mock_job]
        mock_db.query.return_value.options.return_value.filter.return_value.all.return_value = [req]

        result = service.get_match_detail("match-1")

        assert result.success is True
        assert result.match.match_id == "match-1"
        assert result.match.fit_confidence == 0.78
        assert result.job.title == "Developer"
        assert len(result.requirements) == 1

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
        mock_db.query.return_value.get.side_effect = [mock_match, mock_job]
        mock_db.query.return_value.options.return_value.filter.return_value.all.return_value = []

        result = service.get_match_detail("match-1")

        assert result.match.preference_score is None

    def test_not_found_raises(self, service, mock_db):
        from web.backend.exceptions import MatchNotFoundException
        mock_db.query.return_value.get.return_value = None
        with pytest.raises(MatchNotFoundException, match="not found"):
            service.get_match_detail("nonexistent")

    def test_no_job_returns_null_job_fields(self, service, mock_db):
        mock_match = self._make_full_match()
        mock_db.query.return_value.get.side_effect = [mock_match, None]
        mock_db.query.return_value.options.return_value.filter.return_value.all.return_value = []
        result = service.get_match_detail("match-1")
        assert result.job.job_id is None

    def test_db_error_is_reraised(self, service, mock_db):
        mock_match = self._make_full_match()
        mock_db.query.return_value.get.side_effect = [mock_match, RuntimeError("DB gone")]
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
        repo.update_hidden_status.assert_called_once_with("m-1", True)
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
        mock_db.query.return_value.get.return_value = m
        result = service.get_match_explanation("m-1")
        assert result["success"] is True
        assert result["explanation"] == {"summary": "ok"}

    def test_no_explanation_key(self, service, mock_db):
        m = Mock(id="m-1", fit_components={})
        mock_db.query.return_value.get.return_value = m
        result = service.get_match_explanation("m-1")
        assert result["explanation"] is None

    def test_null_fit_components(self, service, mock_db):
        m = Mock(id="m-1", fit_components=None)
        mock_db.query.return_value.get.return_value = m
        result = service.get_match_explanation("m-1")
        assert result["explanation"] is None

    def test_not_found_raises(self, service, mock_db):
        from web.backend.exceptions import MatchNotFoundException
        mock_db.query.return_value.get.return_value = None
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
