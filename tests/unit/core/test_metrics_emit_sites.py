"""Emit-site smoke tests for PR-B.

Each test invokes a code path that should fire exactly one of the
typed ``record_*`` helpers, then asserts the Prometheus sample delta.
The autouse ``_reset_prometheus_metrics`` fixture in
``tests/conftest.py`` zeros children between tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import REGISTRY


def _sample(name: str, labels: dict[str, str]) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _match(
    job_id: str,
    *,
    fit_score: float,
    preference_score: float | None,
    job_similarity: float,
    required_coverage: float,
):
    return SimpleNamespace(
        id=f"match-{job_id}",
        job=SimpleNamespace(id=job_id),
        fit_score=fit_score,
        preference_score=preference_score,
        job_similarity=job_similarity,
        jd_required_coverage=required_coverage,
    )


class TestScorerRouteEmits:
    def test_threshold_scorer_emits_route(self):
        from core.scorer.semantic_fit import ThresholdSemanticFitScorer

        scorer = ThresholdSemanticFitScorer()
        before = _sample("jobscout_scorer_route_total", {"route": "threshold"})

        preliminary = MagicMock()
        preliminary.requirement_matches = []
        preliminary.missing_requirements = []
        preliminary.job = MagicMock()
        config = MagicMock()
        config.semantic_fit = MagicMock(enabled=True)

        try:
            scorer.score(preliminary, fit_penalties=0.0, config=config)
        except Exception:
            # Result construction can fail with the mock; we only care the
            # route was recorded at function entry.
            pass

        after = _sample("jobscout_scorer_route_total", {"route": "threshold"})
        assert after - before == 1


class TestSelectionTierEmits:
    def _ranking_context(self):
        from core.ranking import RankingContext, RankingMode
        from core.ranking.policy import RankingConfig

        return RankingContext(
            mode=RankingMode.FIT_FIRST,
            config=RankingConfig(active_default_mode="fit_first"),
        )

    def test_primary_snapshot_emits_counter(self):
        from core.match_selection.engine import select_matches

        before = _sample(
            "jobscout_selection_tier_items_total",
            {"tier": "primary", "reason": "none"},
        )
        matches = [
            _match("top", fit_score=90.0, preference_score=0.2, job_similarity=0.8, required_coverage=0.9),
        ]
        select_matches(
            matches,
            ranking_context=self._ranking_context(),
            fit_floor_used=50.0,
            required_coverage_floor_used=None,
            top_k_used=1,
            notification_fit_floor_used=70.0,
            resume_resolution_reason="test",
        )
        after = _sample(
            "jobscout_selection_tier_items_total",
            {"tier": "primary", "reason": "none"},
        )
        assert after - before == 1

    def test_excluded_beyond_top_k_emits_counter(self):
        from core.match_selection.engine import select_matches

        before = _sample(
            "jobscout_selection_tier_items_total",
            {"tier": "excluded", "reason": "beyond_top_k"},
        )
        matches = [
            _match("a", fit_score=90.0, preference_score=0.2, job_similarity=0.8, required_coverage=0.9),
            _match("b", fit_score=80.0, preference_score=0.2, job_similarity=0.7, required_coverage=0.9),
        ]
        select_matches(
            matches,
            ranking_context=self._ranking_context(),
            fit_floor_used=50.0,
            required_coverage_floor_used=None,
            top_k_used=1,
            notification_fit_floor_used=70.0,
            resume_resolution_reason="test",
        )
        after = _sample(
            "jobscout_selection_tier_items_total",
            {"tier": "excluded", "reason": "beyond_top_k"},
        )
        assert after - before == 1

    def test_truncated_excluded_emits_counter(self):
        from core.match_selection.engine import select_matches

        before = _sample(
            "jobscout_selection_tier_items_total",
            {"tier": "excluded", "reason": "truncated"},
        )
        matches = [
            _match("primary", fit_score=90.0, preference_score=0.2, job_similarity=0.8, required_coverage=0.9),
            _match("excluded-low", fit_score=35.0, preference_score=0.2, job_similarity=0.4, required_coverage=0.9),
            _match("excluded-high", fit_score=49.0, preference_score=0.2, job_similarity=0.4, required_coverage=0.9),
        ]
        with patch("core.match_selection.engine.EXCLUDED_STORAGE_CAP", 1):
            select_matches(
                matches,
                ranking_context=self._ranking_context(),
                fit_floor_used=50.0,
                required_coverage_floor_used=None,
                top_k_used=1,
                notification_fit_floor_used=70.0,
                resume_resolution_reason="test",
            )
        after = _sample(
            "jobscout_selection_tier_items_total",
            {"tier": "excluded", "reason": "truncated"},
        )
        assert after - before == 1


class TestPreferenceStatusEmits:
    def test_unconfigured_when_no_preferences(self):
        from services.scorer_matcher.candidate_preferences import (
            apply_preference_semantic_reranking,
        )

        before = _sample(
            "jobscout_preference_reranker_status_total",
            {"applied": "false", "reason": "unconfigured"},
        )
        result = apply_preference_semantic_reranking(
            scored_matches=[],
            preferences=None,
            config=MagicMock(),
        )
        assert result.status.applied is False
        after = _sample(
            "jobscout_preference_reranker_status_total",
            {"applied": "false", "reason": "unconfigured"},
        )
        assert after - before == 1

    def test_disabled_when_soft_preferences_empty(self):
        from services.scorer_matcher.candidate_preferences import (
            apply_preference_semantic_reranking,
        )

        before = _sample(
            "jobscout_preference_reranker_status_total",
            {"applied": "false", "reason": "disabled"},
        )
        apply_preference_semantic_reranking(
            scored_matches=[],
            preferences={"soft_preferences": "   "},
            config=MagicMock(),
        )
        after = _sample(
            "jobscout_preference_reranker_status_total",
            {"applied": "false", "reason": "disabled"},
        )
        assert after - before == 1


class TestEmailEventEmits:
    def _email_sample(self, event: str) -> float:
        return _sample("jobscout_email_verification_events_total", {"event": event})

    def test_invalid_address_on_bad_input(self):
        from web.backend.services.notification_service import (
            NotificationServiceWrapper,
        )
        from notification.exceptions import NotificationConfigurationError

        wrapper = NotificationServiceWrapper.__new__(NotificationServiceWrapper)
        before = self._email_sample("invalid_address")

        user = MagicMock()
        user.email = "me@example.com"
        with pytest.raises(NotificationConfigurationError):
            wrapper.send_email_override_verification(user, "not-an-email")
        assert self._email_sample("invalid_address") - before == 1


class TestEvidenceRerankLatencyEmits:
    def test_observation_increments_histogram_count(self):
        from core.metrics import evidence_rerank_latency_ms

        before = REGISTRY.get_sample_value(
            "jobscout_evidence_rerank_latency_ms_count", {}
        ) or 0
        evidence_rerank_latency_ms.observe(123.4)
        after = REGISTRY.get_sample_value(
            "jobscout_evidence_rerank_latency_ms_count", {}
        ) or 0
        assert after - before == 1
