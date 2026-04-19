"""Tests for the pipeline run-summary observability helpers (§I)."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from services.scorer_matcher.candidate_preferences import PreferenceStatus
from services.scorer_matcher.pipeline import (
    _count_reranked_requirements,
    _degraded_reason_breakdown,
    _log_pipeline_run_summary,
    _tier_breakdown,
    _truncated_excluded_count,
)


def _item(tier: str = "primary", excluded_reason=None):
    return SimpleNamespace(selection_tier=tier, excluded_reason=excluded_reason)


def _dto(fit_components=None, requirement_matches=()):
    return SimpleNamespace(
        fit_components=fit_components,
        requirement_matches=list(requirement_matches),
    )


class TestTierBreakdown:
    def test_empty_input_returns_empty_maps(self):
        assert _tier_breakdown([]) == ({}, {})

    def test_primary_only_run(self):
        counts, reasons = _tier_breakdown([_item(), _item(), _item()])
        assert counts == {"primary": 3}
        assert reasons == {}

    def test_counts_reasons_for_excluded_items(self):
        items = [
            _item(),
            _item(tier="excluded", excluded_reason="below_min_fit"),
            _item(tier="excluded", excluded_reason="below_min_fit"),
            _item(tier="excluded", excluded_reason="beyond_top_k"),
        ]
        counts, reasons = _tier_breakdown(items)
        assert counts == {"primary": 1, "excluded": 3}
        assert reasons == {"below_min_fit": 2, "beyond_top_k": 1}

    def test_missing_tier_defaults_to_primary(self):
        weird = SimpleNamespace()  # no selection_tier attr
        counts, reasons = _tier_breakdown([weird])
        assert counts == {"primary": 1}
        assert reasons == {}

    def test_falsy_tier_coerces_to_primary(self):
        counts, _ = _tier_breakdown([_item(tier=None)])
        assert counts == {"primary": 1}

    def test_excluded_without_reason_is_counted_as_unspecified(self):
        _, reasons = _tier_breakdown([_item(tier="excluded", excluded_reason=None)])
        assert reasons == {"unspecified": 1}


class TestCountReranked:
    def test_counts_requirements_with_numeric_evidence_score(self):
        req_scored = SimpleNamespace(evidence_score=0.82)
        req_unscored = SimpleNamespace(evidence_score=None)
        dto_a = _dto(requirement_matches=[req_scored, req_unscored])
        dto_b = _dto(requirement_matches=[req_scored])
        assert _count_reranked_requirements([dto_a, dto_b]) == 2

    def test_handles_missing_requirement_matches(self):
        dto_empty = SimpleNamespace(fit_components=None)  # no requirement_matches attr
        assert _count_reranked_requirements([dto_empty]) == 0

    def test_empty_input(self):
        assert _count_reranked_requirements([]) == 0


class TestDegradedReasonBreakdown:
    def test_records_fallback_reason(self):
        dto = _dto(fit_components={"semantic_fit_fallback_reason": "remote_unavailable"})
        assert _degraded_reason_breakdown([dto, dto]) == {"remote_unavailable": 2}

    def test_ignores_non_dict_components(self):
        dto = _dto(fit_components="string-not-dict")
        assert _degraded_reason_breakdown([dto]) == {}

    def test_missing_reason_omitted(self):
        dto = _dto(fit_components={"some_other_field": 1})
        assert _degraded_reason_breakdown([dto]) == {}


class TestTruncatedExcludedCount:
    def test_none_policy_returns_zero(self):
        assert _truncated_excluded_count(None) == 0

    def test_missing_snapshot_returns_zero(self):
        assert _truncated_excluded_count(SimpleNamespace(ranking_config_snapshot=None)) == 0

    def test_returns_stored_count(self):
        snap = SimpleNamespace(
            ranking_config_snapshot={"excluded_truncated_count": 17}
        )
        assert _truncated_excluded_count(snap) == 17


class TestLogPipelineRunSummary:
    def test_emits_single_info_log_with_extra_fields(self, caplog):
        items = [
            _item(),
            _item(tier="excluded", excluded_reason="below_min_fit"),
        ]
        dto = _dto(
            fit_components={"semantic_fit_fallback_reason": "remote_unavailable"},
            requirement_matches=[SimpleNamespace(evidence_score=0.9)],
        )
        policy = SimpleNamespace(ranking_config_snapshot={"excluded_truncated_count": 2})
        pref = PreferenceStatus(applied=True, reason="applied")

        with caplog.at_level(logging.INFO, logger="services.scorer_matcher.pipeline"):
            _log_pipeline_run_summary(
                match_dtos=[dto],
                item_snapshots=items,
                preference_status=pref,
                policy_snapshot=policy,
            )

        summary_records = [r for r in caplog.records if "pipeline.run_summary" in r.getMessage()]
        assert len(summary_records) == 1
        record = summary_records[0]
        assert record.__dict__.get("event") == "pipeline.run_summary"
        assert record.__dict__.get("tier_counts") == {"primary": 1, "excluded": 1}
        assert record.__dict__.get("excluded_reasons") == {"below_min_fit": 1}
        assert record.__dict__.get("evidence_rerank_scored") == 1
        assert record.__dict__.get("degraded_reasons") == {"remote_unavailable": 1}
        assert record.__dict__.get("excluded_truncated") == 2
        assert record.__dict__.get("preference_applied") is True
