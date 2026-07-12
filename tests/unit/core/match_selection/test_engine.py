"""Unit tests for the canonical match selection engine."""

from types import SimpleNamespace
from unittest.mock import patch

from core.match_selection.engine import select_matches
from core.ranking import RankingContext, RankingMode
from core.ranking.policy import RankingConfig


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


def test_select_matches_applies_fit_floor_ranking_and_top_k() -> None:
    ranking_context = RankingContext(
        mode=RankingMode.BALANCED,
        config=RankingConfig(
            active_default_mode="balanced",
            balanced_w_pref=0.7,
            balanced_w_fit=0.3,
        ),
    )
    matches = [
        _match("fit-only", fit_score=95.0, preference_score=10, job_similarity=0.8, required_coverage=0.9),
        _match("preferred", fit_score=80.0, preference_score=95, job_similarity=0.8, required_coverage=0.9),
        _match("below-floor", fit_score=45.0, preference_score=100.0, job_similarity=0.9, required_coverage=0.9),
    ]

    result = select_matches(
        matches,
        ranking_context=ranking_context,
        fit_floor_used=50.0,
        required_coverage_floor_used=None,
        top_k_used=1,
        notification_fit_floor_used=70.0,
        resume_resolution_reason="test",
        task_id="task-1",
    )

    assert [match.job.id for match in result.selected_matches] == ["preferred"]
    assert result.policy_snapshot.candidate_pool_size == 2
    assert result.policy_snapshot.selected_count == 1
    assert result.item_snapshots[0].job_id == "preferred"
    assert result.item_snapshots[0].alert_eligible is True
    assert result.item_snapshots[0].dominant_reason_code == "balanced_blend"


def test_select_matches_applies_required_coverage_floor() -> None:
    ranking_context = RankingContext(
        mode=RankingMode.FIT_FIRST,
        config=RankingConfig(active_default_mode="fit_first"),
    )
    matches = [
        _match("kept", fit_score=75.0, preference_score=20, job_similarity=0.6, required_coverage=0.8),
        _match("dropped", fit_score=90.0, preference_score=90, job_similarity=0.9, required_coverage=0.4),
    ]

    result = select_matches(
        matches,
        ranking_context=ranking_context,
        fit_floor_used=50.0,
        required_coverage_floor_used=0.6,
        top_k_used=5,
        notification_fit_floor_used=80.0,
        resume_resolution_reason="test",
    )

    assert [match.job.id for match in result.selected_matches] == ["kept"]
    assert result.item_snapshots[0].alert_eligible is False


def test_select_matches_with_zero_top_k_promotes_all_candidates_to_excluded() -> None:
    ranking_context = RankingContext(
        mode=RankingMode.BALANCED,
        config=RankingConfig(active_default_mode="balanced"),
    )
    matches = [
        _match("first", fit_score=80.0, preference_score=20, job_similarity=0.7, required_coverage=0.9),
        _match("second", fit_score=75.0, preference_score=10, job_similarity=0.6, required_coverage=0.8),
    ]

    result = select_matches(
        matches,
        ranking_context=ranking_context,
        fit_floor_used=50.0,
        required_coverage_floor_used=None,
        top_k_used=0,
        notification_fit_floor_used=70.0,
        resume_resolution_reason="test",
    )

    assert result.selected_matches == []
    assert [item.selection_tier for item in result.item_snapshots] == ["excluded", "excluded"]
    assert [item.excluded_reason for item in result.item_snapshots] == ["beyond_top_k", "beyond_top_k"]


def test_select_matches_truncates_excluded_by_best_fit_not_input_order() -> None:
    ranking_context = RankingContext(
        mode=RankingMode.FIT_FIRST,
        config=RankingConfig(active_default_mode="fit_first"),
    )
    matches = [
        _match("excluded-low", fit_score=35.0, preference_score=20, job_similarity=0.4, required_coverage=0.9),
        _match("excluded-high", fit_score=49.0, preference_score=20, job_similarity=0.4, required_coverage=0.9),
        _match("primary", fit_score=90.0, preference_score=20, job_similarity=0.8, required_coverage=0.9),
    ]

    with patch("core.match_selection.engine.EXCLUDED_STORAGE_CAP", 1):
        result = select_matches(
            matches,
            ranking_context=ranking_context,
            fit_floor_used=50.0,
            required_coverage_floor_used=None,
            top_k_used=1,
            notification_fit_floor_used=70.0,
            resume_resolution_reason="test",
        )

    assert [item.job_id for item in result.item_snapshots] == ["primary", "excluded-high"]
    assert result.policy_snapshot.ranking_config_snapshot["excluded_truncated_count"] == 1


def test_select_matches_disabled_two_tier_does_not_report_truncation() -> None:
    ranking_context = RankingContext(
        mode=RankingMode.FIT_FIRST,
        config=RankingConfig(active_default_mode="fit_first"),
    )
    matches = [
        _match("primary", fit_score=90.0, preference_score=20, job_similarity=0.8, required_coverage=0.9),
        _match("excluded", fit_score=45.0, preference_score=20, job_similarity=0.4, required_coverage=0.9),
    ]

    result = select_matches(
        matches,
        ranking_context=ranking_context,
        fit_floor_used=50.0,
        required_coverage_floor_used=None,
        top_k_used=5,
        notification_fit_floor_used=70.0,
        resume_resolution_reason="test",
        two_tier_enabled=False,
    )

    assert [item.job_id for item in result.item_snapshots] == ["primary"]
    assert "excluded_truncated_count" not in result.policy_snapshot.ranking_config_snapshot
