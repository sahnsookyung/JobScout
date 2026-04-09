"""Pure selection engine for canonical match publication."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Optional

from core.match_selection.contracts import (
    MatchSelectionItemSnapshot,
    MatchSelectionPolicySnapshot,
    MatchSelectionResult,
)
from core.ranking import RankingContext, rank_matches


def select_matches(
    matches: list[Any],
    *,
    ranking_context: RankingContext,
    fit_floor_used: float,
    required_coverage_floor_used: Optional[float],
    top_k_used: int,
    notification_fit_floor_used: float,
    resume_resolution_reason: str,
    task_id: Optional[str] = None,
) -> MatchSelectionResult:
    eligible = [
        match
        for match in matches
        if _passes_selection_floors(match, fit_floor_used, required_coverage_floor_used)
    ]

    candidate_pool_size = len(eligible)
    if candidate_pool_size:
        rank_matches(eligible, ranking_context)

    selected_matches = eligible[:top_k_used] if top_k_used > 0 else []
    item_snapshots: list[MatchSelectionItemSnapshot] = []
    for index, match in enumerate(selected_matches, start=1):
        item_snapshots.append(
            _item_snapshot_from_match(
                match,
                rank_position=index,
                notification_fit_floor_used=notification_fit_floor_used,
            )
        )

    policy_snapshot = MatchSelectionPolicySnapshot.from_ranking_context(
        ranking_context=ranking_context,
        fit_floor_used=fit_floor_used,
        required_coverage_floor_used=required_coverage_floor_used,
        notification_fit_floor_used=notification_fit_floor_used,
        top_k_used=top_k_used,
        candidate_pool_size=candidate_pool_size,
        selected_count=len(selected_matches),
        alert_candidate_count=sum(1 for item in item_snapshots if item.alert_eligible),
        resume_resolution_reason=resume_resolution_reason,
        task_id=task_id,
    )
    return MatchSelectionResult(
        selected_matches=selected_matches,
        item_snapshots=item_snapshots,
        policy_snapshot=policy_snapshot,
    )


def _score(match: Any, attribute: str) -> float:
    return float(getattr(match, attribute, 0.0) or 0.0)


def _passes_selection_floors(
    match: Any,
    fit_floor_used: float,
    required_coverage_floor_used: Optional[float],
) -> bool:
    if _score(match, "fit_score") < fit_floor_used:
        return False
    return (
        required_coverage_floor_used is None
        or _score(match, "jd_required_coverage") >= required_coverage_floor_used
    )


def _ranking_snapshot(match: Any) -> dict[str, Any]:
    explanation = getattr(match, "ranking_explanation", None)
    return asdict(explanation) if explanation is not None else {}


def _preference_score(match: Any) -> Optional[float]:
    value = getattr(match, "preference_score", None)
    return None if value is None else float(value)


def _item_snapshot_from_match(
    match: Any,
    *,
    rank_position: int,
    notification_fit_floor_used: float,
) -> MatchSelectionItemSnapshot:
    fit_score = _score(match, "fit_score")
    ranking_snapshot = _ranking_snapshot(match)
    return MatchSelectionItemSnapshot(
        job_id=str(match.job.id),
        rank_position=rank_position,
        fit_score_at_selection=fit_score,
        preference_score_at_selection=_preference_score(match),
        job_similarity_at_selection=_score(match, "job_similarity"),
        required_coverage_at_selection=_score(match, "jd_required_coverage"),
        alert_eligible=fit_score >= notification_fit_floor_used,
        dominant_reason_code=ranking_snapshot.get("dominant_reason_code"),
        explanation_label=ranking_snapshot.get("explanation_label"),
        ranking_snapshot=ranking_snapshot,
    )
