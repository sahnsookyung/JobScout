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
        if (float(getattr(match, "fit_score", 0.0) or 0.0) >= fit_floor_used)
        and (
            required_coverage_floor_used is None
            or float(getattr(match, "jd_required_coverage", 0.0) or 0.0)
            >= required_coverage_floor_used
        )
    ]

    candidate_pool_size = len(eligible)
    if candidate_pool_size:
        rank_matches(eligible, ranking_context)

    selected_matches = eligible[:top_k_used] if top_k_used > 0 else []
    item_snapshots: list[MatchSelectionItemSnapshot] = []
    for index, match in enumerate(selected_matches, start=1):
        fit_score = float(getattr(match, "fit_score", 0.0) or 0.0)
        preference_score = getattr(match, "preference_score", None)
        job_similarity = float(getattr(match, "job_similarity", 0.0) or 0.0)
        required_coverage = float(getattr(match, "jd_required_coverage", 0.0) or 0.0)
        explanation = getattr(match, "ranking_explanation", None)
        ranking_snapshot = asdict(explanation) if explanation is not None else {}
        item_snapshots.append(
            MatchSelectionItemSnapshot(
                job_id=str(match.job.id),
                rank_position=index,
                fit_score_at_selection=fit_score,
                preference_score_at_selection=(
                    None if preference_score is None else float(preference_score)
                ),
                job_similarity_at_selection=job_similarity,
                required_coverage_at_selection=required_coverage,
                alert_eligible=fit_score >= notification_fit_floor_used,
                dominant_reason_code=(
                    ranking_snapshot.get("dominant_reason_code")
                    if ranking_snapshot
                    else None
                ),
                explanation_label=(
                    ranking_snapshot.get("explanation_label")
                    if ranking_snapshot
                    else None
                ),
                ranking_snapshot=ranking_snapshot,
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
