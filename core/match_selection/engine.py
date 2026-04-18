"""Pure selection engine for canonical match publication.

Two-tier model (see plan §C):
- Every scored match that enters this engine emits a snapshot.
- Items passing the fit floor, coverage floor, and the top-K rank cut are
  `selection_tier="primary"`. They become `selected_matches` (what callers
  index as "the selection").
- Items that fail one of those gates are `selection_tier="excluded"` with
  `excluded_reason` set. They are NOT in `selected_matches` — callers that
  only care about the canonical selection behave unchanged — but they ARE in
  `item_snapshots` so persistence can store them for UI "show excluded".
- A storage cap of EXCLUDED_STORAGE_CAP bounds the excluded-tier volume for
  pathological runs; overflow items are dropped with reason 'truncated'.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Optional

from core.match_selection.contracts import (
    MatchSelectionItemSnapshot,
    MatchSelectionPolicySnapshot,
    MatchSelectionResult,
)
from core.ranking import RankingContext, rank_matches

EXCLUDED_STORAGE_CAP = 500


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
    # Partition scored matches by floor gates. Items that fail any floor can
    # never be primary; items that pass may or may not make it through top-K.
    eligible: list[Any] = []
    excluded_by_floor: list[tuple[Any, str]] = []
    for match in matches:
        reason = _excluded_reason_from_floors(
            match, fit_floor_used, required_coverage_floor_used
        )
        if reason is None:
            eligible.append(match)
        else:
            excluded_by_floor.append((match, reason))

    candidate_pool_size = len(eligible)
    if candidate_pool_size:
        rank_matches(eligible, ranking_context)

    if top_k_used > 0:
        selected_matches = eligible[:top_k_used]
        beyond_top_k = eligible[top_k_used:]
    else:
        selected_matches = []
        beyond_top_k = list(eligible)

    item_snapshots: list[MatchSelectionItemSnapshot] = []
    for index, match in enumerate(selected_matches, start=1):
        item_snapshots.append(
            _item_snapshot_from_match(
                match,
                rank_position=index,
                notification_fit_floor_used=notification_fit_floor_used,
                selection_tier="primary",
                excluded_reason=None,
            )
        )

    # Excluded tier: beyond_top_k first (already rank-ordered), then floor
    # failures. Continue rank_position past primary so the unique index
    # (selection_run_id, rank_position) holds.
    excluded_candidates: list[tuple[Any, str]] = [
        (match, "beyond_top_k") for match in beyond_top_k
    ]
    excluded_candidates.extend(excluded_by_floor)

    excluded_budget = max(0, EXCLUDED_STORAGE_CAP)
    next_rank = len(selected_matches) + 1
    truncated_count = 0
    for match, reason in excluded_candidates:
        if excluded_budget <= 0:
            truncated_count += 1
            continue
        item_snapshots.append(
            _item_snapshot_from_match(
                match,
                rank_position=next_rank,
                notification_fit_floor_used=notification_fit_floor_used,
                selection_tier="excluded",
                excluded_reason=reason,
            )
        )
        next_rank += 1
        excluded_budget -= 1

    policy_snapshot = MatchSelectionPolicySnapshot.from_ranking_context(
        ranking_context=ranking_context,
        fit_floor_used=fit_floor_used,
        required_coverage_floor_used=required_coverage_floor_used,
        notification_fit_floor_used=notification_fit_floor_used,
        top_k_used=top_k_used,
        candidate_pool_size=candidate_pool_size,
        selected_count=len(selected_matches),
        alert_candidate_count=sum(
            1 for item in item_snapshots
            if item.selection_tier == "primary" and item.alert_eligible
        ),
        resume_resolution_reason=resume_resolution_reason,
        task_id=task_id,
    )
    if truncated_count:
        # Surface truncation in the policy snapshot via ranking_config_snapshot
        # piggyback. Auditable in the run row without a new column.
        policy_snapshot.ranking_config_snapshot["excluded_truncated_count"] = truncated_count
    return MatchSelectionResult(
        selected_matches=selected_matches,
        item_snapshots=item_snapshots,
        policy_snapshot=policy_snapshot,
    )


def _score(match: Any, attribute: str) -> float:
    return float(getattr(match, attribute, 0.0) or 0.0)


def _excluded_reason_from_floors(
    match: Any,
    fit_floor_used: float,
    required_coverage_floor_used: Optional[float],
) -> Optional[str]:
    if _score(match, "fit_score") < fit_floor_used:
        return "below_min_fit"
    if (
        required_coverage_floor_used is not None
        and _score(match, "jd_required_coverage") < required_coverage_floor_used
    ):
        return "below_coverage_floor"
    return None


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
    selection_tier: str = "primary",
    excluded_reason: Optional[str] = None,
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
        alert_eligible=selection_tier == "primary" and fit_score >= notification_fit_floor_used,
        dominant_reason_code=ranking_snapshot.get("dominant_reason_code"),
        explanation_label=ranking_snapshot.get("explanation_label"),
        ranking_snapshot=ranking_snapshot,
        selection_tier=selection_tier,
        excluded_reason=excluded_reason,
    )
