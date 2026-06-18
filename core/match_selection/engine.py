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
from core.metrics import record_selection_tier_item
from core.ranking import RankingContext, rank_matches

EXCLUDED_STORAGE_CAP = 5000


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
    two_tier_enabled: bool = True,
) -> MatchSelectionResult:
    eligible, excluded_by_floor = _partition_matches(
        matches,
        fit_floor_used=fit_floor_used,
        required_coverage_floor_used=required_coverage_floor_used,
    )
    candidate_pool_size = len(eligible)
    if candidate_pool_size:
        rank_matches(eligible, ranking_context)

    selected_matches, beyond_top_k = _split_primary_matches(eligible, top_k_used)
    item_snapshots = _primary_item_snapshots(
        selected_matches,
        notification_fit_floor_used=notification_fit_floor_used,
    )
    excluded_candidates = _sorted_excluded_candidates(
        beyond_top_k,
        excluded_by_floor,
    )
    truncated_count = _append_excluded_item_snapshots(
        item_snapshots,
        excluded_candidates,
        notification_fit_floor_used=notification_fit_floor_used,
        two_tier_enabled=two_tier_enabled,
    )

    policy_snapshot = MatchSelectionPolicySnapshot.from_ranking_context(
        ranking_context=ranking_context,
        fit_floor_used=fit_floor_used,
        required_coverage_floor_used=required_coverage_floor_used,
        notification_fit_floor_used=notification_fit_floor_used,
        top_k_used=top_k_used,
        candidate_pool_size=candidate_pool_size,
        selected_count=len(selected_matches),
        alert_candidate_count=_alert_candidate_count(item_snapshots),
        resume_resolution_reason=resume_resolution_reason,
        task_id=task_id,
    )
    _record_truncation_count(
        policy_snapshot,
        truncated_count=truncated_count,
        two_tier_enabled=two_tier_enabled,
    )
    return MatchSelectionResult(
        selected_matches=selected_matches,
        item_snapshots=item_snapshots,
        policy_snapshot=policy_snapshot,
    )


def _partition_matches(
    matches: list[Any],
    *,
    fit_floor_used: float,
    required_coverage_floor_used: Optional[float],
) -> tuple[list[Any], list[tuple[Any, str]]]:
    eligible: list[Any] = []
    excluded_by_floor: list[tuple[Any, str]] = []
    for match in matches:
        reason = _excluded_reason_from_floors(
            match,
            fit_floor_used,
            required_coverage_floor_used,
        )
        if reason is None:
            eligible.append(match)
        else:
            excluded_by_floor.append((match, reason))
    return eligible, excluded_by_floor


def _split_primary_matches(
    eligible: list[Any],
    top_k_used: int,
) -> tuple[list[Any], list[Any]]:
    if top_k_used > 0:
        return eligible[:top_k_used], eligible[top_k_used:]
    return [], list(eligible)


def _primary_item_snapshots(
    selected_matches: list[Any],
    *,
    notification_fit_floor_used: float,
) -> list[MatchSelectionItemSnapshot]:
    snapshots = [
        _item_snapshot_from_match(
            match,
            rank_position=index,
            notification_fit_floor_used=notification_fit_floor_used,
            selection_tier="primary",
            excluded_reason=None,
        )
        for index, match in enumerate(selected_matches, start=1)
    ]
    for snapshot in snapshots:
        record_selection_tier_item(snapshot.selection_tier, snapshot.excluded_reason)
    return snapshots


def _sorted_excluded_candidates(
    beyond_top_k: list[Any],
    excluded_by_floor: list[tuple[Any, str]],
) -> list[tuple[Any, str, int]]:
    excluded_candidates: list[tuple[Any, str, int]] = [
        (match, "beyond_top_k", index) for index, match in enumerate(beyond_top_k)
    ]
    base_offset = len(excluded_candidates)
    excluded_candidates.extend(
        (match, reason, base_offset + index)
        for index, (match, reason) in enumerate(excluded_by_floor)
    )
    excluded_candidates.sort(key=lambda item: (-_score(item[0], "fit_score"), item[2]))
    return excluded_candidates


def _append_excluded_item_snapshots(
    item_snapshots: list[MatchSelectionItemSnapshot],
    excluded_candidates: list[tuple[Any, str, int]],
    *,
    notification_fit_floor_used: float,
    two_tier_enabled: bool,
) -> int:
    excluded_budget = max(0, EXCLUDED_STORAGE_CAP) if two_tier_enabled else 0
    next_rank = len(item_snapshots) + 1
    truncated_count = 0
    for match, reason, _original_order in excluded_candidates:
        if excluded_budget <= 0:
            if two_tier_enabled:
                truncated_count += 1
                record_selection_tier_item("excluded", "truncated")
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
        record_selection_tier_item("excluded", reason)
        next_rank += 1
        excluded_budget -= 1
    return truncated_count


def _alert_candidate_count(item_snapshots: list[MatchSelectionItemSnapshot]) -> int:
    return sum(
        1
        for item in item_snapshots
        if item.selection_tier == "primary" and item.alert_eligible
    )


def _record_truncation_count(
    policy_snapshot: MatchSelectionPolicySnapshot,
    *,
    truncated_count: int,
    two_tier_enabled: bool,
) -> None:
    if not two_tier_enabled or not truncated_count:
        return
    # Surface truncation in the policy snapshot via ranking_config_snapshot
    # piggyback. Auditable in the run row without a new column.
    policy_snapshot.ranking_config_snapshot["excluded_truncated_count"] = truncated_count


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
