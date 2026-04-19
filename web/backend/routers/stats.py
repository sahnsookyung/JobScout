#!/usr/bin/env python3
"""
Stats endpoints - view match statistics.
"""

from typing import Annotated
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.match_selection import resolve_canonical_resume_selection
from database.models import JobMatch
from database.uow import job_uow

from ..dependencies import get_current_user, get_db
from ..models.responses import StatsResponse
from ..services.policy_service import get_policy_service

router = APIRouter(prefix="/api/stats", tags=["stats"])


def _empty_stats_payload() -> dict[str, object]:
    return {
        "total_scored": 0,
        "total_matches": 0,
        "active_matches": 0,
        "hidden_count": 0,
        "below_threshold_count": 0,
        "score_distribution": {
            "excellent": 0,
            "good": 0,
            "average": 0,
            "poor": 0,
        },
        "primary_count": 0,
        "excluded_count": 0,
        "excluded_by_reason": {},
        "preference_status": None,
    }


def _accumulate_score_bucket(score_dist: dict[str, int], fit_score: float | None) -> None:
    if fit_score is None:
        return
    if fit_score >= 80:
        score_dist["excellent"] += 1
        return
    if fit_score >= 60:
        score_dist["good"] += 1
        return
    if fit_score >= 40:
        score_dist["average"] += 1
        return
    score_dist["poor"] += 1


def _canonical_stats_payload(repo, owner_id: object | None) -> dict[str, object]:
    stats = _empty_stats_payload()
    canonical = resolve_canonical_resume_selection(repo, owner_id)
    if canonical is None:
        return stats

    tier_counts = repo.match_selection.count_items_for_run_by_tier(
        canonical.selection_run_id
    )
    excluded_by_reason = repo.match_selection.count_excluded_items_by_reason(
        canonical.selection_run_id
    )
    items = repo.match_selection.get_items_for_run(
        canonical.selection_run_id,
        tier="all",
    )

    primary_count = int(tier_counts.get("primary", 0))
    excluded_count = int(tier_counts.get("excluded", 0))
    total_scored = primary_count + excluded_count
    hidden_count = _hidden_primary_count(items)
    preference_status = _preference_status_from_items(items)
    score_dist = _score_distribution_from_items(items)

    stats.update(
        {
            "primary_count": primary_count,
            "excluded_count": excluded_count,
            "total_scored": total_scored,
            "total_matches": total_scored,
            "hidden_count": hidden_count,
            "active_matches": max(primary_count - hidden_count, 0),
            "below_threshold_count": int(excluded_by_reason.get("below_min_fit", 0)),
            "excluded_by_reason": excluded_by_reason,
            "preference_status": preference_status,
            "score_distribution": score_dist,
        }
    )
    return stats


def _hidden_primary_count(items) -> int:
    return sum(
        1
        for item in items
        if (getattr(item, "selection_tier", "primary") or "primary") == "primary"
        and bool(getattr(item.job_match, "is_hidden", False))
    )


def _preference_status_from_items(items) -> dict | None:
    for item in items:
        ranking_snapshot = getattr(item.job_match, "ranking_snapshot", None)
        if not isinstance(ranking_snapshot, dict):
            continue
        status = ranking_snapshot.get("preference_status")
        if isinstance(status, dict):
            return status
    return None


def _score_distribution_from_items(items) -> dict[str, int]:
    score_dist = {
        "excellent": 0,
        "good": 0,
        "average": 0,
        "poor": 0,
    }
    for item in items:
        _accumulate_score_bucket(
            score_dist,
            getattr(item, "fit_score_at_selection", None),
        )
    return score_dist

@router.get("", response_model=StatsResponse)
def get_stats(
    db: Annotated[Session, Depends(get_db)],
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Get statistics about matches for the user's canonical selection run.

    Reports totals and tier counts scoped to the latest canonical selection run.
    Legacy dashboard fields remain for backwards-compat, but are also derived
    from that same canonical run so the payload does not mix scopes.
    """
    del db
    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()
    min_fit = current_policy.min_fit

    owner_id = getattr(user, "id", None)
    stats = _empty_stats_payload()

    try:
        with job_uow() as repo:
            stats = _canonical_stats_payload(repo, owner_id)
    except Exception:
        # Stats should never fail the page; canonical-run fields fall back to 0.
        pass

    return StatsResponse(
        success=True,
        stats={
            'total_matches': stats['total_matches'],
            'active_matches': stats['active_matches'],
            'hidden_count': stats['hidden_count'],
            'below_threshold_count': stats['below_threshold_count'],
            'min_fit_threshold': min_fit,
            'score_distribution': stats['score_distribution'],
            # Canonical selection-run scoped counts.
            'total_scored': stats['total_scored'],
            'primary_count': stats['primary_count'],
            'excluded_count': stats['excluded_count'],
            'excluded_by_reason': stats['excluded_by_reason'],
            'preference_status': stats['preference_status'],
        }
    )
