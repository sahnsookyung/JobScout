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
    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()
    min_fit = current_policy.min_fit

    owner_id = getattr(user, "id", None)

    total_scored = 0
    total_matches = 0
    active_matches = 0
    hidden_count = 0
    below_threshold_count = 0
    score_dist = {
        'excellent': 0,
        'good': 0,
        'average': 0,
        'poor': 0,
    }
    primary_count = 0
    excluded_count = 0
    excluded_by_reason: dict[str, int] = {}
    preference_status = None

    try:
        with job_uow() as repo:
            canonical = resolve_canonical_resume_selection(repo, owner_id)
            if canonical is not None:
                tier_counts = repo.match_selection.count_items_for_run_by_tier(
                    canonical.selection_run_id
                )
                primary_count = int(tier_counts.get("primary", 0))
                excluded_count = int(tier_counts.get("excluded", 0))
                total_scored = primary_count + excluded_count
                excluded_by_reason = repo.match_selection.count_excluded_items_by_reason(
                    canonical.selection_run_id
                )
                items = repo.match_selection.get_items_for_run(
                    canonical.selection_run_id,
                    tier="all",
                )
                total_matches = total_scored
                below_threshold_count = int(excluded_by_reason.get("below_min_fit", 0))
                for item in items:
                    tier = getattr(item, "selection_tier", "primary") or "primary"
                    fit_score = getattr(item, "fit_score_at_selection", None)
                    is_hidden = bool(getattr(item.job_match, "is_hidden", False))
                    if tier == "primary" and is_hidden:
                        hidden_count += 1
                    ranking_snapshot = getattr(item.job_match, "ranking_snapshot", None)
                    if isinstance(ranking_snapshot, dict):
                        status = ranking_snapshot.get("preference_status")
                        if isinstance(status, dict):
                            preference_status = status
                    if fit_score is None:
                        continue
                    if fit_score >= 80:
                        score_dist['excellent'] += 1
                    elif fit_score >= 60:
                        score_dist['good'] += 1
                    elif fit_score >= 40:
                        score_dist['average'] += 1
                    else:
                        score_dist['poor'] += 1
                active_matches = max(primary_count - hidden_count, 0)
    except Exception:
        # Stats should never fail the page; canonical-run fields fall back to 0.
        pass

    return StatsResponse(
        success=True,
        stats={
            'total_matches': total_matches,
            'active_matches': active_matches,
            'hidden_count': hidden_count,
            'below_threshold_count': below_threshold_count,
            'min_fit_threshold': min_fit,
            'score_distribution': score_dist,
            # Canonical selection-run scoped counts.
            'total_scored': total_scored,
            'primary_count': primary_count,
            'excluded_count': excluded_count,
            'excluded_by_reason': excluded_by_reason,
            'preference_status': preference_status,
        }
    )
