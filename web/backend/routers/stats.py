#!/usr/bin/env python3
"""
Stats endpoints - view match statistics.
"""

from typing import Annotated
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.match_selection import resolve_canonical_resume_selection
from database.models import JobMatch, StructuredResume
from database.uow import job_uow

from ..dependencies import get_current_user, get_db
from ..models.responses import StatsResponse
from ..services.policy_service import get_policy_service

router = APIRouter(prefix="/api/stats", tags=["stats"])


def _owner_scoped_match_query(db: Session, owner_id: object | None):
    query = db.query(JobMatch)
    if owner_id is None:
        return query
    return query.join(
        StructuredResume,
        StructuredResume.resume_fingerprint == JobMatch.resume_fingerprint,
    ).filter(StructuredResume.owner_id == owner_id)


@router.get("", response_model=StatsResponse)
def get_stats(
    db: Annotated[Session, Depends(get_db)],
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Get statistics about matches for the user's canonical selection run.

    Reports totals and tier counts scoped to the latest selection run, so
    `total_scored` reconciles with `GET /api/matches?tier=all` and
    `primary_count` matches `?tier=primary`. Legacy fields remain for
    backwards-compat with existing UI code.
    """
    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()
    min_fit = current_policy.min_fit

    owner_id = getattr(user, "id", None)

    # Canonical-run scoped tier counts (primary source of truth).
    total_scored = 0
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
                for item in items:
                    ranking_snapshot = getattr(item.job_match, "ranking_snapshot", None)
                    if isinstance(ranking_snapshot, dict):
                        status = ranking_snapshot.get("preference_status")
                        if isinstance(status, dict):
                            preference_status = status
                            break
    except Exception:
        # Stats should never fail the page; fall back to DB-wide numbers.
        pass

    # Legacy dashboard fields stay available, but they must remain user-scoped.
    base_query = _owner_scoped_match_query(db, owner_id)
    total_matches = base_query.count()
    hidden_count = base_query.filter(JobMatch.is_hidden.is_(True)).count()
    below_threshold_count = base_query.filter(
        (JobMatch.fit_score < min_fit) | (JobMatch.fit_score.is_(None)),
        JobMatch.is_hidden.is_(False)
    ).count()
    active_matches = total_matches - hidden_count - below_threshold_count

    score_dist = {
        'excellent': base_query.filter(JobMatch.fit_score >= 80).count(),
        'good': base_query.filter(
            JobMatch.fit_score >= 60,
            JobMatch.fit_score < 80
        ).count(),
        'average': base_query.filter(
            JobMatch.fit_score >= 40,
            JobMatch.fit_score < 60
        ).count(),
        'poor': base_query.filter(JobMatch.fit_score < 40).count(),
    }

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
