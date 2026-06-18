#!/usr/bin/env python3
"""
Stats endpoints - view match statistics.
"""

from typing import Annotated
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from core.match_selection import resolve_canonical_resume_selection
from database.models import JobPost
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
        "beyond_top_k_count": 0,
        "qualifying_count": 0,
        "policy_top_k": None,
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
        "job_post_total": 0,
        "active_job_posts": 0,
        "inactive_job_posts": 0,
        "extracted_job_posts": 0,
        "embedded_job_posts": 0,
        "ready_to_score_job_posts": 0,
        "pending_extraction_job_posts": 0,
        "processing_extraction_job_posts": 0,
        "retryable_extraction_job_posts": 0,
        "failed_extraction_job_posts": 0,
        "pending_embedding_job_posts": 0,
        "processing_embedding_job_posts": 0,
        "retryable_embedding_job_posts": 0,
        "failed_embedding_job_posts": 0,
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


def _canonical_stats_payload(
    repo,
    owner_id: object | None,
    *,
    min_fit: float,
    top_k: int | None,
) -> dict[str, object]:
    stats = _empty_stats_payload()
    try:
        stats.update(_job_processing_stats(repo))
    except Exception:
        pass
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
    policy_counts = _policy_counts_from_items(items, min_fit=min_fit, top_k=top_k)
    preference_status = _preference_status_from_items(items)
    score_dist = _score_distribution_from_items(items)

    stats.update(
        {
            "primary_count": primary_count,
            "excluded_count": excluded_count,
            "total_scored": total_scored,
            "total_matches": total_scored,
            "hidden_count": policy_counts["hidden_count"],
            "active_matches": policy_counts["active_matches"],
            "below_threshold_count": policy_counts["below_threshold_count"],
            "beyond_top_k_count": policy_counts["beyond_top_k_count"],
            "qualifying_count": policy_counts["qualifying_count"],
            "policy_top_k": top_k,
            "excluded_by_reason": excluded_by_reason,
            "preference_status": preference_status,
            "score_distribution": score_dist,
        }
    )
    return stats

def _job_processing_stats(repo) -> dict[str, int]:
    row = repo.db.query(
        func.count(JobPost.id).label("job_post_total"),
        func.count(JobPost.id).filter(JobPost.status == "active").label("active_job_posts"),
        func.count(JobPost.id).filter(JobPost.status == "inactive").label("inactive_job_posts"),
        func.count(JobPost.id).filter(JobPost.is_extracted.is_(True)).label("extracted_job_posts"),
        func.count(JobPost.id).filter(JobPost.is_embedded.is_(True)).label("embedded_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.is_extracted.is_(True), JobPost.is_embedded.is_(True))
        .label("ready_to_score_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.extraction_status == "pending")
        .label("pending_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.extraction_status.in_(("in_progress", "processing")))
        .label("processing_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.extraction_status == "failed_retryable")
        .label("retryable_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.extraction_status.in_(("failed_terminal", "failed")))
        .label("failed_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.embedding_status == "pending")
        .label("pending_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.embedding_status.in_(("in_progress", "processing")))
        .label("processing_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.embedding_status == "failed_retryable")
        .label("retryable_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.embedding_status.in_(("failed_terminal", "failed")))
        .label("failed_embedding_job_posts"),
    ).one()
    return {
        key: int(getattr(row, key) or 0)
        for key in (
            "job_post_total",
            "active_job_posts",
            "inactive_job_posts",
            "extracted_job_posts",
            "embedded_job_posts",
            "ready_to_score_job_posts",
            "pending_extraction_job_posts",
            "processing_extraction_job_posts",
            "retryable_extraction_job_posts",
            "failed_extraction_job_posts",
            "pending_embedding_job_posts",
            "processing_embedding_job_posts",
            "retryable_embedding_job_posts",
            "failed_embedding_job_posts",
        )
    }


def _item_fit_score(item) -> float | None:
    value = getattr(item, "fit_score_at_selection", None)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _item_is_hidden_primary(item) -> bool:
    tier = getattr(item, "selection_tier", "primary") or "primary"
    return tier == "primary" and bool(getattr(item.job_match, "is_hidden", False))


def _policy_counts_from_items(
    items,
    *,
    min_fit: float,
    top_k: int | None,
) -> dict[str, int]:
    below_threshold_count = 0
    hidden_count = 0
    visible_qualifying_count = 0

    for item in items:
        fit_score = _item_fit_score(item)
        if fit_score is None or fit_score < min_fit:
            below_threshold_count += 1
            continue
        if _item_is_hidden_primary(item):
            hidden_count += 1
            continue
        visible_qualifying_count += 1

    active_matches = visible_qualifying_count
    if top_k is not None:
        active_matches = min(active_matches, max(int(top_k), 0))
    beyond_top_k_count = max(visible_qualifying_count - active_matches, 0)

    return {
        "active_matches": active_matches,
        "hidden_count": hidden_count,
        "below_threshold_count": below_threshold_count,
        "beyond_top_k_count": beyond_top_k_count,
        "qualifying_count": visible_qualifying_count + hidden_count,
    }


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
    min_fit: Annotated[
        float | None,
        Query(ge=0, le=100, description="Minimum fit score to use for live dashboard buckets"),
    ] = None,
    top_k: Annotated[
        int | None,
        Query(ge=1, le=500, description="Maximum visible shortlist size to use for live dashboard buckets"),
    ] = None,
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
    effective_min_fit = current_policy.min_fit if min_fit is None else min_fit
    effective_top_k = current_policy.top_k if top_k is None else top_k

    owner_id = getattr(user, "id", None)
    stats = _empty_stats_payload()

    try:
        with job_uow() as repo:
            stats = _canonical_stats_payload(
                repo,
                owner_id,
                min_fit=effective_min_fit,
                top_k=effective_top_k,
            )
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
            'beyond_top_k_count': stats['beyond_top_k_count'],
            'qualifying_count': stats['qualifying_count'],
            'policy_top_k': effective_top_k,
            'min_fit_threshold': effective_min_fit,
            'score_distribution': stats['score_distribution'],
            # Canonical selection-run scoped counts.
            'total_scored': stats['total_scored'],
            'primary_count': stats['primary_count'],
            'excluded_count': stats['excluded_count'],
            'excluded_by_reason': stats['excluded_by_reason'],
            'preference_status': stats['preference_status'],
            'job_post_total': stats['job_post_total'],
            'active_job_posts': stats['active_job_posts'],
            'inactive_job_posts': stats['inactive_job_posts'],
            'extracted_job_posts': stats['extracted_job_posts'],
            'embedded_job_posts': stats['embedded_job_posts'],
            'ready_to_score_job_posts': stats['ready_to_score_job_posts'],
            'pending_extraction_job_posts': stats['pending_extraction_job_posts'],
            'processing_extraction_job_posts': stats['processing_extraction_job_posts'],
            'retryable_extraction_job_posts': stats['retryable_extraction_job_posts'],
            'failed_extraction_job_posts': stats['failed_extraction_job_posts'],
            'pending_embedding_job_posts': stats['pending_embedding_job_posts'],
            'processing_embedding_job_posts': stats['processing_embedding_job_posts'],
            'retryable_embedding_job_posts': stats['retryable_embedding_job_posts'],
            'failed_embedding_job_posts': stats['failed_embedding_job_posts'],
        }
    )
