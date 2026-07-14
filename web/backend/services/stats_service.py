"""
Stats aggregation use cases for dashboard and match summary payloads.
"""

import logging
from datetime import datetime, timezone
from typing import Callable

from sqlalchemy import and_, func, or_

from core.match_selection import resolve_canonical_resume_selection
from core.metrics import (
    set_description_recovery_oldest_missing_age_seconds,
    set_job_inventory_metrics,
)
from database.models import JobMatch, JobPost, MatchSelectionItem
from database.uow import job_uow
from web.backend.models.responses import StatsResponse
from web.backend.services.policy_service import get_policy_service

logger = logging.getLogger(__name__)


def _degraded_reason(code: str, exc: Exception) -> dict[str, str]:
    return {"code": code, "detail": exc.__class__.__name__}


def _append_degraded_reason(
    reasons: list[dict[str, str]] | None,
    *,
    code: str,
    exc: Exception,
) -> None:
    if reasons is not None:
        reasons.append(_degraded_reason(code, exc))
    logger.warning("Stats aggregation degraded: %s", code, exc_info=True)


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
        "expired_job_posts": 0,
        "extracted_job_posts": 0,
        "embedded_job_posts": 0,
        "ready_to_score_job_posts": 0,
        "active_extracted_job_posts": 0,
        "active_embedded_job_posts": 0,
        "active_ready_to_score_job_posts": 0,
        "pending_extraction_job_posts": 0,
        "processing_extraction_job_posts": 0,
        "retryable_extraction_job_posts": 0,
        "failed_extraction_job_posts": 0,
        "active_pending_extraction_job_posts": 0,
        "active_retryable_extraction_job_posts": 0,
        "inactive_pending_extraction_job_posts": 0,
        "ready_for_extraction_job_posts": 0,
        "active_ready_for_extraction_job_posts": 0,
        "pending_embedding_job_posts": 0,
        "processing_embedding_job_posts": 0,
        "retryable_embedding_job_posts": 0,
        "failed_embedding_job_posts": 0,
        "active_pending_embedding_job_posts": 0,
        "active_retryable_embedding_job_posts": 0,
        "inactive_pending_embedding_job_posts": 0,
        "missing_description_job_posts": 0,
        "active_missing_description_job_posts": 0,
        "inactive_missing_description_job_posts": 0,
        "description_recovery_queued_job_posts": 0,
        "description_recovery_retryable_job_posts": 0,
        "active_recoverable_missing_description_job_posts": 0,
        "description_recovery_posting_not_found_job_posts": 0,
        "description_recovery_adapter_missing_job_posts": 0,
        "description_recovery_prohibited_job_posts": 0,
        "description_recovery_unmapped_job_posts": 0,
        "description_recovery_unavailable_job_posts": 0,
        "oldest_missing_description_age_seconds": 0,
    }


def _canonical_stats_payload(
    repo,
    owner_id: object | None,
    *,
    min_fit: float,
    top_k: int | None,
    tenant_id: object | None,
    degraded_reasons: list[dict[str, str]] | None = None,
    canonical_resolver: Callable = resolve_canonical_resume_selection,
) -> dict[str, object]:
    stats = _empty_stats_payload()
    try:
        job_stats = _job_processing_stats(repo, tenant_id=tenant_id)
        stats.update(job_stats)
        set_job_inventory_metrics(job_stats)
    except Exception as exc:
        _append_degraded_reason(
            degraded_reasons,
            code="job_processing_stats_unavailable",
            exc=exc,
        )

    try:
        canonical = canonical_resolver(repo, owner_id, tenant_id=tenant_id)
    except Exception as exc:
        _append_degraded_reason(
            degraded_reasons,
            code="canonical_selection_unavailable",
            exc=exc,
        )
        return stats

    if canonical is None:
        return stats

    try:
        tier_counts = _count_items_for_run_by_tier(
            repo,
            canonical.selection_run_id,
            tenant_id=tenant_id,
        )
    except Exception as exc:
        _append_degraded_reason(
            degraded_reasons,
            code="tenant_scoped_tier_counts_unavailable",
            exc=exc,
        )
        try:
            tier_counts = repo.match_selection.count_items_for_run_by_tier(
                canonical.selection_run_id
            )
        except Exception as fallback_exc:
            _append_degraded_reason(
                degraded_reasons,
                code="tier_counts_unavailable",
                exc=fallback_exc,
            )
            tier_counts = {}

    try:
        excluded_by_reason = _count_excluded_items_by_reason(
            repo,
            canonical.selection_run_id,
            tenant_id=tenant_id,
        )
    except Exception as exc:
        _append_degraded_reason(
            degraded_reasons,
            code="tenant_scoped_excluded_reason_counts_unavailable",
            exc=exc,
        )
        try:
            excluded_by_reason = repo.match_selection.count_excluded_items_by_reason(
                canonical.selection_run_id
            )
        except Exception as fallback_exc:
            _append_degraded_reason(
                degraded_reasons,
                code="excluded_reason_counts_unavailable",
                exc=fallback_exc,
            )
            excluded_by_reason = {}

    try:
        item_stats = _selection_run_item_stats(
            repo,
            canonical.selection_run_id,
            min_fit=min_fit,
            top_k=top_k,
            tenant_id=tenant_id,
        )
    except Exception as exc:
        _append_degraded_reason(
            degraded_reasons,
            code="tenant_scoped_selection_item_stats_unavailable",
            exc=exc,
        )
        try:
            items = repo.match_selection.get_items_for_run(
                canonical.selection_run_id,
                tier="all",
                tenant_id=tenant_id,
            )
            item_stats = _selection_run_item_stats_from_items(
                items,
                min_fit=min_fit,
                top_k=top_k,
            )
        except Exception as fallback_exc:
            _append_degraded_reason(
                degraded_reasons,
                code="selection_item_stats_unavailable",
                exc=fallback_exc,
            )
            item_stats = _selection_run_item_stats_from_items(
                [],
                min_fit=min_fit,
                top_k=top_k,
            )

    primary_count = int(tier_counts.get("primary", 0))
    excluded_count = int(tier_counts.get("excluded", 0))
    total_scored = primary_count + excluded_count

    stats.update(
        {
            "primary_count": primary_count,
            "excluded_count": excluded_count,
            "total_scored": total_scored,
            "total_matches": total_scored,
            "hidden_count": item_stats["hidden_count"],
            "active_matches": item_stats["active_matches"],
            "below_threshold_count": item_stats["below_threshold_count"],
            "beyond_top_k_count": item_stats["beyond_top_k_count"],
            "qualifying_count": item_stats["qualifying_count"],
            "policy_top_k": top_k,
            "excluded_by_reason": excluded_by_reason,
            "preference_status": item_stats["preference_status"],
            "score_distribution": item_stats["score_distribution"],
        }
    )
    return stats


def _job_processing_stats(repo, tenant_id=None) -> dict[str, int]:
    active_job = JobPost.status == "active"
    inactive_job = JobPost.status != "active"
    pending_extraction = JobPost.extraction_status == "pending"
    retryable_extraction = JobPost.extraction_status == "failed_retryable"
    pending_embedding = JobPost.embedding_status == "pending"
    retryable_embedding = JobPost.embedding_status == "failed_retryable"
    missing_description = or_(
        JobPost.description.is_(None),
        func.length(func.trim(JobPost.description)) == 0,
        JobPost.description_completeness == "missing",
        JobPost.extraction_status == "no_description",
    )
    description_available = ~missing_description
    ready_for_extraction = and_(
        JobPost.is_extracted.is_(False),
        JobPost.extraction_status.in_(("pending", "queued")),
        description_available,
    )
    query = repo.db.query(
        func.count(JobPost.id).label("job_post_total"),
        func.count(JobPost.id).filter(active_job).label("active_job_posts"),
        func.count(JobPost.id).filter(JobPost.status == "inactive").label("inactive_job_posts"),
        func.count(JobPost.id).filter(JobPost.status == "expired").label("expired_job_posts"),
        func.count(JobPost.id).filter(JobPost.is_extracted.is_(True)).label("extracted_job_posts"),
        func.count(JobPost.id).filter(JobPost.is_embedded.is_(True)).label("embedded_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.is_extracted.is_(True), JobPost.is_embedded.is_(True))
        .label("ready_to_score_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, JobPost.is_extracted.is_(True))
        .label("active_extracted_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, JobPost.is_embedded.is_(True))
        .label("active_embedded_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, JobPost.is_extracted.is_(True), JobPost.is_embedded.is_(True))
        .label("active_ready_to_score_job_posts"),
        func.count(JobPost.id)
        .filter(pending_extraction, description_available)
        .label("pending_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.extraction_status.in_(("in_progress", "processing")))
        .label("processing_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(retryable_extraction, description_available)
        .label("retryable_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.extraction_status.in_(("failed_terminal", "failed")))
        .label("failed_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, pending_extraction, description_available)
        .label("active_pending_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, retryable_extraction, description_available)
        .label("active_retryable_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(
            inactive_job,
            JobPost.is_extracted.is_(False),
            JobPost.extraction_status.in_(("pending", "failed_retryable")),
            description_available,
        )
        .label("inactive_pending_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(ready_for_extraction)
        .label("ready_for_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, ready_for_extraction)
        .label("active_ready_for_extraction_job_posts"),
        func.count(JobPost.id)
        .filter(pending_embedding)
        .label("pending_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.embedding_status.in_(("in_progress", "processing")))
        .label("processing_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(retryable_embedding)
        .label("retryable_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.embedding_status.in_(("failed_terminal", "failed")))
        .label("failed_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, pending_embedding)
        .label("active_pending_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, retryable_embedding)
        .label("active_retryable_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(inactive_job, JobPost.is_embedded.is_(False), JobPost.embedding_status.in_(("pending", "failed_retryable")))
        .label("inactive_pending_embedding_job_posts"),
        func.count(JobPost.id)
        .filter(missing_description)
        .label("missing_description_job_posts"),
        func.count(JobPost.id)
        .filter(active_job, missing_description)
        .label("active_missing_description_job_posts"),
        func.count(JobPost.id)
        .filter(inactive_job, missing_description)
        .label("inactive_missing_description_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.description_recovery_status.in_(("queued", "refreshing")))
        .label("description_recovery_queued_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.description_recovery_status == "failed_retryable")
        .label("description_recovery_retryable_job_posts"),
        func.count(JobPost.id)
        .filter(
            active_job,
            missing_description,
            JobPost.description_recovery_status.in_(("not_needed", "pending", "failed_retryable")),
        )
        .label("active_recoverable_missing_description_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.description_recovery_status == "posting_not_found")
        .label("description_recovery_posting_not_found_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.description_recovery_status == "source_adapter_missing")
        .label("description_recovery_adapter_missing_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.description_recovery_status == "source_prohibited")
        .label("description_recovery_prohibited_job_posts"),
        func.count(JobPost.id)
        .filter(JobPost.description_recovery_status == "source_unmapped")
        .label("description_recovery_unmapped_job_posts"),
        func.count(JobPost.id)
        .filter(
            JobPost.description_recovery_status.in_(
                (
                    "source_unsupported",
                    "source_prohibited",
                    "source_unmapped",
                    "source_adapter_missing",
                )
            )
        )
        .label("description_recovery_unavailable_job_posts"),
        func.min(JobPost.first_seen_at)
        .filter(active_job, missing_description)
        .label("oldest_missing_description_first_seen_at"),
    )
    if tenant_id is not None:
        query = query.filter(JobPost.tenant_id == tenant_id)
    row = query.one()
    oldest_missing = getattr(row, "oldest_missing_description_first_seen_at", None)
    oldest_age_seconds = 0
    if oldest_missing is not None:
        if getattr(oldest_missing, "tzinfo", None) is None:
            oldest_missing = oldest_missing.replace(tzinfo=timezone.utc)
        oldest_age_seconds = max(
            int((datetime.now(timezone.utc) - oldest_missing).total_seconds()),
            0,
        )
    set_description_recovery_oldest_missing_age_seconds(oldest_age_seconds)
    stats = {
        key: int(getattr(row, key, 0) or 0)
        for key in (
            "job_post_total",
            "active_job_posts",
            "inactive_job_posts",
            "expired_job_posts",
            "extracted_job_posts",
            "embedded_job_posts",
            "ready_to_score_job_posts",
            "active_extracted_job_posts",
            "active_embedded_job_posts",
            "active_ready_to_score_job_posts",
            "pending_extraction_job_posts",
            "processing_extraction_job_posts",
            "retryable_extraction_job_posts",
            "failed_extraction_job_posts",
            "active_pending_extraction_job_posts",
            "active_retryable_extraction_job_posts",
            "inactive_pending_extraction_job_posts",
            "ready_for_extraction_job_posts",
            "active_ready_for_extraction_job_posts",
            "pending_embedding_job_posts",
            "processing_embedding_job_posts",
            "retryable_embedding_job_posts",
            "failed_embedding_job_posts",
            "active_pending_embedding_job_posts",
            "active_retryable_embedding_job_posts",
            "inactive_pending_embedding_job_posts",
            "missing_description_job_posts",
            "active_missing_description_job_posts",
            "inactive_missing_description_job_posts",
            "description_recovery_queued_job_posts",
            "description_recovery_retryable_job_posts",
            "active_recoverable_missing_description_job_posts",
            "description_recovery_posting_not_found_job_posts",
            "description_recovery_adapter_missing_job_posts",
            "description_recovery_prohibited_job_posts",
            "description_recovery_unmapped_job_posts",
            "description_recovery_unavailable_job_posts",
        )
    }
    stats["oldest_missing_description_age_seconds"] = oldest_age_seconds
    return stats


def _count_items_for_run_by_tier(
    repo,
    selection_run_id,
    *,
    tenant_id,
) -> dict[str, int]:
    query = (
        repo.db.query(MatchSelectionItem.selection_tier, func.count(MatchSelectionItem.id))
        .join(JobMatch, JobMatch.id == MatchSelectionItem.job_match_id)
        .filter(MatchSelectionItem.selection_run_id == selection_run_id)
    )
    if tenant_id is not None:
        query = query.join(JobPost, JobPost.id == JobMatch.job_post_id).filter(
            JobPost.tenant_id == tenant_id
        )
    query = query.group_by(MatchSelectionItem.selection_tier)
    return {
        (tier or "primary"): int(count or 0)
        for tier, count in query.all()
    }


def _count_excluded_items_by_reason(
    repo,
    selection_run_id,
    *,
    tenant_id,
) -> dict[str, int]:
    query = (
        repo.db.query(MatchSelectionItem.excluded_reason, func.count(MatchSelectionItem.id))
        .join(JobMatch, JobMatch.id == MatchSelectionItem.job_match_id)
        .filter(
            MatchSelectionItem.selection_run_id == selection_run_id,
            MatchSelectionItem.selection_tier == "excluded",
        )
    )
    if tenant_id is not None:
        query = query.join(JobPost, JobPost.id == JobMatch.job_post_id).filter(
            JobPost.tenant_id == tenant_id
        )
    query = query.group_by(MatchSelectionItem.excluded_reason)
    return {
        (reason or "unknown"): int(count or 0)
        for reason, count in query.all()
    }


def _preference_status_for_selection_run(repo, selection_run_id, *, tenant_id) -> dict | None:
    query = (
        repo.db.query(JobMatch.ranking_snapshot)
        .join(MatchSelectionItem, MatchSelectionItem.job_match_id == JobMatch.id)
        .filter(
            MatchSelectionItem.selection_run_id == selection_run_id,
            JobMatch.ranking_snapshot.isnot(None),
        )
        .limit(50)
    )
    if tenant_id is not None:
        query = query.join(JobPost, JobPost.id == JobMatch.job_post_id).filter(
            JobPost.tenant_id == tenant_id
        )
    for (ranking_snapshot,) in query.all():
        if not isinstance(ranking_snapshot, dict):
            continue
        status = ranking_snapshot.get("preference_status")
        if isinstance(status, dict):
            return status
    return None


def _selection_run_item_stats(
    repo,
    selection_run_id,
    *,
    min_fit: float,
    top_k: int | None,
    tenant_id,
) -> dict[str, object]:
    fit_score = MatchSelectionItem.fit_score_at_selection
    tier = func.coalesce(MatchSelectionItem.selection_tier, "primary")
    qualifying = and_(fit_score.isnot(None), fit_score >= min_fit)
    below_threshold = or_(fit_score.is_(None), fit_score < min_fit)
    hidden_primary = and_(
        qualifying,
        tier == "primary",
        JobMatch.is_hidden.is_(True),
    )
    visible_qualifying = and_(
        qualifying,
        or_(tier != "primary", JobMatch.is_hidden.is_(False), JobMatch.is_hidden.is_(None)),
    )

    query = (
        repo.db.query(
            func.count(MatchSelectionItem.id)
            .filter(below_threshold)
            .label("below_threshold_count"),
            func.count(MatchSelectionItem.id)
            .filter(hidden_primary)
            .label("hidden_count"),
            func.count(MatchSelectionItem.id)
            .filter(visible_qualifying)
            .label("visible_qualifying_count"),
            func.count(MatchSelectionItem.id)
            .filter(fit_score >= 80)
            .label("excellent_count"),
            func.count(MatchSelectionItem.id)
            .filter(and_(fit_score >= 60, fit_score < 80))
            .label("good_count"),
            func.count(MatchSelectionItem.id)
            .filter(and_(fit_score >= 40, fit_score < 60))
            .label("average_count"),
            func.count(MatchSelectionItem.id)
            .filter(and_(fit_score.isnot(None), fit_score < 40))
            .label("poor_count"),
        )
        .select_from(MatchSelectionItem)
        .join(JobMatch, JobMatch.id == MatchSelectionItem.job_match_id)
        .filter(MatchSelectionItem.selection_run_id == selection_run_id)
    )
    if tenant_id is not None:
        query = query.join(JobPost, JobPost.id == JobMatch.job_post_id).filter(
            JobPost.tenant_id == tenant_id
        )
    row = query.one()
    visible_qualifying_count = int(getattr(row, "visible_qualifying_count") or 0)
    active_matches = visible_qualifying_count
    if top_k is not None:
        active_matches = min(active_matches, max(int(top_k), 0))
    return {
        "below_threshold_count": int(getattr(row, "below_threshold_count") or 0),
        "hidden_count": int(getattr(row, "hidden_count") or 0),
        "active_matches": active_matches,
        "beyond_top_k_count": max(visible_qualifying_count - active_matches, 0),
        "qualifying_count": visible_qualifying_count + int(getattr(row, "hidden_count") or 0),
        "preference_status": _preference_status_for_selection_run(
            repo,
            selection_run_id,
            tenant_id=tenant_id,
        ),
        "score_distribution": {
            "excellent": int(getattr(row, "excellent_count") or 0),
            "good": int(getattr(row, "good_count") or 0),
            "average": int(getattr(row, "average_count") or 0),
            "poor": int(getattr(row, "poor_count") or 0),
        },
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


def _selection_run_item_stats_from_items(
    items,
    *,
    min_fit: float,
    top_k: int | None,
) -> dict[str, object]:
    below_threshold_count = 0
    hidden_count = 0
    visible_qualifying_count = 0
    preference_status = None
    score_dist = {
        "excellent": 0,
        "good": 0,
        "average": 0,
        "poor": 0,
    }

    for item in items:
        fit_score = _item_fit_score(item)
        if fit_score is not None:
            if fit_score >= 80:
                score_dist["excellent"] += 1
            elif fit_score >= 60:
                score_dist["good"] += 1
            elif fit_score >= 40:
                score_dist["average"] += 1
            else:
                score_dist["poor"] += 1

        if preference_status is None:
            ranking_snapshot = getattr(item.job_match, "ranking_snapshot", None)
            if isinstance(ranking_snapshot, dict) and isinstance(
                ranking_snapshot.get("preference_status"),
                dict,
            ):
                preference_status = ranking_snapshot["preference_status"]

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

    return {
        "below_threshold_count": below_threshold_count,
        "hidden_count": hidden_count,
        "active_matches": active_matches,
        "beyond_top_k_count": max(visible_qualifying_count - active_matches, 0),
        "qualifying_count": visible_qualifying_count + hidden_count,
        "preference_status": preference_status,
        "score_distribution": score_dist,
    }


def build_stats_response(
    *,
    user: object,
    tenant_id: object | None,
    min_fit: float | None,
    top_k: int | None,
    policy_service_factory: Callable = get_policy_service,
    job_uow_factory: Callable = job_uow,
    canonical_resolver: Callable = resolve_canonical_resume_selection,
) -> StatsResponse:
    """Build tenant-aware canonical summary stats for dashboard and matches summary."""
    policy_service = policy_service_factory()
    current_policy = policy_service.get_current_policy(getattr(user, "id", None))
    effective_min_fit = current_policy.min_fit if min_fit is None else min_fit
    effective_top_k = current_policy.top_k if top_k is None else top_k

    owner_id = getattr(user, "id", None)
    stats = _empty_stats_payload()
    degraded_reasons: list[dict[str, str]] = []

    try:
        with job_uow_factory() as repo:
            stats = _canonical_stats_payload(
                repo,
                owner_id,
                min_fit=effective_min_fit,
                top_k=effective_top_k,
                tenant_id=tenant_id,
                degraded_reasons=degraded_reasons,
                canonical_resolver=canonical_resolver,
            )
    except Exception as exc:
        _append_degraded_reason(
            degraded_reasons,
            code="stats_unavailable",
            exc=exc,
        )

    return StatsResponse(
        success=True,
        stats={
            "total_matches": stats["total_matches"],
            "active_matches": stats["active_matches"],
            "hidden_count": stats["hidden_count"],
            "below_threshold_count": stats["below_threshold_count"],
            "beyond_top_k_count": stats["beyond_top_k_count"],
            "qualifying_count": stats["qualifying_count"],
            "policy_top_k": effective_top_k,
            "min_fit_threshold": effective_min_fit,
            "score_distribution": stats["score_distribution"],
            "total_scored": stats["total_scored"],
            "primary_count": stats["primary_count"],
            "excluded_count": stats["excluded_count"],
            "excluded_by_reason": stats["excluded_by_reason"],
            "preference_status": stats["preference_status"],
            "job_post_total": stats["job_post_total"],
            "active_job_posts": stats["active_job_posts"],
            "inactive_job_posts": stats["inactive_job_posts"],
            "expired_job_posts": stats["expired_job_posts"],
            "extracted_job_posts": stats["extracted_job_posts"],
            "embedded_job_posts": stats["embedded_job_posts"],
            "ready_to_score_job_posts": stats["ready_to_score_job_posts"],
            "active_extracted_job_posts": stats["active_extracted_job_posts"],
            "active_embedded_job_posts": stats["active_embedded_job_posts"],
            "active_ready_to_score_job_posts": stats["active_ready_to_score_job_posts"],
            "pending_extraction_job_posts": stats["pending_extraction_job_posts"],
            "processing_extraction_job_posts": stats["processing_extraction_job_posts"],
            "retryable_extraction_job_posts": stats["retryable_extraction_job_posts"],
            "failed_extraction_job_posts": stats["failed_extraction_job_posts"],
            "active_pending_extraction_job_posts": stats["active_pending_extraction_job_posts"],
            "active_retryable_extraction_job_posts": stats["active_retryable_extraction_job_posts"],
            "inactive_pending_extraction_job_posts": stats["inactive_pending_extraction_job_posts"],
            "ready_for_extraction_job_posts": stats["ready_for_extraction_job_posts"],
            "active_ready_for_extraction_job_posts": stats["active_ready_for_extraction_job_posts"],
            "pending_embedding_job_posts": stats["pending_embedding_job_posts"],
            "processing_embedding_job_posts": stats["processing_embedding_job_posts"],
            "retryable_embedding_job_posts": stats["retryable_embedding_job_posts"],
            "failed_embedding_job_posts": stats["failed_embedding_job_posts"],
            "active_pending_embedding_job_posts": stats["active_pending_embedding_job_posts"],
            "active_retryable_embedding_job_posts": stats["active_retryable_embedding_job_posts"],
            "inactive_pending_embedding_job_posts": stats["inactive_pending_embedding_job_posts"],
            "missing_description_job_posts": stats["missing_description_job_posts"],
            "active_missing_description_job_posts": stats["active_missing_description_job_posts"],
            "inactive_missing_description_job_posts": stats["inactive_missing_description_job_posts"],
            "description_recovery_queued_job_posts": stats["description_recovery_queued_job_posts"],
            "description_recovery_retryable_job_posts": stats["description_recovery_retryable_job_posts"],
            "active_recoverable_missing_description_job_posts": stats[
                "active_recoverable_missing_description_job_posts"
            ],
            "description_recovery_posting_not_found_job_posts": stats[
                "description_recovery_posting_not_found_job_posts"
            ],
            "description_recovery_adapter_missing_job_posts": stats[
                "description_recovery_adapter_missing_job_posts"
            ],
            "description_recovery_prohibited_job_posts": stats[
                "description_recovery_prohibited_job_posts"
            ],
            "description_recovery_unmapped_job_posts": stats[
                "description_recovery_unmapped_job_posts"
            ],
            "description_recovery_unavailable_job_posts": stats["description_recovery_unavailable_job_posts"],
            "oldest_missing_description_age_seconds": stats["oldest_missing_description_age_seconds"],
            "degraded": bool(degraded_reasons),
            "degraded_reasons": degraded_reasons,
        },
    )
