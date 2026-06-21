#!/usr/bin/env python3
"""
Stats endpoints - view match statistics.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from core.match_selection import resolve_canonical_resume_selection
from database.uow import job_uow

from ..dependencies import TenantContext, get_current_user, get_db, get_tenant_context
from ..models.responses import StatsResponse
from ..services import stats_service as _stats_service
from ..services.policy_service import get_policy_service

router = APIRouter(prefix="/api/stats", tags=["stats"])

_canonical_stats_payload = _stats_service._canonical_stats_payload
_count_excluded_items_by_reason = _stats_service._count_excluded_items_by_reason
_count_items_for_run_by_tier = _stats_service._count_items_for_run_by_tier
_empty_stats_payload = _stats_service._empty_stats_payload
_item_fit_score = _stats_service._item_fit_score
_item_is_hidden_primary = _stats_service._item_is_hidden_primary
_job_processing_stats = _stats_service._job_processing_stats
_preference_status_for_selection_run = _stats_service._preference_status_for_selection_run
_selection_run_item_stats = _stats_service._selection_run_item_stats
_selection_run_item_stats_from_items = _stats_service._selection_run_item_stats_from_items


def _request_tenant_id(request: Request):
    """Compatibility wrapper for tests and callers importing the old helper."""
    return get_tenant_context(request).tenant_id


@router.get("", response_model=StatsResponse)
def get_stats(
    db: Annotated[Session, Depends(get_db)],
    user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
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
    """
    del db
    return build_stats_response(
        user=user,
        tenant_id=tenant_context.tenant_id,
        min_fit=min_fit,
        top_k=top_k,
    )


def build_stats_response(
    *,
    user: object,
    tenant_id: object | None,
    min_fit: float | None,
    top_k: int | None,
) -> StatsResponse:
    return _stats_service.build_stats_response(
        user=user,
        tenant_id=tenant_id,
        min_fit=min_fit,
        top_k=top_k,
        policy_service_factory=get_policy_service,
        job_uow_factory=job_uow,
        canonical_resolver=resolve_canonical_resume_selection,
    )
