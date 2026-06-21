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
