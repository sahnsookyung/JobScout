#!/usr/bin/env python3
"""
Match endpoints - view and manage job matches.
"""

import uuid
import logging
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from ..dependencies import get_current_user, get_db
from ..exceptions import InvalidMatchOperationException
from ..services.match_service import MatchService
from ..services.policy_service import get_policy_service
from ..models.responses import (
    MatchesResponse,
    MatchDetailResponse,
    HideMatchResponse,
    MatchExplanationResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/matches", tags=["matches"])

DbSession = Annotated[Session, Depends(get_db)]


def validate_uuid(match_id: str) -> str:
    """Validate that match_id is a valid UUID format."""
    try:
        uuid.UUID(match_id)
        return match_id
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid match_id format: {match_id}. Must be a valid UUID."
        )


_VALID_RANKING_MODES = {"preference_first", "fit_first", "balanced"}
_VALID_TIERS = {"primary", "all"}


def _request_tenant_id(request: Request):
    """Return the cloud-selected tenant ID when the SaaS wrapper set one."""
    state_tenant_id = getattr(request.state, "tenant_id", None)
    if state_tenant_id is not None:
        return state_tenant_id

    tenant_header = request.headers.get("X-Tenant-Id", "").strip()
    if not tenant_header:
        return None
    try:
        return uuid.UUID(tenant_header)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="X-Tenant-Id must be a UUID.") from exc


@router.get(
    "",
    response_model=MatchesResponse,
    responses={422: {"description": "Invalid query parameter"}}
)
def get_matches(
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
    status: Annotated[str, Query(description="Match status for primary-tier matches: active, stale, or all")] = "active",
    min_fit: Annotated[float | None, Query(ge=0, le=100, description="Minimum fit score filter")] = None,
    top_k: Annotated[int | None, Query(ge=1, le=500, description="Maximum results to return. When tier=all and omitted, the full canonical run is returned.")] = None,
    remote_only: Annotated[bool, Query(description="Filter to remote jobs only")] = False,
    show_hidden: Annotated[bool, Query(description="Include hidden primary-tier matches in results")] = False,
    ranking_mode: Annotated[str | None, Query(description="Ranking mode: preference_first, fit_first, or balanced")] = None,
    tier: Annotated[str, Query(description="Selection tier: primary (default) or all (include excluded; status/show_hidden only apply to primary items)")] = "primary",
):
    """
    Get a list of job matches ranked by the declared mode.

    Stage 1 retrieves the canonical resume's persisted match set.
    Stage 2 re-ranks using the requested mode with NULL-aware sort keys.
    Stage 3 truncates primary-tier results to effective_top_k by default.
    When `tier=all`, omitted `top_k` returns the full canonical run and an
    explicit `top_k` caps the final combined result count.

    Raises:
        422: Invalid `status` or `ranking_mode` value.
    """
    _VALID_STATUSES = {"active", "stale", "all"}
    if status not in _VALID_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid status '{status}'. Valid values: {', '.join(sorted(_VALID_STATUSES))}"
        )

    if ranking_mode is not None and ranking_mode not in _VALID_RANKING_MODES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid ranking_mode '{ranking_mode}'. Valid values: {', '.join(sorted(_VALID_RANKING_MODES))}"
        )

    if tier not in _VALID_TIERS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid tier '{tier}'. Valid values: {', '.join(sorted(_VALID_TIERS))}"
        )

    # Rollout gate: when two-tier selection is disabled, tier=all collapses
    # to tier=primary so the API behaves like the pre-§C single-tier contract.
    from core.config_loader import load_config
    if tier == "all":
        matching_cfg = getattr(load_config(), "matching", None)
        if matching_cfg is not None and not getattr(
            matching_cfg, "two_tier_selection_enabled", True
        ):
            tier = "primary"

    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()

    effective_top_k = top_k
    if effective_top_k is None and tier == "primary":
        effective_top_k = current_policy.top_k

    service = MatchService(db)
    matches = service.get_matches(
        owner_id=getattr(user, "id", None),
        status=status,
        min_fit=min_fit,
        top_k=effective_top_k,
        remote_only=remote_only,
        show_hidden=show_hidden,
        ranking_mode=ranking_mode,
        tier=tier,
        tenant_id=_request_tenant_id(request),
    )

    return MatchesResponse(
        success=True,
        count=len(matches),
        matches=matches
    )


@router.get(
    "/{match_id}",
    response_model=MatchDetailResponse,
    responses={400: {"description": "Invalid match ID"}},
)
def get_match_details(
    match_id: str,
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Get detailed information about a specific match.
    
    Includes match metadata, job details, and requirement coverage.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    return service.get_match_detail(
        match_id,
        owner_id=getattr(user, "id", None),
        tenant_id=_request_tenant_id(request),
    )


@router.post(
    "/{match_id}/hide",
    response_model=HideMatchResponse,
    responses={
        400: {"description": "Invalid match ID"},
        409: {"description": "Match cannot be hidden in its current selection tier"},
    },
)
def toggle_match_hidden(
    match_id: str,
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Toggle the hidden status of a match.
    
    Returns the updated hidden status.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    try:
        new_status = service.toggle_hidden(
            match_id,
            owner_id=getattr(user, "id", None),
            tenant_id=_request_tenant_id(request),
        )
    except InvalidMatchOperationException as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    
    return HideMatchResponse(
        success=True,
        match_id=match_id,
        is_hidden=new_status
    )


@router.get(
    "/{match_id}/explanation",
    response_model=MatchExplanationResponse,
    responses={400: {"description": "Invalid match ID"}},
)
def get_match_explanation(
    match_id: str,
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Get explainability details for a specific match.

    Returns the persisted semantic fit explanation generated during scoring.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    result = service.get_match_explanation(
        match_id,
        owner_id=getattr(user, "id", None),
        tenant_id=_request_tenant_id(request),
    )
    
    return MatchExplanationResponse(**result)
