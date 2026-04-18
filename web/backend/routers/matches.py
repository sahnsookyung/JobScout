#!/usr/bin/env python3
"""
Match endpoints - view and manage job matches.
"""

import uuid
import logging
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..dependencies import get_current_user, get_db
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


@router.get(
    "",
    response_model=MatchesResponse,
    responses={422: {"description": "Invalid query parameter"}}
)
def get_matches(
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
    status: Annotated[str, Query(description="Match status: active, stale, or all")] = "active",
    min_fit: Annotated[float | None, Query(ge=0, le=100, description="Minimum fit score filter")] = None,
    top_k: Annotated[int | None, Query(ge=1, le=500, description="Maximum results to return")] = None,
    remote_only: Annotated[bool, Query(description="Filter to remote jobs only")] = False,
    show_hidden: Annotated[bool, Query(description="Include hidden matches in results")] = False,
    ranking_mode: Annotated[str | None, Query(description="Ranking mode: preference_first, fit_first, or balanced")] = None,
    tier: Annotated[str, Query(description="Selection tier: primary (default) or all (include excluded)")] = "primary",
):
    """
    Get a list of job matches ranked by the declared mode.

    Stage 1 retrieves the canonical resume's persisted match set.
    Stage 2 re-ranks using the requested mode with NULL-aware sort keys.
    Stage 3 truncates to effective_top_k.

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

    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()

    effective_top_k = top_k if top_k is not None else current_policy.top_k

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
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Get detailed information about a specific match.
    
    Includes match metadata, job details, and requirement coverage.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    return service.get_match_detail(match_id, owner_id=getattr(user, "id", None))


@router.post(
    "/{match_id}/hide",
    response_model=HideMatchResponse,
    responses={400: {"description": "Invalid match ID"}},
)
def toggle_match_hidden(
    match_id: str,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Toggle the hidden status of a match.
    
    Returns the updated hidden status.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    new_status = service.toggle_hidden(match_id, owner_id=getattr(user, "id", None))
    
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
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    """
    Get explainability details for a specific match.

    Returns the persisted semantic fit explanation generated during scoring.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    result = service.get_match_explanation(match_id, owner_id=getattr(user, "id", None))
    
    return MatchExplanationResponse(**result)
