#!/usr/bin/env python3
"""
Match endpoints - view and manage job matches.
"""

import uuid
import logging
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from ..dependencies import get_db
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


@router.get("", response_model=MatchesResponse)
def get_matches(
    status: str = Query(default="active", description="Match status: active, stale, or all"),
    min_fit: float = Query(default=None, ge=0, le=100, description="Minimum fit score filter"),
    top_k: int = Query(default=None, ge=1, le=500, description="Maximum results to return"),
    remote_only: bool = Query(default=False, description="Filter to remote jobs only"),
    show_hidden: bool = Query(default=False, description="Include hidden matches in results"),
    db: Session = Depends(get_db)
):
    """
    Get a list of job matches filtered by result policy.
    
    Uses the current policy settings by default (min_fit, top_k).
    Both can be overridden via query parameters.
    Returns matches sorted by overall score (highest first).
    """
    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()
    
    # Use policy defaults if not specified
    effective_min_fit = min_fit if min_fit is not None else current_policy.min_fit
    effective_top_k = top_k if top_k is not None else current_policy.top_k
    
    service = MatchService(db)
    matches = service.get_matches(
        status=status,
        min_fit=effective_min_fit,
        top_k=effective_top_k,
        remote_only=remote_only,
        show_hidden=show_hidden
    )
    
    return MatchesResponse(
        success=True,
        count=len(matches),
        matches=matches
    )


@router.get("/{match_id}", response_model=MatchDetailResponse)
def get_match_details(
    match_id: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific match.
    
    Includes match metadata, job details, and requirement coverage.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    return service.get_match_detail(match_id)


@router.post("/{match_id}/hide", response_model=HideMatchResponse)
def toggle_match_hidden(
    match_id: str,
    db: Session = Depends(get_db)
):
    """
    Toggle the hidden status of a match.
    
    Returns the updated hidden status.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    new_status = service.toggle_hidden(match_id)
    
    return HideMatchResponse(
        success=True,
        match_id=match_id,
        is_hidden=new_status
    )


@router.get("/{match_id}/explanation", response_model=MatchExplanationResponse)
def get_match_explanation(
    match_id: str,
    db: Session = Depends(get_db)
):
    """
    Get explainability details for a specific match.
    
    Shows which resume sections matched which job requirements,
    enabling explainable match scores and actionable feedback.
    """
    validate_uuid(match_id)
    service = MatchService(db)
    result = service.get_match_explanation(match_id)
    
    return MatchExplanationResponse(**result)
