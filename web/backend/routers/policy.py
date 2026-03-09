#!/usr/bin/env python3
"""
Policy endpoints - manage result filtering policies.
"""

from fastapi import APIRouter, HTTPException
from typing import Optional, cast

from ..services.policy_service import get_policy_service
from ..models.requests import PolicyUpdate
from ..models.responses import PolicyResponse, ScoringWeightsResponse
from ..config import get_config

router = APIRouter(prefix="/api", tags=["policy"])


@router.get("/v1/policy", response_model=PolicyResponse)
def get_policy():
    """
    Get current result policy configuration.
    
    Returns the in-memory policy settings for filtering and truncating results.
    """
    policy_service = get_policy_service()
    policy = policy_service.get_current_policy()
    
    return PolicyResponse(
        min_fit=policy.min_fit,
        top_k=policy.top_k,
        min_jd_required_coverage=policy.min_jd_required_coverage
    )


@router.put("/v1/policy", response_model=PolicyResponse)
def update_policy(policy_update: PolicyUpdate):
    """
    Update result policy configuration.
    
    Updates persisted policy settings. Changes are stored in the database.
    
    - min_fit: Minimum fit score (0-100) to include in results
    - top_k: Maximum number of results to return (1-500)
    - min_jd_required_coverage: Minimum job description coverage (0-1), or null to disable
    """
    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()
    
    policy = policy_service.update_policy(
        min_fit=cast(float, policy_update.min_fit if policy_update.min_fit is not None else current_policy.min_fit),
        top_k=cast(int, policy_update.top_k if policy_update.top_k is not None else current_policy.top_k),
        min_jd_required_coverage=policy_update.min_jd_required_coverage if policy_update.min_jd_required_coverage is not None else current_policy.min_jd_required_coverage
    )
    
    return PolicyResponse(
        min_fit=policy.min_fit,
        top_k=policy.top_k,
        min_jd_required_coverage=policy.min_jd_required_coverage
    )


@router.post(
    "/v1/policy/preset/{preset_name}",
    response_model=PolicyResponse,
    responses={400: {"description": "Unknown preset name"}}
)
def apply_preset(preset_name: str):
    """
    Apply a result policy preset.
    
    Presets:
    - strict: min_fit=70, min_required_coverage=0.80, top_k=25
    - balanced: min_fit=55, min_required_coverage=0.60, top_k=50
    - discovery: min_fit=40, min_required_coverage=null, top_k=100

    Raises:
        400: Unknown preset name.
    """
    _VALID_PRESETS = {"strict", "balanced", "discovery"}
    normalized_preset = preset_name.lower()
    if normalized_preset not in _VALID_PRESETS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown preset '{preset_name}'. Valid presets: {', '.join(sorted(_VALID_PRESETS))}"
        )

    policy_service = get_policy_service()
    policy = policy_service.apply_preset(normalized_preset)
    
    return PolicyResponse(
        min_fit=policy.min_fit,
        top_k=policy.top_k,
        min_jd_required_coverage=policy.min_jd_required_coverage
    )


@router.get("/config/scoring-weights", response_model=ScoringWeightsResponse)
def get_scoring_weights():
    """
    Get current scoring weights configuration.
    
    Returns Fit/Want weights and facet weights for Want score calculation.
    """
    config = get_config()
    scorer_config = config.matching.scorer
    
    return ScoringWeightsResponse(
        fit_weight=scorer_config.fit_weight,
        want_weight=scorer_config.want_weight,
        facet_weights=scorer_config.facet_weights
    )
