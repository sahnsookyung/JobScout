#!/usr/bin/env python3
"""
Policy endpoints - manage result filtering policies.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Annotated, cast

from core.config_loader import RankingConfig
from core.metrics import record_match_query_degraded

from ..dependencies import TenantContext, get_current_user, get_db, get_tenant_context
from ..services.match_service import MatchService
from ..services.policy_service import get_policy_service
from ..models.requests import PolicyUpdate
from ..models.responses import PolicyResponse, ScoringWeightsResponse
from ..config import get_config

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api",
    tags=["policy"],
    dependencies=[Depends(get_current_user)],
)

DbSession = Annotated[Session, Depends(get_db)]

_ZERO_LLM_ENQUEUE_STATS = {
    "attempted": 0,
    "reused": 0,
    "created": 0,
    "enqueued": 0,
    "failed": 0,
}


def _zero_llm_enqueue_stats() -> dict[str, int]:
    return dict(_ZERO_LLM_ENQUEUE_STATS)

def _should_enqueue_llm_top_n(
    previous_policy,
    next_policy,
) -> bool:
    return (
        bool(getattr(next_policy, "auto_enqueue_enabled", False))
        and bool(getattr(next_policy, "enabled", False))
        and bool(getattr(next_policy, "available", False))
        and (
            not bool(getattr(previous_policy, "enabled", False))
            or int(getattr(next_policy, "top_n", 0) or 0)
            > int(getattr(previous_policy, "top_n", 0) or 0)
        )
    )


def _enqueue_llm_top_n_after_policy_update(
    db,
    *,
    owner_id,
    tenant_id,
    previous_policy,
    next_policy,
) -> tuple[dict[str, int], list[dict[str, str]], str | None, str | None]:
    if not _should_enqueue_llm_top_n(previous_policy, next_policy):
        return _zero_llm_enqueue_stats(), [], None, None

    try:
        canonical_selection = MatchService(db)._resolve_canonical_selection(
            owner_id=owner_id,
            tenant_id=tenant_id,
        )
        if canonical_selection is None:
            return _zero_llm_enqueue_stats(), [], "skipped", None

        from core.llm_evaluation_queue import enqueue_llm_top_n_for_selection

        scheduled = enqueue_llm_top_n_for_selection(
            selection_run_id=canonical_selection.selection_run_id,
            owner_id=owner_id,
            tenant_id=tenant_id,
            top_n=int(getattr(next_policy, "top_n", 0) or 0),
            policy_revision=int(getattr(next_policy, "revision", 0) or 0),
        )
        return (
            _zero_llm_enqueue_stats(),
            [],
            str(scheduled.get("state") or "scheduled"),
            scheduled.get("job_id"),
        )
    except Exception as exc:
        logger.warning("Could not enqueue LLM top-N evaluations after policy update: %s", exc)
        record_match_query_degraded("policy_llm_enqueue_unavailable")
        return (
            _zero_llm_enqueue_stats(),
            [
                {
                    "reason": "policy_llm_enqueue_unavailable",
                    "detail": exc.__class__.__name__,
                }
            ],
            "failed",
            None,
        )


def _policy_response(
    policy,
    llm_policy,
    *,
    ranking_config: RankingConfig | None = None,
    llm_enqueue_stats: dict[str, int] | None = None,
    llm_enqueue_state: str | None = None,
    llm_enqueue_job_id: str | None = None,
    degraded_reasons: list[dict[str, str]] | None = None,
) -> PolicyResponse:
    ranking_config = ranking_config or RankingConfig()
    unavailable_reason = getattr(llm_policy, "unavailable_reason", "available")
    if not isinstance(unavailable_reason, str):
        unavailable_reason = "available" if getattr(llm_policy, "available", False) else "unknown"
    degraded_reasons = degraded_reasons or []
    return PolicyResponse(
        min_fit=policy.min_fit,
        top_k=policy.top_k,
        min_jd_required_coverage=policy.min_jd_required_coverage,
        active_default_mode=ranking_config.active_default_mode,
        balanced_w_pref=ranking_config.balanced_w_pref,
        balanced_w_fit=ranking_config.balanced_w_fit,
        llm_judge_enabled=llm_policy.enabled,
        llm_judge_auto_enqueue_enabled=getattr(llm_policy, "auto_enqueue_enabled", False),
        llm_judge_top_n=llm_policy.top_n,
        llm_judge_top_n_max=llm_policy.top_n_max,
        llm_judge_available=llm_policy.available,
        llm_judge_unavailable_reason=unavailable_reason,
        llm_judge_revision=llm_policy.revision,
        llm_judge_enqueue_stats=llm_enqueue_stats,
        llm_judge_enqueue_state=llm_enqueue_state,
        llm_judge_enqueue_job_id=llm_enqueue_job_id,
        degraded=bool(degraded_reasons),
        degraded_reasons=degraded_reasons,
    )


def _get_ranking_config(policy_service) -> RankingConfig:
    try:
        ranking_config = policy_service.get_ranking_config()
    except Exception:
        logger.warning("Could not load ranking configuration; using defaults", exc_info=True)
        return RankingConfig()
    return ranking_config if isinstance(ranking_config, RankingConfig) else RankingConfig()


def _updated_ranking_config(
    current: RankingConfig,
    policy_update: PolicyUpdate,
) -> RankingConfig:
    fields_set = policy_update.model_fields_set
    data = current.model_dump()
    if "active_default_mode" in fields_set and policy_update.active_default_mode is not None:
        data["active_default_mode"] = policy_update.active_default_mode

    preference_updated = "balanced_w_pref" in fields_set
    fit_updated = "balanced_w_fit" in fields_set
    if preference_updated and policy_update.balanced_w_pref is not None:
        data["balanced_w_pref"] = policy_update.balanced_w_pref
        if not fit_updated:
            data["balanced_w_fit"] = 1.0 - policy_update.balanced_w_pref
    if fit_updated and policy_update.balanced_w_fit is not None:
        data["balanced_w_fit"] = policy_update.balanced_w_fit
        if not preference_updated:
            data["balanced_w_pref"] = 1.0 - policy_update.balanced_w_fit
    return RankingConfig(**data)


@router.get("/v1/policy", response_model=PolicyResponse)
def get_policy(user: Annotated[object, Depends(get_current_user)]):
    """
    Get current result policy configuration.
    
    Returns the in-memory policy settings for filtering and truncating results.
    """
    policy_service = get_policy_service()
    policy = policy_service.get_current_policy()
    llm_policy = policy_service.get_llm_judge_policy(getattr(user, "id", None))
    ranking_config = _get_ranking_config(policy_service)

    return _policy_response(policy, llm_policy, ranking_config=ranking_config)


@router.put("/v1/policy", response_model=PolicyResponse)
def update_policy(
    policy_update: PolicyUpdate,
    user: Annotated[object, Depends(get_current_user)],
    db: DbSession,
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
):
    """
    Update result policy configuration.
    
    Updates persisted policy settings. Changes are stored in the database.
    
    - min_fit: Minimum fit score (0-100) to include in results
    - top_k: Maximum number of results to return (1-500)
    - min_jd_required_coverage: Minimum job description coverage (0-1), or null to disable
    """
    policy_service = get_policy_service()
    current_policy = policy_service.get_current_policy()
    current_ranking_config = _get_ranking_config(policy_service)
    previous_llm_policy = policy_service.get_llm_judge_policy(getattr(user, "id", None))
    fields_set = policy_update.model_fields_set

    policy = policy_service.update_policy(
        min_fit=cast(
            float,
            policy_update.min_fit
            if "min_fit" in fields_set and policy_update.min_fit is not None
            else current_policy.min_fit,
        ),
        top_k=cast(
            int,
            policy_update.top_k
            if "top_k" in fields_set and policy_update.top_k is not None
            else current_policy.top_k,
        ),
        min_jd_required_coverage=(
            policy_update.min_jd_required_coverage
            if "min_jd_required_coverage" in fields_set
            else current_policy.min_jd_required_coverage
        ),
    )
    llm_policy = policy_service.update_llm_judge_policy(
        owner_id=getattr(user, "id", None),
        enabled=policy_update.llm_judge_enabled,
        auto_enqueue_enabled=policy_update.llm_judge_auto_enqueue_enabled,
        top_n=policy_update.llm_judge_top_n,
    )
    ranking_fields = {"active_default_mode", "balanced_w_pref", "balanced_w_fit"}
    try:
        ranking_config = _updated_ranking_config(current_ranking_config, policy_update)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if fields_set & ranking_fields:
        ranking_config = policy_service.update_ranking_config(ranking_config)
    enqueue_stats, degraded_reasons, enqueue_state, enqueue_job_id = (
        _enqueue_llm_top_n_after_policy_update(
            db,
            owner_id=getattr(user, "id", None),
            tenant_id=tenant_context.tenant_id,
            previous_policy=previous_llm_policy,
            next_policy=llm_policy,
        )
    )

    return _policy_response(
        policy,
        llm_policy,
        ranking_config=ranking_config,
        llm_enqueue_stats=enqueue_stats,
        llm_enqueue_state=enqueue_state,
        llm_enqueue_job_id=enqueue_job_id,
        degraded_reasons=degraded_reasons,
    )


@router.post(
    "/v1/policy/preset/{preset_name}",
    response_model=PolicyResponse,
    responses={400: {"description": "Unknown preset name"}}
)
def apply_preset(
    preset_name: str,
    user: Annotated[object, Depends(get_current_user)],
):
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
    llm_policy = policy_service.get_llm_judge_policy(getattr(user, "id", None))
    ranking_config = _get_ranking_config(policy_service)

    return _policy_response(policy, llm_policy, ranking_config=ranking_config)


@router.get("/config/scoring-weights", response_model=ScoringWeightsResponse)
def get_scoring_weights():
    """
    Get the current final-score source.
    """
    get_config()
    
    return ScoringWeightsResponse(
        fit_score_source="fit-only"
    )
