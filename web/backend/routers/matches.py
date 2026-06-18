#!/usr/bin/env python3
"""
Match endpoints - view and manage job matches.
"""

import uuid
import logging
from typing import Annotated
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from core.llm_evaluation import (
    LlmJudgeConflictError,
    LlmJudgeQuotaExceededError,
    LlmJudgeUnavailableError,
    MatchLlmEvaluationService,
    evaluation_public_dict,
)
from ..dependencies import get_current_user, get_db
from ..exceptions import InvalidMatchOperationException
from ..models.requests import MatchLlmEvaluationRequest
from ..services.match_service import MatchService
from ..services.policy_service import get_policy_service
from ..models.responses import (
    MatchesResponse,
    MatchDetailResponse,
    HideMatchResponse,
    MatchExplanationResponse,
    MatchLlmEvaluationListResponse,
    MatchLlmEvaluationMutationResponse,
    MatchLlmEvaluationSummary,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/matches", tags=["matches"])

DbSession = Annotated[Session, Depends(get_db)]


def _run_match_llm_evaluation_background(
    evaluation_id: str,
    provider_payload: dict,
    truncation: dict | None,
) -> None:
    from database.database import SessionLocal

    db = SessionLocal()
    try:
        service = MatchLlmEvaluationService(db)
        service.run_pending_evaluation(
            evaluation_id,
            provider_payload,
            truncation=truncation or {},
        )
    except Exception:
        logger.exception("Background LLM evaluation failed for %s", evaluation_id)
        db.rollback()
    finally:
        db.close()


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


def _to_evaluation_summary(evaluation) -> MatchLlmEvaluationSummary:
    return MatchLlmEvaluationSummary(**evaluation_public_dict(evaluation))


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

def _safe_nonnegative_int(value, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(parsed, 0)


@router.get(
    "",
    response_model=MatchesResponse,
    responses={
        400: {"description": "Invalid tenant header"},
        422: {"description": "Invalid query parameter"},
    }
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
    limit: Annotated[int | None, Query(ge=1, le=500, description="Optional response page size applied after ranking")] = None,
    offset: Annotated[int, Query(ge=0, description="Response page offset applied with limit")] = 0,
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
        limit=limit,
        offset=offset,
    )

    llm_rerank = getattr(service, "last_llm_rerank_metadata", None)
    if not isinstance(llm_rerank, dict):
        llm_rerank = {}
    total = _safe_nonnegative_int(
        getattr(service, "last_matches_total", None),
        len(matches),
    )
    response_limit = getattr(service, "last_matches_limit", limit)
    if response_limit is not None:
        try:
            response_limit = max(int(response_limit), 0)
        except (TypeError, ValueError):
            response_limit = limit
    response_offset = _safe_nonnegative_int(
        getattr(service, "last_matches_offset", None),
        offset,
    )
    has_more = (
        response_limit is not None
        and response_offset + len(matches) < total
    )

    return MatchesResponse(
        success=True,
        count=len(matches),
        total=total,
        limit=response_limit,
        offset=response_offset,
        has_more=has_more,
        matches=matches,
        llm_rerank=llm_rerank,
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


@router.get(
    "/{match_id}/llm-evaluations",
    response_model=MatchLlmEvaluationListResponse,
    responses={400: {"description": "Invalid match ID"}, 404: {"description": "Match not found"}},
)
def list_match_llm_evaluations(
    match_id: str,
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    validate_uuid(match_id)
    service = MatchLlmEvaluationService(db)
    try:
        evaluations = service.list_for_match(
            match_id,
            owner_id=getattr(user, "id", None),
            tenant_id=_request_tenant_id(request),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="Match not found") from exc

    return MatchLlmEvaluationListResponse(
        success=True,
        count=len(evaluations),
        evaluations=[_to_evaluation_summary(evaluation) for evaluation in evaluations],
    )


@router.post(
    "/{match_id}/llm-evaluations",
    response_model=MatchLlmEvaluationMutationResponse,
    responses={
        400: {"description": "Invalid match ID"},
        404: {"description": "Match not found"},
        409: {"description": "Evaluation already running"},
        429: {"description": "LLM judge quota exhausted"},
        503: {"description": "LLM judge unavailable"},
    },
)
def generate_match_llm_evaluation(
    match_id: str,
    body: MatchLlmEvaluationRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    validate_uuid(match_id)
    service = MatchLlmEvaluationService(db)
    try:
        result = service.start_for_match(
            match_id,
            owner_id=getattr(user, "id", None),
            tenant_id=_request_tenant_id(request),
            force=body.force,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="Match not found") from exc
    except LlmJudgeConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except LlmJudgeQuotaExceededError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    except LlmJudgeUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    should_run = bool(getattr(result, "should_run", False))
    if should_run:
        background_tasks.add_task(
            _run_match_llm_evaluation_background,
            str(result.evaluation.id),
            getattr(result, "provider_payload", None) or {},
            getattr(result, "truncation", None) or {},
        )

    return MatchLlmEvaluationMutationResponse(
        success=True,
        evaluation=_to_evaluation_summary(result.evaluation),
        reused=result.reused,
        accepted=should_run,
        message="Reused cached LLM evaluation." if result.reused else "Queued LLM evaluation.",
    )


@router.delete(
    "/{match_id}/llm-evaluations/{evaluation_id}",
    response_model=MatchLlmEvaluationMutationResponse,
    responses={
        400: {"description": "Invalid match/evaluation ID"},
        404: {"description": "Evaluation not found"},
    },
)
def delete_match_llm_evaluation(
    match_id: str,
    evaluation_id: str,
    request: Request,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
):
    validate_uuid(match_id)
    validate_uuid(evaluation_id)
    service = MatchLlmEvaluationService(db)
    try:
        service.delete_evaluation(
            match_id,
            evaluation_id,
            owner_id=getattr(user, "id", None),
            tenant_id=_request_tenant_id(request),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="Evaluation not found") from exc

    return MatchLlmEvaluationMutationResponse(
        success=True,
        evaluation=None,
        reused=False,
        message="Deleted LLM evaluation.",
    )
