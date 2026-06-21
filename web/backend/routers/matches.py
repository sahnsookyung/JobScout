#!/usr/bin/env python3
"""
Match endpoints - view and manage job matches.
"""

import uuid
import logging
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from core.llm_evaluation import (
    LlmJudgeConflictError,
    LlmJudgeQuotaExceededError,
    LlmJudgeUnavailableError,
    MatchLlmEvaluationService,
    evaluation_public_dict,
)
from core.llm_evaluation_queue import enqueue_llm_evaluation
from core.metrics import record_match_query_payload_bytes
from ..dependencies import TenantContext, get_current_user, get_db, get_tenant_context
from ..exceptions import InvalidMatchOperationException
from ..models.requests import MatchLlmEvaluationRequest
from ..services.match_service import DEFAULT_ALL_TIER_PAGE_LIMIT, MatchService
from ..services.policy_service import get_policy_service
from ..services.cursors import CursorDecodeError
from ..models.responses import (
    MatchesResponse,
    MatchDetailResponse,
    HideMatchResponse,
    MatchExplanationResponse,
    MatchLlmEvaluationListResponse,
    MatchLlmEvaluationMutationResponse,
    MatchLlmEvaluationSummary,
    StatsResponse,
)
from .stats import build_stats_response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/matches", tags=["matches"])

DbSession = Annotated[Session, Depends(get_db)]


def _run_match_llm_evaluation_background(
    evaluation_id: str,
    provider_payload: dict,
    truncation: dict | None,
) -> str:
    """Compatibility hook that now enqueues durable RQ work."""
    return enqueue_llm_evaluation(
        evaluation_id,
        provider_payload=provider_payload,
        truncation=truncation or {},
    )


def _llm_evaluation_queue_unavailable_detail(exc: BaseException) -> str:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        exc_name = type(current).__name__.lower()
        message = str(current).lower()
        if (
            "ratelimit" in exc_name
            or "rate limit" in message
            or "rate_limit" in message
            or "429" in message
        ):
            return "LLM evaluation queue unavailable: provider rate limit"
        current = current.__cause__ or current.__context__
    return "LLM evaluation queue unavailable"


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
_VALID_PAGE_MODES = {"offset", "cursor"}
_VALID_VIEWS = {"summary", "compact"}
_VALID_INCLUDES = {"llm"}


def _request_tenant_id(request: Request):
    """Compatibility wrapper for callers importing the old helper."""
    return get_tenant_context(request).tenant_id

def _safe_nonnegative_int(value, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(parsed, 0)

def _safe_str_attr(obj, name: str, fallback: str) -> str:
    value = getattr(obj, name, fallback)
    return value if isinstance(value, str) else fallback

def _safe_optional_str_attr(obj, name: str):
    value = getattr(obj, name, None)
    return value if isinstance(value, str) else None


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
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
    status: Annotated[str, Query(description="Match status for primary-tier matches: active, stale, or all")] = "active",
    min_fit: Annotated[float | None, Query(ge=0, le=100, description="Minimum fit score filter")] = None,
    top_k: Annotated[int | None, Query(ge=1, le=500, description="Maximum pre-page result window. Defaults to policy top_k for tier=primary.")] = None,
    remote_only: Annotated[bool, Query(description="Filter to remote jobs only")] = False,
    show_hidden: Annotated[bool, Query(description="Include hidden primary-tier matches in results")] = False,
    ranking_mode: Annotated[str | None, Query(description="Ranking mode: preference_first, fit_first, or balanced")] = None,
    tier: Annotated[str, Query(description="Selection tier: primary (default) or all (include excluded; status/show_hidden only apply to primary items)")] = "primary",
    limit: Annotated[int | None, Query(ge=1, le=500, description="Response page size applied after ranking. tier=all defaults to a bounded first page when omitted.")] = None,
    offset: Annotated[int, Query(ge=0, description="Response page offset applied with limit")] = 0,
    cursor: Annotated[str | None, Query(description="Opaque cursor returned by a prior cursor-mode response")] = None,
    page_mode: Annotated[str, Query(description="Pagination mode: offset (default) or cursor")] = "offset",
    view: Annotated[str, Query(description="Payload view: summary (default) or compact")] = "summary",
    include: Annotated[str | None, Query(description="Comma-delimited optional expansions; currently supports llm")] = None,
):
    """
    Get a list of job matches ranked by the declared mode.

    Stage 1 retrieves the canonical resume's persisted match set.
    Stage 2 re-ranks using the requested mode with NULL-aware sort keys.
    Stage 3 truncates primary-tier results to effective_top_k by default.
    When `tier=all`, the full canonical run is only exposed through bounded
    pages; omitted `limit` returns the first server-default page.

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
    if page_mode not in _VALID_PAGE_MODES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid page_mode '{page_mode}'. Valid values: {', '.join(sorted(_VALID_PAGE_MODES))}"
        )
    if view not in _VALID_VIEWS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid view '{view}'. Valid values: {', '.join(sorted(_VALID_VIEWS))}"
        )
    if include:
        include_values = {value.strip() for value in include.split(",") if value.strip()}
        unknown_includes = include_values - _VALID_INCLUDES
        if unknown_includes:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid include '{sorted(unknown_includes)[0]}'. Valid values: {', '.join(sorted(_VALID_INCLUDES))}"
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
    effective_limit = limit
    if effective_limit is None and tier == "all":
        effective_limit = DEFAULT_ALL_TIER_PAGE_LIMIT

    service = MatchService(db)
    try:
        matches = service.get_matches(
            owner_id=getattr(user, "id", None),
            status=status,
            min_fit=min_fit,
            top_k=effective_top_k,
            remote_only=remote_only,
            show_hidden=show_hidden,
            ranking_mode=ranking_mode,
            tier=tier,
            tenant_id=tenant_context.tenant_id,
            limit=effective_limit,
            offset=offset,
            cursor=cursor,
            page_mode=page_mode,
            view=view,
            include=include,
        )
    except CursorDecodeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    llm_rerank = getattr(service, "last_llm_rerank_metadata", None)
    if not isinstance(llm_rerank, dict):
        llm_rerank = {}
    degraded_reasons = getattr(service, "last_degraded_reasons", None)
    if not isinstance(degraded_reasons, list):
        degraded_reasons = []
    total = _safe_nonnegative_int(
        getattr(service, "last_matches_total", None),
        len(matches),
    )
    response_limit = getattr(service, "last_matches_limit", effective_limit)
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
    explicit_has_more = getattr(service, "last_matches_has_more", None)
    if isinstance(explicit_has_more, bool):
        has_more = bool(explicit_has_more)

    response = MatchesResponse(
        success=True,
        count=len(matches),
        total=total,
        limit=response_limit,
        offset=response_offset,
        has_more=has_more,
        page_mode=_safe_str_attr(service, "last_matches_page_mode", page_mode),
        view=_safe_str_attr(service, "last_matches_view", view),
        next_cursor=_safe_optional_str_attr(service, "last_matches_next_cursor"),
        llm_judge_revision=_safe_nonnegative_int(llm_rerank.get("policy_revision"), 0),
        rank_source=_safe_str_attr(service, "last_matches_rank_source", "computed"),
        matches=matches,
        llm_rerank=llm_rerank,
        degraded=bool(degraded_reasons),
        degraded_reasons=degraded_reasons,
    )
    record_match_query_payload_bytes(
        response.page_mode,
        response.view,
        len(response.model_dump_json().encode("utf-8")),
    )
    return response


@router.get(
    "/summary",
    response_model=StatsResponse,
    responses={
        400: {"description": "Invalid tenant header"},
        422: {"description": "Invalid query parameter"},
    },
)
def get_match_summary(
    request: Request,
    _db: DbSession,
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
    """Return lightweight match and job-processing counts without match hydration."""
    return build_stats_response(
        user=user,
        tenant_id=tenant_context.tenant_id,
        min_fit=min_fit,
        top_k=top_k,
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
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
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
        tenant_id=tenant_context.tenant_id,
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
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
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
            tenant_id=tenant_context.tenant_id,
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
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
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
        tenant_id=tenant_context.tenant_id,
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
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
):
    validate_uuid(match_id)
    service = MatchLlmEvaluationService(db)
    try:
        evaluations = service.list_for_match(
            match_id,
            owner_id=getattr(user, "id", None),
            tenant_id=tenant_context.tenant_id,
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
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
):
    validate_uuid(match_id)
    service = MatchLlmEvaluationService(db)
    try:
        result = service.start_for_match(
            match_id,
            owner_id=getattr(user, "id", None),
            tenant_id=tenant_context.tenant_id,
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
        try:
            _run_match_llm_evaluation_background(
                str(result.evaluation.id),
                getattr(result, "provider_payload", None) or {},
                getattr(result, "truncation", None) or {},
            )
        except Exception as exc:
            logger.exception("Failed to enqueue LLM evaluation %s", result.evaluation.id)
            raise HTTPException(
                status_code=503,
                detail=_llm_evaluation_queue_unavailable_detail(exc),
            ) from exc

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
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
):
    validate_uuid(match_id)
    validate_uuid(evaluation_id)
    service = MatchLlmEvaluationService(db)
    try:
        service.delete_evaluation(
            match_id,
            evaluation_id,
            owner_id=getattr(user, "id", None),
            tenant_id=tenant_context.tenant_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="Evaluation not found") from exc

    return MatchLlmEvaluationMutationResponse(
        success=True,
        evaluation=None,
        reused=False,
        message="Deleted LLM evaluation.",
    )
