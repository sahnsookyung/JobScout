"""Durable pipeline run operations endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..dependencies import TenantContext, get_current_user, get_db, get_tenant_context
from ..models.requests import LlmEvaluationQueuePauseRequest, LlmProviderCircuitResetRequest
from ..models.responses import (
    LlmEvaluationQueueOperationResponse,
    LlmEvaluationQueueStatusResponse,
    LlmProviderCanaryResponse,
    LlmProviderCircuitResetResponse,
    LlmProviderStatusResponse,
    PipelineRunDetailResponse,
    PipelineRunSummary,
    PipelineRunOperationResponse,
    PipelineRunsResponse,
)
from ..services.pipeline_run_ops_service import pipeline_run_ops_service
from ..services.pipeline_run_service import pipeline_run_read_service
from ..services.cursors import CursorDecodeError

router = APIRouter(prefix="/api/pipeline-runs", tags=["pipeline-runs"])

DbSession = Annotated[Session, Depends(get_db)]

VALID_RUN_STATUSES = {"all", "pending", "running", "completed", "failed", "cancelled"}
VALID_RUN_TYPES = {
    "all",
    "stage",
    "pipeline",
    "match",
    "resume_upload",
    "repair",
    "scrape",
}
VALID_PAGE_MODES = {"offset", "cursor"}
VALID_RUN_VIEWS = {"compact", "detail"}


def _pipeline_run_for_view(run: PipelineRunSummary, view: str) -> PipelineRunSummary:
    if view != "compact":
        return run
    return run.model_copy(update={"metadata": {}, "stages": []})


@router.get(
    "",
    response_model=PipelineRunsResponse,
    responses={
        400: {"description": "Invalid tenant header"},
        422: {"description": "Invalid query parameter"},
    },
)
def get_pipeline_runs(
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
    status: Annotated[str, Query(description="Run status or all")] = "all",
    run_type: Annotated[str, Query(description="Run type or all")] = "all",
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    offset: Annotated[int, Query(ge=0)] = 0,
    cursor: Annotated[str | None, Query(description="Opaque cursor returned by a prior response")] = None,
    page_mode: Annotated[str, Query(description="Pagination mode: offset (default) or cursor")] = "offset",
    view: Annotated[str, Query(description="Payload view: detail (default) or compact")] = "detail",
) -> PipelineRunsResponse:
    if status not in VALID_RUN_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid status '{status}'. Valid values: {', '.join(sorted(VALID_RUN_STATUSES))}",
        )
    if run_type not in VALID_RUN_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid run_type '{run_type}'. Valid values: {', '.join(sorted(VALID_RUN_TYPES))}",
        )
    if page_mode not in VALID_PAGE_MODES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid page_mode '{page_mode}'. Valid values: {', '.join(sorted(VALID_PAGE_MODES))}",
        )
    if view not in VALID_RUN_VIEWS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid view '{view}'. Valid values: {', '.join(sorted(VALID_RUN_VIEWS))}",
        )

    try:
        result = pipeline_run_read_service.list_runs(
            db,
            tenant_id=tenant_context.tenant_id,
            status=status,
            run_type=run_type,
            limit=limit,
            offset=offset,
            cursor=cursor,
            page_mode=page_mode,
        )
    except (CursorDecodeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if len(result) == 2:
        runs, total = result
        next_cursor = None
        page_mode = "offset"
        response_offset = offset
        has_more = response_offset + len(runs) < total
    else:
        runs, total, next_cursor, has_more, page_mode, response_offset = result
        if page_mode == "offset":
            has_more = response_offset + len(runs) < total
    return PipelineRunsResponse(
        success=True,
        count=len(runs),
        total=total,
        limit=limit,
        offset=response_offset,
        has_more=has_more,
        page_mode=page_mode,
        view=view,
        next_cursor=next_cursor,
        runs=[_pipeline_run_for_view(run, view) for run in runs],
    )

@router.get(
    "/llm-evaluations/queue",
    response_model=LlmEvaluationQueueStatusResponse,
)
def get_llm_evaluation_queue_status(
    _user: Annotated[object, Depends(get_current_user)],
) -> LlmEvaluationQueueStatusResponse:
    return LlmEvaluationQueueStatusResponse(**pipeline_run_ops_service.llm_queue_status())


@router.post(
    "/llm-evaluations/queue/pause",
    response_model=LlmEvaluationQueueOperationResponse,
)
def pause_llm_evaluation_queue(
    request: LlmEvaluationQueuePauseRequest,
    _user: Annotated[object, Depends(get_current_user)],
) -> LlmEvaluationQueueOperationResponse:
    status = pipeline_run_ops_service.pause_llm_queue(
        reason=request.reason,
        ttl_seconds=request.ttl_seconds,
    )
    return LlmEvaluationQueueOperationResponse(
        success=True,
        action="pause_llm_queue",
        message="LLM queue paused. Running jobs will defer themselves without consuming retry attempts.",
        status=LlmEvaluationQueueStatusResponse(**status),
    )


@router.post(
    "/llm-evaluations/queue/resume",
    response_model=LlmEvaluationQueueOperationResponse,
)
def resume_llm_evaluation_queue(
    _user: Annotated[object, Depends(get_current_user)],
) -> LlmEvaluationQueueOperationResponse:
    status = pipeline_run_ops_service.resume_llm_queue()
    return LlmEvaluationQueueOperationResponse(
        success=True,
        action="resume_llm_queue",
        message="LLM queue resumed.",
        status=LlmEvaluationQueueStatusResponse(**status),
    )


@router.post(
    "/llm-evaluations/queue/retry",
    response_model=LlmEvaluationQueueOperationResponse,
)
def retry_llm_evaluation_queue(
    _user: Annotated[object, Depends(get_current_user)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> LlmEvaluationQueueOperationResponse:
    status, enqueued = pipeline_run_ops_service.retry_llm_queue(limit=limit)
    return LlmEvaluationQueueOperationResponse(
        success=True,
        action="retry_llm_queue",
        message=f"Queued {enqueued} pending, stale, or retryable LLM evaluations.",
        enqueued_count=enqueued,
        status=LlmEvaluationQueueStatusResponse(**status),
    )


@router.get(
    "/llm-evaluations/providers",
    response_model=LlmProviderStatusResponse,
)
def get_llm_provider_status(
    _user: Annotated[object, Depends(get_current_user)],
) -> LlmProviderStatusResponse:
    return LlmProviderStatusResponse(**pipeline_run_ops_service.llm_provider_status())


@router.post(
    "/llm-evaluations/providers/canary",
    response_model=LlmProviderCanaryResponse,
)
def run_llm_provider_canaries(
    _user: Annotated[object, Depends(get_current_user)],
) -> LlmProviderCanaryResponse:
    return LlmProviderCanaryResponse(**pipeline_run_ops_service.run_llm_provider_canaries())


@router.post(
    "/llm-evaluations/providers/circuit/reset",
    response_model=LlmProviderCircuitResetResponse,
    responses={400: {"description": "Provider and model are required"}},
)
def reset_llm_provider_circuit(
    request: LlmProviderCircuitResetRequest,
    _user: Annotated[object, Depends(get_current_user)],
) -> LlmProviderCircuitResetResponse:
    try:
        result = pipeline_run_ops_service.reset_llm_provider_circuit(
            provider=request.provider,
            model=request.model,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return LlmProviderCircuitResetResponse(**result)


@router.post(
    "/{run_id}/cancel",
    response_model=PipelineRunOperationResponse,
    responses={
        400: {"description": "Invalid tenant header or action not allowed"},
        404: {"description": "Pipeline run not found"},
    },
)
def cancel_pipeline_run(
    run_id: str,
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
) -> PipelineRunOperationResponse:
    try:
        run = pipeline_run_ops_service.cancel_run(
            db,
            tenant_id=tenant_context.tenant_id,
            run_id=run_id,
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Pipeline run not found") from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineRunOperationResponse(
        success=True,
        action="cancel",
        message="Pipeline run cancelled.",
        run=run,
    )


@router.post(
    "/{run_id}/requeue",
    response_model=PipelineRunOperationResponse,
    responses={
        400: {"description": "Invalid tenant header or action not allowed"},
        404: {"description": "Pipeline run not found"},
    },
)
def requeue_pipeline_run(
    run_id: str,
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
) -> PipelineRunOperationResponse:
    try:
        run, enqueued_task_id = pipeline_run_ops_service.requeue_run(
            db,
            tenant_id=tenant_context.tenant_id,
            run_id=run_id,
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Pipeline run not found") from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineRunOperationResponse(
        success=True,
        action="requeue",
        message="Pipeline run stage requeued.",
        run=run,
        source_run_id=run_id,
        enqueued_task_id=enqueued_task_id,
    )


@router.post(
    "/{run_id}/retry",
    response_model=PipelineRunOperationResponse,
    responses={
        400: {"description": "Invalid tenant header or action not allowed"},
        404: {"description": "Pipeline run not found"},
    },
)
def retry_pipeline_run(
    run_id: str,
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
) -> PipelineRunOperationResponse:
    try:
        run, enqueued_task_id = pipeline_run_ops_service.retry_run(
            db,
            tenant_id=tenant_context.tenant_id,
            run_id=run_id,
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Pipeline run not found") from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PipelineRunOperationResponse(
        success=True,
        action="retry",
        message="Pipeline run retry enqueued.",
        run=run,
        source_run_id=run_id,
        enqueued_task_id=enqueued_task_id,
    )


@router.get(
    "/{run_id}",
    response_model=PipelineRunDetailResponse,
    responses={
        400: {"description": "Invalid tenant header"},
        404: {"description": "Pipeline run not found"},
    },
)
def get_pipeline_run(
    run_id: str,
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
) -> PipelineRunDetailResponse:
    run = pipeline_run_read_service.get_run(
        db,
        tenant_id=tenant_context.tenant_id,
        run_id=run_id,
    )
    if run is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found")
    return PipelineRunDetailResponse(success=True, run=run)
