"""Durable pipeline run operations endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..dependencies import TenantContext, get_current_user, get_db, get_tenant_context
from ..models.responses import (
    LlmEvaluationQueueStatusResponse,
    PipelineRunDetailResponse,
    PipelineRunOperationResponse,
    PipelineRunsResponse,
)
from ..services.pipeline_run_ops_service import pipeline_run_ops_service
from ..services.pipeline_run_service import pipeline_run_read_service

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

    runs, total = pipeline_run_read_service.list_runs(
        db,
        tenant_id=tenant_context.tenant_id,
        status=status,
        run_type=run_type,
        limit=limit,
        offset=offset,
    )
    return PipelineRunsResponse(
        success=True,
        count=len(runs),
        total=total,
        limit=limit,
        offset=offset,
        runs=runs,
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
