"""Read service for durable pipeline runs."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from database.models import PipelineRun, PipelineRunStage
from web.backend.models.responses import PipelineRunStageSummary, PipelineRunSummary

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
REQUEUEABLE_STAGES = {"extraction", "embedding", "matching"}


def _isoformat(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def pipeline_run_stage_summary(stage: PipelineRunStage) -> PipelineRunStageSummary:
    return PipelineRunStageSummary(
        id=str(stage.id),
        stage=stage.stage,
        status=stage.status,
        queued_count=int(stage.queued_count or 0),
        processed_count=int(stage.processed_count or 0),
        succeeded_count=int(stage.succeeded_count or 0),
        failed_count=int(stage.failed_count or 0),
        skipped_count=int(stage.skipped_count or 0),
        retry_count=int(stage.retry_count or 0),
        retry_eligible=bool(stage.retry_eligible),
        last_error=stage.last_error,
        started_at=_isoformat(stage.started_at),
        completed_at=_isoformat(stage.completed_at),
        metadata=stage.metadata_json or {},
    )


def pipeline_run_summary(run: PipelineRun) -> PipelineRunSummary:
    return PipelineRunSummary(
        id=str(run.id),
        task_id=run.task_id,
        run_type=run.run_type,
        status=run.status,
        current_stage=run.current_stage,
        queued_count=int(run.queued_count or 0),
        processed_count=int(run.processed_count or 0),
        succeeded_count=int(run.succeeded_count or 0),
        failed_count=int(run.failed_count or 0),
        skipped_count=int(run.skipped_count or 0),
        retry_eligible=bool(run.retry_eligible),
        last_error=run.last_error,
        owner_id=str(run.owner_id) if run.owner_id else None,
        tenant_id=str(run.tenant_id) if run.tenant_id else None,
        resume_fingerprint=run.resume_fingerprint,
        started_at=_isoformat(run.started_at),
        completed_at=_isoformat(run.completed_at),
        heartbeat_at=_isoformat(run.heartbeat_at),
        created_at=_isoformat(run.created_at),
        updated_at=_isoformat(run.updated_at),
        metadata=run.metadata_json or {},
        stages=[pipeline_run_stage_summary(stage) for stage in list(run.stages or [])],
        allowed_actions=pipeline_run_allowed_actions(run),
    )

def _latest_failed_or_retryable_stage(run: PipelineRun) -> str | None:
    stages = list(run.stages or [])
    for stage in reversed(stages):
        if stage.status == "failed" or stage.retry_eligible:
            return stage.stage
    return run.current_stage

def pipeline_run_allowed_actions(run: PipelineRun) -> list[str]:
    actions: list[str] = []
    if run.status in {"pending", "running"}:
        actions.append("cancel")
    target_stage = _latest_failed_or_retryable_stage(run)
    if run.retry_eligible and run.status in TERMINAL_STATUSES:
        actions.append("retry")
    if run.retry_eligible and target_stage in REQUEUEABLE_STAGES:
        actions.append("requeue")
    return actions


class PipelineRunReadService:
    """Query durable pipeline runs for API presentation."""

    def list_runs(
        self,
        db: Session,
        *,
        tenant_id: Any,
        status: str,
        run_type: str,
        limit: int,
        offset: int,
    ) -> tuple[list[PipelineRunSummary], int]:
        filters = []
        filters.append(PipelineRun.tenant_id.is_(None) if tenant_id is None else PipelineRun.tenant_id == tenant_id)
        if status != "all":
            filters.append(PipelineRun.status == status)
        if run_type != "all":
            filters.append(PipelineRun.run_type == run_type)

        total = int(db.execute(select(func.count(PipelineRun.id)).where(*filters)).scalar_one() or 0)
        stmt = (
            select(PipelineRun)
            .options(selectinload(PipelineRun.stages))
            .where(*filters)
            .order_by(PipelineRun.created_at.desc(), PipelineRun.id.desc())
            .offset(offset)
            .limit(limit)
        )
        runs = db.execute(stmt).scalars().all()
        return [pipeline_run_summary(run) for run in runs], total

    def get_run(
        self,
        db: Session,
        *,
        tenant_id: Any,
        run_id: str,
    ) -> PipelineRunSummary | None:
        try:
            lookup_id = uuid.UUID(str(run_id))
        except (TypeError, ValueError):
            return None
        filters = [PipelineRun.id == lookup_id]
        filters.append(PipelineRun.tenant_id.is_(None) if tenant_id is None else PipelineRun.tenant_id == tenant_id)
        run = db.execute(
            select(PipelineRun)
            .options(selectinload(PipelineRun.stages))
            .where(*filters)
        ).scalar_one_or_none()
        return pipeline_run_summary(run) if run is not None else None


pipeline_run_read_service = PipelineRunReadService()
