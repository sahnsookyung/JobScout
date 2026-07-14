"""Operator actions for durable pipeline runs."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from core.llm.provider_health import (
    configured_llm_provider_status,
    reset_llm_provider_circuit,
    run_llm_provider_canaries,
)
from core.llm_evaluation_queue import (
    check_llm_evaluation_queue_readiness,
    enqueue_stale_or_retryable_evaluations,
    resume_llm_evaluation_queue,
    set_llm_evaluation_queue_paused,
)
from core.metrics import record_llm_evaluation_queue_operator_action
from core.redis_streams import (
    STREAM_EMBEDDINGS_BATCH,
    STREAM_EXTRACTION_BATCH,
    STREAM_MATCHING,
    enqueue_job,
    set_task_cancellation_requested,
)
from database.models import PipelineRun
from database.repositories.pipeline_run import PipelineRunRepository
from web.backend.models.responses import PipelineRunSummary
from web.backend.services.pipeline_run_service import (
    pipeline_run_allowed_actions,
    pipeline_run_summary,
)

REQUEUE_STAGE_STREAMS = {
    "extraction": STREAM_EXTRACTION_BATCH,
    "embedding": STREAM_EMBEDDINGS_BATCH,
    "matching": STREAM_MATCHING,
}

def _tenant_filter(tenant_id: Any):
    return PipelineRun.tenant_id.is_(None) if tenant_id is None else PipelineRun.tenant_id == tenant_id

def _coerce_run_id(run_id: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(run_id))
    except (TypeError, ValueError):
        return None

def _target_stage(run: PipelineRun) -> str | None:
    stages = list(run.stages or [])
    for stage in reversed(stages):
        if stage.status == "failed" or stage.retry_eligible:
            return stage.stage
    return run.current_stage

def _stage_limit(run: PipelineRun, stage: str) -> int:
    for stage_row in reversed(list(run.stages or [])):
        if stage_row.stage == stage and stage_row.queued_count:
            return int(stage_row.queued_count)
    return max(int(run.queued_count or 0), 1)

class PipelineRunOpsService:
    """Execute explicit operator actions against durable pipeline runs."""

    def get_run_model(
        self,
        db: Session,
        *,
        owner_id: Any,
        tenant_id: Any,
        run_id: str,
    ) -> PipelineRun | None:
        lookup_id = _coerce_run_id(run_id)
        if lookup_id is None:
            return None
        return db.execute(
            select(PipelineRun)
            .options(selectinload(PipelineRun.stages))
            .where(
                PipelineRun.id == lookup_id,
                PipelineRun.owner_id == owner_id,
                _tenant_filter(tenant_id),
            )
        ).scalar_one_or_none()

    def cancel_run(
        self,
        db: Session,
        *,
        owner_id: Any,
        tenant_id: Any,
        run_id: str,
    ) -> PipelineRunSummary:
        run = self.get_run_model(
            db,
            owner_id=owner_id,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        if run is None:
            raise LookupError("Pipeline run not found")
        if "cancel" not in pipeline_run_allowed_actions(run):
            raise ValueError("Pipeline run cannot be cancelled from its current state.")

        repo = PipelineRunRepository(db)
        metadata = {"operator_action": "cancel", "runtime_cancel_requested": False}
        try:
            set_task_cancellation_requested(run.task_id, ttl=3600)
            metadata["runtime_cancel_requested"] = True
        except Exception as exc:
            metadata["runtime_cancel_error"] = str(exc)

        repo.cancel_run(run, metadata=metadata)
        db.commit()
        return pipeline_run_summary(run)

    def requeue_run(
        self,
        db: Session,
        *,
        owner_id: Any,
        tenant_id: Any,
        run_id: str,
        action: str = "requeue",
    ) -> tuple[PipelineRunSummary, str]:
        source = self.get_run_model(
            db,
            owner_id=owner_id,
            tenant_id=tenant_id,
            run_id=run_id,
        )
        if source is None:
            raise LookupError("Pipeline run not found")
        if action not in pipeline_run_allowed_actions(source):
            action_label = "retried" if action == "retry" else "requeued"
            raise ValueError(f"Pipeline run cannot be {action_label} from its current state.")

        stage = _target_stage(source)
        if stage not in REQUEUE_STAGE_STREAMS:
            raise ValueError("Pipeline run does not have a requeueable current stage.")
        if stage == "matching" and not source.resume_fingerprint:
            raise ValueError("Matching requeue requires a resume fingerprint.")

        repo = PipelineRunRepository(db)
        new_task_id = f"{source.task_id}-{action}-{uuid.uuid4().hex[:8]}"
        retry_run = repo.create_run(
            task_id=new_task_id,
            run_type=source.run_type,
            owner_id=source.owner_id,
            tenant_id=source.tenant_id,
            resume_fingerprint=source.resume_fingerprint,
            current_stage=stage,
            metadata={
                "operator_action": action,
                "source_pipeline_run_id": str(source.id),
                "source_task_id": source.task_id,
            },
        )
        stage_row = repo.start_stage(
            retry_run,
            stage=stage,
            queued_count=_stage_limit(source, stage),
            metadata={"operator_action": action},
        )
        payload: dict[str, Any] = {
            "task_id": new_task_id,
            "pipeline_run_id": str(retry_run.id),
            "pipeline_stage_id": str(stage_row.id),
            "owner_id": str(source.owner_id),
            "tenant_id": str(source.tenant_id) if source.tenant_id else None,
        }
        if stage in {"extraction", "embedding"}:
            payload["limit"] = _stage_limit(source, stage)
        if stage == "matching":
            payload["resume_fingerprint"] = source.resume_fingerprint

        enqueue_job(REQUEUE_STAGE_STREAMS[stage], payload)
        db.commit()
        return pipeline_run_summary(retry_run), new_task_id

    def retry_run(
        self,
        db: Session,
        *,
        owner_id: Any,
        tenant_id: Any,
        run_id: str,
    ) -> tuple[PipelineRunSummary, str]:
        return self.requeue_run(
            db,
            owner_id=owner_id,
            tenant_id=tenant_id,
            run_id=run_id,
            action="retry",
        )

    def llm_queue_status(self) -> dict[str, Any]:
        try:
            status = check_llm_evaluation_queue_readiness()
            return {"success": True, **status}
        except Exception as exc:
            return {
                "success": False,
                "ready": False,
                "queue": "llm_evaluations",
                "queued": 0,
                "started": 0,
                "deferred": 0,
                "scheduled": 0,
                "failed": 0,
                "db_pending": 0,
                "db_running": 0,
                "db_failed": 0,
                "db_retryable_failed": 0,
                "oldest_pending_age_seconds": None,
                "oldest_retryable_failed_age_seconds": None,
                "drain_estimate_seconds": None,
                "paused": False,
                "pause_reason": None,
                "pause_ttl_seconds": None,
                "error": str(exc),
            }

    def pause_llm_queue(
        self,
        *,
        reason: str | None = None,
        ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        set_llm_evaluation_queue_paused(reason=reason, ttl_seconds=ttl_seconds)
        record_llm_evaluation_queue_operator_action("pause")
        return self.llm_queue_status()

    def resume_llm_queue(self) -> dict[str, Any]:
        resume_llm_evaluation_queue()
        record_llm_evaluation_queue_operator_action("resume")
        return self.llm_queue_status()

    def retry_llm_queue(self, *, limit: int = 100) -> tuple[dict[str, Any], int]:
        enqueued = enqueue_stale_or_retryable_evaluations(limit=limit)
        record_llm_evaluation_queue_operator_action("retry")
        return self.llm_queue_status(), enqueued

    def llm_provider_status(self) -> dict[str, Any]:
        return configured_llm_provider_status()

    def run_llm_provider_canaries(self) -> dict[str, Any]:
        return run_llm_provider_canaries()

    def reset_llm_provider_circuit(
        self,
        *,
        provider: str,
        model: str,
    ) -> dict[str, Any]:
        return reset_llm_provider_circuit(provider=provider, model=model)

pipeline_run_ops_service = PipelineRunOpsService()
