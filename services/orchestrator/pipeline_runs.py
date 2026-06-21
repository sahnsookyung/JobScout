from __future__ import annotations

import logging
import uuid
from typing import Any

from database.database import db_session_scope
from database.models import PipelineRun, PipelineRunStage
from database.repositories.pipeline_run import PipelineRunRepository
from services.orchestrator.redis_gateway import RedisTaskStateGateway

logger = logging.getLogger(__name__)


def _coerce_uuid(value: Any | None) -> uuid.UUID | None:
    if value is None or isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


class PipelineRunService:
    """Durable pipeline-run use case with Redis task-state projection."""

    def __init__(self, *, redis_gateway: RedisTaskStateGateway | None = None) -> None:
        self.redis_gateway = redis_gateway or RedisTaskStateGateway()

    @staticmethod
    def _stage_snapshot(stage: PipelineRunStage) -> dict[str, Any]:
        return {
            "id": str(stage.id),
            "stage": stage.stage,
            "status": stage.status,
            "queued_count": stage.queued_count,
            "processed_count": stage.processed_count,
            "succeeded_count": stage.succeeded_count,
            "failed_count": stage.failed_count,
            "skipped_count": stage.skipped_count,
            "retry_count": stage.retry_count,
            "retry_eligible": stage.retry_eligible,
            "last_error": stage.last_error,
            "started_at": stage.started_at.isoformat() if stage.started_at else None,
            "completed_at": stage.completed_at.isoformat() if stage.completed_at else None,
            "metadata": stage.metadata_json or {},
        }

    @classmethod
    def _run_snapshot(cls, run: PipelineRun) -> dict[str, Any]:
        stages = [cls._stage_snapshot(stage) for stage in list(run.stages or [])]
        metadata = run.metadata_json or {}
        snapshot = {
            "success": True,
            "task_id": run.task_id,
            "status": run.status,
            "task_type": run.run_type,
            "current_stage": run.current_stage,
            "phase": run.current_stage,
            "pipeline_run_id": str(run.id),
            "metadata": metadata,
            "owner_id": str(run.owner_id) if run.owner_id else None,
            "tenant_id": str(run.tenant_id) if run.tenant_id else None,
            "resume_fingerprint": run.resume_fingerprint,
            "retry_eligible": run.retry_eligible,
            "error": run.last_error,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "heartbeat_at": run.heartbeat_at.isoformat() if run.heartbeat_at else None,
            "result": {
                **metadata,
                "queued_count": run.queued_count,
                "processed_count": run.processed_count,
                "succeeded_count": run.succeeded_count,
                "failed_count": run.failed_count,
                "skipped_count": run.skipped_count,
                "stages": stages,
            },
        }
        for key in (
            "step",
            "phase",
            "upload_id",
            "resume_file",
            "matches_count",
            "saved_count",
            "notified_count",
            "updated_at",
            "stale_due_to_newer_upload",
            "latest_upload_id",
            "latest_resume_fingerprint",
            "stale_message",
        ):
            if key in metadata:
                snapshot[key] = metadata[key]
        return snapshot

    def _project(self, snapshot: dict[str, Any]) -> None:
        task_id = str(snapshot["task_id"])
        try:
            self.redis_gateway.set_task_state(task_id, snapshot)
        except Exception:
            logger.warning("Failed to project pipeline run %s to Redis", task_id, exc_info=True)

    @staticmethod
    def _get_or_create(
        repo: PipelineRunRepository,
        *,
        task_id: str,
        run_type: str,
        owner_id: Any | None = None,
        tenant_id: Any | None = None,
        resume_fingerprint: str | None = None,
        current_stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRun:
        return repo.get_or_create_run(
            task_id=task_id,
            run_type=run_type,
            owner_id=_coerce_uuid(owner_id),
            tenant_id=_coerce_uuid(tenant_id),
            resume_fingerprint=resume_fingerprint,
            current_stage=current_stage,
            metadata=metadata,
        )

    def get_snapshot(self, task_id: str) -> dict[str, Any] | None:
        with db_session_scope() as db:
            run = PipelineRunRepository(db).get_by_task_id(task_id)
            if run is None:
                return None
            return self._run_snapshot(run)

    def start_run(
        self,
        *,
        task_id: str,
        run_type: str,
        owner_id: Any | None = None,
        tenant_id: Any | None = None,
        resume_fingerprint: str | None = None,
        current_stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with db_session_scope() as db:
            repo = PipelineRunRepository(db)
            run = self._get_or_create(
                repo,
                task_id=task_id,
                run_type=run_type,
                owner_id=owner_id,
                tenant_id=tenant_id,
                resume_fingerprint=resume_fingerprint,
                current_stage=current_stage,
                metadata=metadata,
            )
            snapshot = self._run_snapshot(run)
        self._project(snapshot)
        return snapshot

    def touch_run(
        self,
        *,
        task_id: str,
        run_type: str = "pipeline",
        current_stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with db_session_scope() as db:
            repo = PipelineRunRepository(db)
            run = self._get_or_create(repo, task_id=task_id, run_type=run_type)
            repo.touch_run(run, current_stage=current_stage, metadata=metadata)
            snapshot = self._run_snapshot(run)
        self._project(snapshot)
        return snapshot

    def start_stage(
        self,
        *,
        task_id: str,
        stage: str,
        run_type: str = "pipeline",
        queued_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with db_session_scope() as db:
            repo = PipelineRunRepository(db)
            run = self._get_or_create(repo, task_id=task_id, run_type=run_type, current_stage=stage)
            repo.start_stage(run, stage=stage, queued_count=queued_count, metadata=metadata)
            snapshot = self._run_snapshot(run)
        self._project(snapshot)
        return snapshot

    def complete_stage(
        self,
        *,
        task_id: str,
        stage: str,
        run_type: str = "pipeline",
        processed_count: int = 0,
        succeeded_count: int | None = None,
        failed_count: int = 0,
        skipped_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with db_session_scope() as db:
            repo = PipelineRunRepository(db)
            run = self._get_or_create(repo, task_id=task_id, run_type=run_type, current_stage=stage)
            repo.complete_stage(
                run,
                stage=stage,
                processed_count=processed_count,
                succeeded_count=succeeded_count,
                failed_count=failed_count,
                skipped_count=skipped_count,
                metadata=metadata,
            )
            snapshot = self._run_snapshot(run)
        self._project(snapshot)
        return snapshot

    def fail_stage(
        self,
        *,
        task_id: str,
        stage: str,
        error: str,
        run_type: str = "pipeline",
        retry_eligible: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with db_session_scope() as db:
            repo = PipelineRunRepository(db)
            run = self._get_or_create(repo, task_id=task_id, run_type=run_type, current_stage=stage)
            repo.fail_stage(
                run,
                stage=stage,
                error=error,
                retry_eligible=retry_eligible,
                metadata=metadata,
            )
            snapshot = self._run_snapshot(run)
        self._project(snapshot)
        return snapshot

    def complete_run(
        self,
        *,
        task_id: str,
        run_type: str = "pipeline",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with db_session_scope() as db:
            repo = PipelineRunRepository(db)
            run = self._get_or_create(repo, task_id=task_id, run_type=run_type)
            repo.complete_run(run, metadata=metadata)
            snapshot = self._run_snapshot(run)
        self._project(snapshot)
        return snapshot

    def fail_run(
        self,
        *,
        task_id: str,
        error: str,
        run_type: str = "pipeline",
        retry_eligible: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with db_session_scope() as db:
            repo = PipelineRunRepository(db)
            run = self._get_or_create(repo, task_id=task_id, run_type=run_type)
            repo.fail_run(run, error=error, retry_eligible=retry_eligible, metadata=metadata)
            snapshot = self._run_snapshot(run)
        self._project(snapshot)
        return snapshot

    def cancel_run(self, *, task_id: str, run_type: str = "pipeline") -> dict[str, Any]:
        with db_session_scope() as db:
            repo = PipelineRunRepository(db)
            run = self._get_or_create(repo, task_id=task_id, run_type=run_type)
            repo.cancel_run(run)
            snapshot = self._run_snapshot(run)
        self._project(snapshot)
        return snapshot
