from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select

from database.models import (
    PIPELINE_RUN_CANCELLED,
    PIPELINE_RUN_COMPLETED,
    PIPELINE_RUN_FAILED,
    PIPELINE_RUN_PENDING,
    PIPELINE_RUN_RUNNING,
    PIPELINE_RUN_TERMINAL_STATUSES,
    PipelineRun,
    PipelineRunStage,
)
from database.repositories.base import BaseRepository


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _merge_metadata(current: dict[str, Any] | None, patch: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(current or {})
    if patch:
        merged.update(patch)
    return merged

_STAGE_ALIASES = {
    "extract": "extraction",
    "extracting": "resume_extraction",
    "resume_extract": "resume_extraction",
    "resume-extraction": "resume_extraction",
    "embed": "embedding",
    "embeddings": "embedding",
    "resume_embed": "resume_embedding",
    "resume-embedding": "resume_embedding",
    "match": "matching",
}

def normalize_pipeline_stage(stage: str) -> str:
    """Return the canonical durable pipeline stage label."""
    return _STAGE_ALIASES.get(stage, stage)


class PipelineRunRepository(BaseRepository):
    """Persistence adapter for durable pipeline run state."""

    def get_by_task_id(self, task_id: str) -> Optional[PipelineRun]:
        stmt = select(PipelineRun).where(PipelineRun.task_id == task_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_stage(self, run_id: uuid.UUID, stage: str) -> Optional[PipelineRunStage]:
        stage = normalize_pipeline_stage(stage)
        stmt = select(PipelineRunStage).where(
            PipelineRunStage.run_id == run_id,
            PipelineRunStage.stage == stage,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def _recompute_counts(self, run: PipelineRun) -> None:
        self.db.flush()
        stages = list(
            self.db.execute(
                select(PipelineRunStage).where(PipelineRunStage.run_id == run.id)
            ).scalars().all()
        )
        run.queued_count = sum(int(stage.queued_count or 0) for stage in stages)
        run.processed_count = sum(int(stage.processed_count or 0) for stage in stages)
        run.succeeded_count = sum(int(stage.succeeded_count or 0) for stage in stages)
        run.failed_count = sum(int(stage.failed_count or 0) for stage in stages)
        run.skipped_count = sum(int(stage.skipped_count or 0) for stage in stages)

    @staticmethod
    def _can_run(run: PipelineRun) -> bool:
        return run.status not in PIPELINE_RUN_TERMINAL_STATUSES

    def create_run(
        self,
        *,
        task_id: str,
        run_type: str,
        owner_id: Any | None = None,
        tenant_id: Any | None = None,
        resume_fingerprint: str | None = None,
        current_stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRun:
        now = _utc_now()
        run = PipelineRun(
            task_id=task_id,
            run_type=run_type,
            owner_id=owner_id,
            tenant_id=tenant_id,
            resume_fingerprint=resume_fingerprint,
            status=PIPELINE_RUN_RUNNING,
            current_stage=normalize_pipeline_stage(current_stage) if current_stage else None,
            started_at=now,
            heartbeat_at=now,
            metadata_json=metadata or {},
        )
        self.db.add(run)
        self.db.flush()
        return run

    def get_or_create_run(
        self,
        *,
        task_id: str,
        run_type: str,
        owner_id: Any | None = None,
        tenant_id: Any | None = None,
        resume_fingerprint: str | None = None,
        current_stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRun:
        existing = self.get_by_task_id(task_id)
        if existing is not None:
            if existing.status == PIPELINE_RUN_PENDING:
                existing.status = PIPELINE_RUN_RUNNING
                existing.started_at = existing.started_at or _utc_now()
            if current_stage is not None and self._can_run(existing):
                existing.current_stage = normalize_pipeline_stage(current_stage)
            existing.heartbeat_at = _utc_now()
            existing.metadata_json = _merge_metadata(existing.metadata_json, metadata)
            self.db.flush()
            return existing
        return self.create_run(
            task_id=task_id,
            run_type=run_type,
            owner_id=owner_id,
            tenant_id=tenant_id,
            resume_fingerprint=resume_fingerprint,
            current_stage=current_stage,
            metadata=metadata,
        )

    def touch_run(
        self,
        run: PipelineRun,
        *,
        current_stage: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRun:
        run.heartbeat_at = _utc_now()
        if current_stage is not None and self._can_run(run):
            run.current_stage = normalize_pipeline_stage(current_stage)
        run.metadata_json = _merge_metadata(run.metadata_json, metadata)
        self.db.flush()
        return run

    def start_stage(
        self,
        run: PipelineRun,
        *,
        stage: str,
        queued_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRunStage:
        stage = normalize_pipeline_stage(stage)
        now = _utc_now()
        stage_row = self.get_stage(run.id, stage)
        if stage_row is None:
            stage_row = PipelineRunStage(
                run_id=run.id,
                stage=stage,
                status=PIPELINE_RUN_RUNNING,
                queued_count=queued_count,
                started_at=now,
                metadata_json=metadata or {},
            )
            self.db.add(stage_row)
        else:
            if stage_row.status != PIPELINE_RUN_COMPLETED:
                stage_row.status = PIPELINE_RUN_RUNNING
            stage_row.queued_count = max(stage_row.queued_count or 0, queued_count)
            stage_row.started_at = stage_row.started_at or now
            if stage_row.status == PIPELINE_RUN_RUNNING:
                stage_row.completed_at = None
            stage_row.last_error = None
            stage_row.metadata_json = _merge_metadata(stage_row.metadata_json, metadata)
        if self._can_run(run):
            run.status = PIPELINE_RUN_RUNNING
            run.current_stage = stage
        run.heartbeat_at = now
        self._recompute_counts(run)
        self.db.flush()
        return stage_row

    def complete_stage(
        self,
        run: PipelineRun,
        *,
        stage: str,
        processed_count: int = 0,
        succeeded_count: int | None = None,
        failed_count: int = 0,
        skipped_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRunStage:
        stage = normalize_pipeline_stage(stage)
        stage_row = self.start_stage(run, stage=stage, metadata=metadata)
        now = _utc_now()
        stage_row.status = PIPELINE_RUN_COMPLETED
        stage_row.processed_count = processed_count
        stage_row.succeeded_count = processed_count if succeeded_count is None else succeeded_count
        stage_row.failed_count = failed_count
        stage_row.skipped_count = skipped_count
        stage_row.retry_eligible = False
        stage_row.completed_at = now
        stage_row.last_error = None
        stage_row.metadata_json = _merge_metadata(stage_row.metadata_json, metadata)
        self._recompute_counts(run)
        run.heartbeat_at = now
        self.db.flush()
        return stage_row

    def fail_stage(
        self,
        run: PipelineRun,
        *,
        stage: str,
        error: str,
        retry_eligible: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRunStage:
        stage = normalize_pipeline_stage(stage)
        stage_row = self.start_stage(run, stage=stage, metadata=metadata)
        now = _utc_now()
        stage_row.status = PIPELINE_RUN_FAILED
        stage_row.failed_count = max(stage_row.failed_count or 0, 1)
        stage_row.retry_eligible = retry_eligible
        stage_row.last_error = error
        stage_row.completed_at = now
        stage_row.metadata_json = _merge_metadata(stage_row.metadata_json, metadata)
        self._recompute_counts(run)
        if run.status != PIPELINE_RUN_CANCELLED:
            run.status = PIPELINE_RUN_FAILED
        run.last_error = error
        run.retry_eligible = retry_eligible
        run.heartbeat_at = now
        self.db.flush()
        return stage_row

    def complete_run(
        self,
        run: PipelineRun,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRun:
        now = _utc_now()
        if run.status not in {PIPELINE_RUN_FAILED, PIPELINE_RUN_CANCELLED}:
            run.status = PIPELINE_RUN_COMPLETED
            run.completed_at = now
            run.retry_eligible = False
        run.heartbeat_at = now
        run.metadata_json = _merge_metadata(run.metadata_json, metadata)
        self.db.flush()
        return run

    def fail_run(
        self,
        run: PipelineRun,
        *,
        error: str,
        retry_eligible: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRun:
        now = _utc_now()
        if run.status != PIPELINE_RUN_CANCELLED:
            run.status = PIPELINE_RUN_FAILED
            run.completed_at = now
            run.retry_eligible = retry_eligible
            run.last_error = error
        run.heartbeat_at = now
        run.metadata_json = _merge_metadata(run.metadata_json, metadata)
        self.db.flush()
        return run

    def cancel_run(
        self,
        run: PipelineRun,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> PipelineRun:
        now = _utc_now()
        if run.status != PIPELINE_RUN_COMPLETED:
            run.status = PIPELINE_RUN_CANCELLED
            run.completed_at = now
        run.heartbeat_at = now
        run.metadata_json = _merge_metadata(run.metadata_json, metadata)
        self.db.flush()
        return run
