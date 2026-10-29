from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, Optional

from sqlalchemy import text

from core.metrics import (
    record_description_recovery_job,
    record_jobs_embedding_queued,
    record_jobs_extraction_queued,
)
from core.redis_streams import (
    STREAM_EMBEDDINGS_BATCH,
    STREAM_EXTRACTION_BATCH,
    STREAM_MATCHING,
    enqueue_job,
)
from database.context_signing import database_context_signing_enabled
from database.database import db_session_scope, worker_database_context
from database.repository import JobRepository
from services.orchestrator.description_recovery import recover_missing_description_jobs
from services.orchestrator.pipeline_runs import PipelineRunService

logger = logging.getLogger(__name__)


def _normalize_preference_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


def _load_candidate_preferences(repo: JobRepository, owner_id: str | None) -> dict[str, Any] | None:
    """Load hard preference fields without importing scorer-only modules."""
    if not owner_id:
        return None

    preferences = repo.candidate_preferences.get_preferences(owner_id)
    if preferences is None:
        return None

    return {
        "remote_mode": _normalize_preference_text(getattr(preferences, "remote_mode", ""))
        or "any",
        "target_locations": list(getattr(preferences, "target_locations", []) or []),
        "visa_sponsorship_required": bool(
            getattr(preferences, "visa_sponsorship_required", False)
        ),
        "salary_min": getattr(preferences, "salary_min", None),
        "employment_types": list(getattr(preferences, "employment_types", []) or []),
    }


def _stage_correlation(snapshot: dict[str, Any] | None, stage: str) -> dict[str, str]:
    """Extract stream correlation fields from a durable stage snapshot."""
    if not snapshot:
        return {}
    pipeline_run_id = snapshot.get("pipeline_run_id")
    stages = ((snapshot.get("result") or {}).get("stages") or [])
    pipeline_stage_id = None
    for stage_row in reversed(stages):
        if stage_row.get("stage") == stage:
            pipeline_stage_id = stage_row.get("id")
            break
    if not pipeline_run_id or not pipeline_stage_id:
        return {}
    return {
        "pipeline_run_id": str(pipeline_run_id),
        "pipeline_stage_id": str(pipeline_stage_id),
    }


def _coerce_job_ids(values: Any) -> list[Any]:
    if not isinstance(values, list):
        return []
    return [value for value in values if value]


def _ready_matching_targets() -> Iterator[Dict[str, Any]]:
    """Discover work through the restricted worker function when RLS is active."""
    if not database_context_signing_enabled():
        with db_session_scope() as db:
            repo = JobRepository(db)
            fingerprint = repo.get_latest_ready_resume_fingerprint()
            if not fingerprint:
                return
            resume = repo.get_structured_resume_by_fingerprint(fingerprint)
            target = {
                "resume_fingerprint": fingerprint,
                "owner_id": getattr(resume, "owner_id", None),
                "tenant_id": getattr(resume, "tenant_id", None),
            }
        yield target
        return

    cursor: Optional[str] = None
    while True:
        with db_session_scope() as db:
            targets = db.execute(
                text(
                    "SELECT * FROM public.jobscout_worker_ready_resume_candidates("
                    "NULL, NULL, 100, :cursor)"
                ),
                {"cursor": cursor},
            ).mappings().all()
        for target in targets:
            yield dict(target)
        if len(targets) < 100:
            return
        cursor = targets[-1]["cursor_value"]


def _enqueue_matching_repairs(task_id: str, correlation: Dict[str, str]) -> int:
    """Count and enqueue each resume's backlog under its owner and tenant."""
    queued = 0
    for index, target in enumerate(_ready_matching_targets()):
        owner_id = target.get("owner_id")
        tenant_id = target.get("tenant_id")
        fingerprint = target["resume_fingerprint"]
        with worker_database_context(user_id=owner_id, tenant_id=tenant_id):
            with db_session_scope() as db:
                repo = JobRepository(db)
                preferences = _load_candidate_preferences(
                    repo, str(owner_id) if owner_id is not None else None,
                )
                pending = repo.count_pending_matching_jobs(
                    fingerprint,
                    tenant_id=tenant_id,
                    candidate_preferences=preferences,
                )
        if not pending:
            continue
        payload = {
            "task_id": f"{task_id}-match-{index}",
            "resume_fingerprint": fingerprint,
            **correlation,
        }
        for key in ("owner_id", "tenant_id", "resume_upload_id"):
            if target.get(key) is not None:
                payload[key] = str(target[key])
        enqueue_job(STREAM_MATCHING, payload)
        queued += pending
    return queued


def run_stuck_job_repair(
    *,
    task_id: str,
    pipeline_runs: PipelineRunService,
    extraction_limit: int,
    embedding_limit: int,
    description_recovery_limit: int = 50,
) -> dict[str, Any]:
    """Requeue stale/retryable job processing work from durable DB state."""
    pipeline_runs.start_run(task_id=task_id, run_type="repair", current_stage="repair")
    stage_snapshot = pipeline_runs.start_stage(
        task_id=task_id,
        stage="repair",
        run_type="repair",
    )
    correlation = _stage_correlation(stage_snapshot, "repair")

    try:
        with db_session_scope() as db:
            repo = JobRepository(db)
            description_recovery_jobs = repo.claim_missing_description_recovery_jobs(
                limit=description_recovery_limit,
                run_id=task_id,
            )
            description_recovery_stats = recover_missing_description_jobs(
                repo,
                description_recovery_jobs,
                run_id=task_id,
            )
            recovered_job_ids = _coerce_job_ids(
                description_recovery_stats.get("description_found_job_ids")
            )
            recovered_extraction_jobs = (
                repo.job_post.claim_unextracted_jobs_by_ids_for_queue(
                    recovered_job_ids,
                    limit=description_recovery_limit,
                )
                if recovered_job_ids
                else []
            )
            extraction_jobs = repo.job_post.claim_unextracted_jobs_for_queue(limit=extraction_limit)
            embedding_jobs = repo.job_post.claim_unembedded_jobs_for_queue(limit=embedding_limit)

        enqueued = {
            "description_recovery_queued": int(description_recovery_stats.get("claimed", 0)),
            "description_recovery_processed": int(description_recovery_stats.get("processed", 0)),
            "description_recovery_found": int(description_recovery_stats.get("description_found", 0)),
            "description_recovery_posting_not_found": int(
                description_recovery_stats.get("posting_not_found", 0)
            ),
            "description_recovery_skipped": (
                int(description_recovery_stats.get("source_unsupported", 0))
                + int(description_recovery_stats.get("source_prohibited", 0))
                + int(description_recovery_stats.get("source_unmapped", 0))
                + int(description_recovery_stats.get("source_adapter_missing", 0))
            ),
            "description_recovery_failed_retryable": int(
                description_recovery_stats.get("failed_retryable", 0)
            ),
            "description_recovery_failed_terminal": int(
                description_recovery_stats.get("failed_terminal", 0)
            ),
            "description_recovery_source_adapter_missing": int(
                description_recovery_stats.get("source_adapter_missing", 0)
            ),
            "description_recovery_provider_breakdown": description_recovery_stats.get(
                "provider_breakdown",
                {},
            ),
            "extraction_queued": 0,
            "description_recovery_extraction_queued": 0,
            "embedding_queued": 0,
            "matching_queued": 0,
            "ready_unmatched_count": 0,
        }
        if enqueued["description_recovery_queued"]:
            record_description_recovery_job(
                "unmapped",
                "queued",
                enqueued["description_recovery_queued"],
            )

        if recovered_extraction_jobs:
            recovered_ids = [str(job.id) for job in recovered_extraction_jobs]
            enqueue_job(
                STREAM_EXTRACTION_BATCH,
                {
                    "task_id": f"{task_id}-extract-recovered",
                    "limit": len(recovered_ids),
                    "job_ids": recovered_ids,
                    "description_recovery_run_id": task_id,
                    **correlation,
                },
            )
            enqueued["extraction_queued"] += len(recovered_ids)
            enqueued["description_recovery_extraction_queued"] = len(recovered_ids)
            record_jobs_extraction_queued(len(recovered_ids))

        if extraction_jobs:
            enqueue_job(
                STREAM_EXTRACTION_BATCH,
                {
                    "task_id": f"{task_id}-extract",
                    "limit": min(len(extraction_jobs), extraction_limit),
                    **correlation,
                },
            )
            enqueued["extraction_queued"] += len(extraction_jobs)
            record_jobs_extraction_queued(len(extraction_jobs))

        if embedding_jobs:
            enqueue_job(
                STREAM_EMBEDDINGS_BATCH,
                {
                    "task_id": f"{task_id}-embed",
                    "limit": min(len(embedding_jobs), embedding_limit),
                    **correlation,
                },
            )
            enqueued["embedding_queued"] = len(embedding_jobs)
            record_jobs_embedding_queued(len(embedding_jobs))

        enqueued["matching_queued"] = _enqueue_matching_repairs(task_id, correlation)
        enqueued["ready_unmatched_count"] = enqueued["matching_queued"]

        pipeline_runs.complete_stage(
            task_id=task_id,
            stage="repair",
            run_type="repair",
            processed_count=(
                enqueued["description_recovery_processed"]
                + enqueued["extraction_queued"]
                + enqueued["embedding_queued"]
                + enqueued["matching_queued"]
            ),
            metadata=enqueued,
        )
        pipeline_runs.complete_run(task_id=task_id, run_type="repair", metadata=enqueued)
        logger.info("Stuck job repair completed: %s", enqueued)
        return enqueued
    except Exception as exc:
        pipeline_runs.fail_stage(
            task_id=task_id,
            stage="repair",
            run_type="repair",
            error=str(exc),
            retry_eligible=True,
        )
        pipeline_runs.fail_run(
            task_id=task_id,
            run_type="repair",
            error=str(exc),
            retry_eligible=True,
        )
        raise
