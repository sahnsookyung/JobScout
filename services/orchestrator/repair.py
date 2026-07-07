from __future__ import annotations

import logging
from typing import Any

from core.metrics import record_jobs_embedding_queued, record_jobs_extraction_queued
from core.redis_streams import (
    STREAM_EMBEDDINGS_BATCH,
    STREAM_EXTRACTION_BATCH,
    STREAM_MATCHING,
    enqueue_job,
)
from database.database import db_session_scope
from database.repository import JobRepository
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


def run_stuck_job_repair(
    *,
    task_id: str,
    pipeline_runs: PipelineRunService,
    extraction_limit: int,
    embedding_limit: int,
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
            extraction_jobs = repo.job_post.claim_unextracted_jobs_for_queue(limit=extraction_limit)
            embedding_jobs = repo.job_post.claim_unembedded_jobs_for_queue(limit=embedding_limit)
            latest_resume_fingerprint = repo.get_latest_ready_resume_fingerprint()
            ready_unmatched_count = 0
            matching_owner_id: str | None = None
            if latest_resume_fingerprint:
                structured_resume = repo.get_structured_resume_by_fingerprint(
                    latest_resume_fingerprint
                )
                owner_id = getattr(structured_resume, "owner_id", None)
                tenant_id = getattr(structured_resume, "tenant_id", None)
                matching_owner_id = str(owner_id) if owner_id is not None else None
                candidate_preferences = _load_candidate_preferences(repo, matching_owner_id)
                ready_unmatched_count = repo.count_pending_matching_jobs(
                    latest_resume_fingerprint,
                    tenant_id=tenant_id,
                    candidate_preferences=candidate_preferences,
                )

        enqueued = {
            "extraction_queued": 0,
            "embedding_queued": 0,
            "matching_queued": 0,
            "ready_unmatched_count": ready_unmatched_count,
        }

        if extraction_jobs:
            enqueue_job(
                STREAM_EXTRACTION_BATCH,
                {
                    "task_id": f"{task_id}-extract",
                    "limit": min(len(extraction_jobs), extraction_limit),
                    **correlation,
                },
            )
            enqueued["extraction_queued"] = len(extraction_jobs)
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

        if latest_resume_fingerprint and ready_unmatched_count:
            matching_payload = {
                "task_id": f"{task_id}-match",
                "resume_fingerprint": latest_resume_fingerprint,
                **correlation,
            }
            if matching_owner_id is not None:
                matching_payload["owner_id"] = matching_owner_id
            enqueue_job(
                STREAM_MATCHING,
                matching_payload,
            )
            enqueued["matching_queued"] = ready_unmatched_count

        pipeline_runs.complete_stage(
            task_id=task_id,
            stage="repair",
            run_type="repair",
            processed_count=(
                enqueued["extraction_queued"]
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
