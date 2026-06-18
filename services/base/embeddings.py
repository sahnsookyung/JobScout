"""
Embeddings module - handles vector generation.

This module provides embedding functionality for the embeddings
microservice and shared embedding helpers used by the current runtime.
"""

import logging
import threading
from typing import Any

from core.app_context import AppContext
from database.uow import job_uow
from database.models import SYSTEM_OWNER_ID

logger = logging.getLogger(__name__)


def _as_embedding_text(value: Any, *, max_chars: int = 2000) -> str:
    """Convert common job metadata values into compact embedding text."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()[:max_chars]
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        items = [
            _as_embedding_text(item, max_chars=max_chars)
            for item in value
        ]
        return ", ".join(item for item in items if item)[:max_chars]
    if isinstance(value, dict):
        selected_keys = (
            "title",
            "company",
            "location",
            "location_text",
            "job_type",
            "job_level",
            "seniority",
            "skills",
            "tags",
            "requirements",
            "benefits",
            "description",
            "summary",
        )
        items = [
            _as_embedding_text(value.get(key), max_chars=max_chars)
            for key in selected_keys
        ]
        return " | ".join(item for item in items if item)[:max_chars]
    return ""


def _append_part(parts: list[str], label: str, value: Any, *, max_chars: int = 2000) -> None:
    text = _as_embedding_text(value, max_chars=max_chars)
    if text:
        parts.append(f"{label}: {text}")


def _build_job_embedding_text(job) -> str:
    """Build an embedding payload from job-card metadata and richer content."""
    parts: list[str] = []
    _append_part(parts, "Title", getattr(job, "title", None), max_chars=300)
    _append_part(parts, "Company", getattr(job, "company", None), max_chars=300)
    _append_part(parts, "Location", getattr(job, "location_text", None), max_chars=300)
    if getattr(job, "is_remote", None) is True:
        parts.append("Work mode: Remote")
    _append_part(parts, "Job type", getattr(job, "job_type", None), max_chars=200)
    _append_part(parts, "Level", getattr(job, "job_level", None), max_chars=200)
    _append_part(parts, "Experience", getattr(job, "experience_range", None), max_chars=300)
    _append_part(parts, "Skills", getattr(job, "skills_raw", None), max_chars=1000)
    _append_part(
        parts,
        "Canonical summary",
        getattr(job, "canonical_job_summary", None),
        max_chars=1200,
    )
    if job.requirements:
        req_text = [getattr(r, "text", "") for r in job.requirements[:20]]
        _append_part(parts, "Requirements", req_text, max_chars=3000)
    if job.benefits:
        benefit_text = [getattr(b, "text", "") for b in job.benefits[:10]]
        _append_part(parts, "Benefits", benefit_text, max_chars=1500)
    _append_part(parts, "Description", getattr(job, "description", None), max_chars=5000)
    _append_part(
        parts,
        "Company description",
        getattr(job, "company_description", None),
        max_chars=1000,
    )
    _append_part(parts, "Source metadata", getattr(job, "raw_payload", None), max_chars=2000)
    return " | ".join(parts)


def _collect_job_embedding_data(limit: int) -> list[tuple[object, str]]:
    """Collect job ids and texts while the session is open."""
    job_data: list[tuple[object, str]] = []
    with job_uow() as repo:
        jobs = list(repo.get_unembedded_jobs(limit))
        if jobs:
            repo.bulk_mark_embedding_in_progress([j.id for j in jobs])
        for job in jobs:
            job_data.append((job.id, _build_job_embedding_text(job)))
    return job_data


def _collect_requirement_embedding_data(limit: int) -> list[tuple[object, str]]:
    """Collect requirement ids and texts while the session is open."""
    req_data: list[tuple[object, str]] = []
    with job_uow() as repo:
        for req in repo.get_unembedded_requirements(limit):
            req_data.append((req.id, req.text))
    return req_data


def _mark_job_embedding_batch_failed(job_ids: tuple[object, ...], error: str) -> None:
    """Persist retryable failure metadata for every job in a failed batch."""
    for job_id in job_ids:
        try:
            with job_uow() as repo:
                repo.mark_embedding_retryable_failed(job_id, error)
        except Exception:
            logger.warning(
                "Failed to persist embedding failure for job_id=%s",
                job_id,
                exc_info=True,
            )


def _save_job_embedding(job_id: object, vector) -> bool:
    """Write a single job embedding back to the database."""
    with job_uow() as repo:
        job_fresh = repo.get_by_id(job_id)
        if job_fresh is None:
            logger.warning(f"Job {job_id} not found, may have been deleted")
            return False
        repo.save_job_embedding(job_fresh, vector)
    return True


def _persist_job_embedding_failure(job_id: object, exc: Exception) -> None:
    """Persist a retryable failure for a job embedding write-back."""
    try:
        with job_uow() as repo:
            # Empty/invalid provider responses and rate limits should remain
            # unprocessed so queue-first reconciliation can retry later.
            repo.mark_embedding_retryable_failed(job_id, f"{type(exc).__name__}: {exc}")
    except Exception:
        logger.warning(
            "Failed to persist embedding failure for job_id=%s",
            job_id,
            exc_info=True,
        )


def _process_job_embedding_batch(
    ctx: AppContext,
    stop_event: threading.Event,
    job_data: list[tuple[object, str]],
) -> int:
    """Generate and persist job embeddings for one batch."""
    if not job_data or stop_event.is_set():
        return 0

    job_ids, job_texts = zip(*job_data)
    try:
        vectors = ctx.job_etl_service.ai.generate_embeddings_batch(list(job_texts))
    except Exception as exc:
        logger.exception("Batch job embedding API failed")
        _mark_job_embedding_batch_failed(job_ids, f"{type(exc).__name__}: {exc}")
        return 0

    job_success = 0
    for job_id, vector in zip(job_ids, vectors):
        if stop_event.is_set():
            break
        try:
            if _save_job_embedding(job_id, vector):
                job_success += 1
        except Exception as exc:
            _persist_job_embedding_failure(job_id, exc)
            logger.exception("Failed job embedding write-back job_id=%s", job_id)
    return job_success


def _process_requirement_embedding_batch(
    ctx: AppContext,
    stop_event: threading.Event,
    req_data: list[tuple[object, str]],
) -> int:
    """Generate and persist requirement embeddings for one batch."""
    if not req_data or stop_event.is_set():
        return 0

    req_ids, req_texts = zip(*req_data)
    try:
        req_vectors = ctx.job_etl_service.ai.generate_embeddings_batch(list(req_texts))
        req_success = 0
        for req_id, vector in zip(req_ids, req_vectors):
            if stop_event.is_set():
                break
            try:
                with job_uow() as repo:
                    repo.save_requirement_embedding(req_id, vector)
                req_success += 1
            except Exception:
                logger.exception("Failed requirement embedding write-back req_id=%s", req_id)
        return req_success
    except Exception:
        logger.exception("Batch requirement embedding API failed, falling back to per-item")

    req_success = 0
    for req_id in req_ids:
        if stop_event.is_set():
            break
        try:
            with job_uow() as repo:
                req = repo.get_requirement_by_id(req_id)
                if req:
                    ctx.job_etl_service.embed_requirement_one(repo, req)
            req_success += 1
        except Exception:
            logger.exception("Failed requirement embedding req_id=%s", req_id)
    return req_success


def _run_embedding_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """Run embedding batch using batch API calls to minimize round-trips.

    Flow per entity type:
      A) Single DB transaction: fetch items, bulk-mark in_progress, collect texts
      B) Single batch API call: generate all embeddings at once
      C) Per-item DB transactions: write back (preserves per-item error isolation)
    """
    # Phase A: collect texts while the session is open to avoid detached objects.
    job_data = _collect_job_embedding_data(limit)
    if job_data:
        logger.info(f"Found {len(job_data)} jobs needing embedding")
    else:
        logger.info("No jobs need embedding — all already processed")

    job_success = _process_job_embedding_batch(ctx, stop_event, job_data)

    # Phase A: extract id+text while session is open to avoid detached objects.
    req_data = _collect_requirement_embedding_data(limit)
    if req_data:
        logger.info(f"Found {len(req_data)} requirements needing embedding")
    else:
        logger.info("No requirements need embedding — all already processed")

    req_success = _process_requirement_embedding_batch(ctx, stop_event, req_data)

    if job_success or req_success:
        logger.info(f"Embedding batch completed: {job_success} jobs, {req_success} reqs")
    return job_success + req_success


def run_embedding_extraction(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """
    Run embedding extraction for jobs.

    Args:
        ctx: Application context
        stop_event: Event to signal shutdown
        limit: Maximum items to process per category (jobs, requirements)

    Returns:
        Total number of items processed (jobs and requirements).
        Note: May be as large as 2 * limit since each category is limited separately.
    """
    return _run_embedding_batch(ctx, stop_event, limit)


def generate_resume_embedding(
    ctx: AppContext,
    resume_fingerprint: str,
    owner_id: str = "",
) -> bool:
    """
    Generate embeddings for a resume.
    
    Args:
        ctx: Application context
        resume_fingerprint: Resume fingerprint
    
    Returns:
        True if embedded, False if resume not found
    """
    owner_id = owner_id or SYSTEM_OWNER_ID
    logger.info(f"Generating embeddings for resume: {resume_fingerprint}")

    with job_uow() as repo:
        embedded, _ = ctx.job_etl_service.embed_resume_stage(
            repo,
            resume_fingerprint,
            owner_id=owner_id,
        )

    return embedded
