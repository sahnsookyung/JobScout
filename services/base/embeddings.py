"""
Embeddings module - handles vector generation.

This module provides embedding functionality for the split embeddings
service and shared embedding helpers used by the current runtime.
"""

import logging
import threading

from core.app_context import AppContext
from database.uow import job_uow
from database.models import DEFAULT_LEGACY_OWNER_ID

logger = logging.getLogger(__name__)


def _run_facet_embedding_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """Run facet embedding batch - embed extracted facets for all jobs."""
    with job_uow() as repo:
        jobs = repo.get_jobs_needing_facet_embedding(limit)
        job_ids = [j.id for j in jobs]

    if job_ids:
        logger.info(f"Found {len(job_ids)} jobs needing facet embedding")
    else:
        logger.info("No jobs need facet embedding — all already processed")

    processed = 0
    total_new = 0
    for job_id in job_ids:
        if stop_event.is_set():
            break
        try:
            with job_uow() as repo:
                job = repo.get_by_id(job_id)
                if job is None:
                    logger.warning(f"Job {job_id} not found, may have been deleted")
                    continue
                if job.facet_status == 'done':
                    total_new += ctx.job_etl_service.embed_facets_one(repo, job)
                    processed += 1
                else:
                    logger.debug(f"Job {job_id} facet_status is '{job.facet_status}', skipping")
        except Exception:
            logger.error("Facet embedding error job_id=%s", job_id, exc_info=True)

    if total_new > 0:
        logger.info(f"Facet embedding batch completed: {total_new} new embeddings across {processed} jobs")
    elif processed > 0:
        logger.info(f"Facet embedding batch: {processed} jobs checked — all facets already embedded")
    return processed


def _run_embedding_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """Run embedding batch using batch API calls to minimize round-trips.

    Flow per entity type:
      A) Single DB transaction: fetch items, bulk-mark in_progress, collect texts
      B) Single batch API call: generate all embeddings at once
      C) Per-item DB transactions: write back (preserves per-item error isolation)
    """
    # --- 1. Jobs ---
    # Phase A: fetch all jobs, bulk mark in_progress, collect texts in one session
    # Accessing lazy relationships (requirements, benefits) here while the
    # session is still open avoids DetachedInstanceError later.
    job_data: list = []  # [(job_id, text)]
    with job_uow() as repo:
        jobs = list(repo.get_unembedded_jobs(limit))
        if jobs:
            repo.bulk_mark_embedding_in_progress([j.id for j in jobs])
        for job in jobs:
            parts = []
            if job.requirements:
                parts.extend([r.text for r in job.requirements[:20]])
            if job.benefits:
                parts.extend([b.text for b in job.benefits[:10]])
            text = " | ".join(parts) if parts else (job.description[:5000] if job.description else "")
            job_data.append((job.id, text))

    if job_data:
        logger.info(f"Found {len(job_data)} jobs needing embedding")
    else:
        logger.info("No jobs need embedding — all already processed")

    job_success = 0
    if job_data and not stop_event.is_set():
        job_ids, job_texts = zip(*job_data)
        # Phase B: single batch API call
        try:
            vectors = ctx.job_etl_service.ai.generate_embeddings_batch(list(job_texts))
        except Exception as exc:
            logger.error("Batch job embedding API failed: %s", exc, exc_info=True)
            error = f"{type(exc).__name__}: {exc}"
            for job_id in job_ids:
                try:
                    with job_uow() as repo:
                        repo.mark_embedding_retryable_failed(job_id, error)
                except Exception:
                    logger.warning("Failed to persist embedding failure for job_id=%s", job_id, exc_info=True)
            vectors = []

        # Phase C: per-job write-back for error isolation
        for job_id, vector in zip(job_ids, vectors):
            if stop_event.is_set():
                break
            try:
                with job_uow() as repo:
                    job_fresh = repo.get_by_id(job_id)
                    if job_fresh is None:
                        logger.warning(f"Job {job_id} not found, may have been deleted")
                        continue
                    repo.save_job_embedding(job_fresh, vector)
                job_success += 1
            except Exception as exc:
                try:
                    with job_uow() as repo:
                        # Empty/invalid provider responses and rate limits should remain
                        # unprocessed so queue-first reconciliation can retry later.
                        repo.mark_embedding_retryable_failed(job_id, f"{type(exc).__name__}: {exc}")
                except Exception:
                    logger.warning("Failed to persist embedding failure for job_id=%s", job_id, exc_info=True)
                logger.error("Failed job embedding write-back job_id=%s", job_id, exc_info=True)

    # --- 2. Requirements ---
    # Phase A: extract id+text while session is open — accessing attributes after
    # session close raises DetachedInstanceError because SQLAlchemy expires all
    # columns on commit, not just relationships.
    req_data: list = []  # [(req_id, text)]
    with job_uow() as repo:
        for req in repo.get_unembedded_requirements(limit):
            req_data.append((req.id, req.text))

    if req_data:
        logger.info(f"Found {len(req_data)} requirements needing embedding")
    else:
        logger.info("No requirements need embedding — all already processed")

    req_success = 0
    if req_data and not stop_event.is_set():
        req_ids, req_texts = zip(*req_data)
        try:
            # Phase B: single batch API call
            req_vectors = ctx.job_etl_service.ai.generate_embeddings_batch(list(req_texts))
            # Phase C: per-item write-back
            for req_id, vector in zip(req_ids, req_vectors):
                if stop_event.is_set():
                    break
                try:
                    with job_uow() as repo:
                        repo.save_requirement_embedding(req_id, vector)
                    req_success += 1
                except Exception:
                    logger.error("Failed requirement embedding write-back req_id=%s", req_id, exc_info=True)
        except Exception:
            logger.error("Batch requirement embedding API failed, falling back to per-item", exc_info=True)
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
                    logger.error("Failed requirement embedding req_id=%s", req_id, exc_info=True)

    if job_success or req_success:
        logger.info(f"Embedding batch completed: {job_success} jobs, {req_success} reqs")
    return job_success + req_success


def run_embedding_extraction(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """
    Run embedding extraction for jobs.

    Args:
        ctx: Application context
        stop_event: Event to signal shutdown
        limit: Maximum items to process per category (facets, jobs, requirements)

    Returns:
        Total number of items processed (facets, jobs, and requirements).
        Note: May be as large as 3 * limit since each category is limited separately.
    """
    facet_count = _run_facet_embedding_batch(ctx, stop_event, limit)
    embed_count = _run_embedding_batch(ctx, stop_event, limit)
    return facet_count + embed_count


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
    owner_id = owner_id or DEFAULT_LEGACY_OWNER_ID
    logger.info(f"Generating embeddings for resume: {resume_fingerprint}")

    with job_uow() as repo:
        embedded, _ = ctx.job_etl_service.embed_resume_stage(
            repo,
            resume_fingerprint,
            owner_id=owner_id,
        )

    return embedded
