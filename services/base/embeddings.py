"""
Embeddings module - handles vector generation.

This module provides embedding functionality that can be used by:
- main.py (backwards compatible)
- services/embeddings/main.py (new microservice)
"""

import logging
import threading

from core.config_loader import load_config
from core.app_context import AppContext
from database.uow import job_uow

logger = logging.getLogger(__name__)


def _run_facet_embedding_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """Run facet embedding batch - embed extracted facets for all jobs."""
    with job_uow() as repo:
        jobs = repo.get_jobs_needing_facet_embedding(limit)
        job_ids = [j.id for j in jobs]

    logger.info(f"Found {len(job_ids)} jobs needing facet embedding")

    processed = 0
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
                    ctx.job_etl_service.embed_facets_one(repo, job)
                    processed += 1
                else:
                    logger.debug(f"Job {job_id} facet_status is '{job.facet_status}', skipping")
        except Exception:
            logger.exception("Facet embedding error job_id=%s", job_id)

    logger.info(f"Facet embedding batch completed: processed={processed}")
    return processed


def _run_embedding_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """Run embedding batch with per-job and per-requirement transactions."""
    # 1. Jobs
    with job_uow() as repo:
        job_ids = [j.id for j in repo.get_unembedded_jobs(limit)]

    logger.info(f"Found {len(job_ids)} jobs needing embedding")

    job_success = 0
    for job_id in job_ids:
        if stop_event.is_set():
            break
        try:
            with job_uow() as repo:
                job = repo.get_by_id(job_id)
                if job is None:
                    logger.warning(f"Job {job_id} not found, may have been deleted")
                    continue
                ctx.job_etl_service.embed_job_one(repo, job)
            job_success += 1
        except Exception:
            logger.exception("Failed job embedding job_id=%s", job_id)

    # 2. Requirements
    with job_uow() as repo:
        req_ids = [r.id for r in repo.get_unembedded_requirements(limit * 10)]

    logger.info(f"Found {len(req_ids)} requirements needing embedding")

    req_success = 0
    for req_id in req_ids:
        if stop_event.is_set():
            break
        try:
            with job_uow() as repo:
                req = repo.get_requirement_by_id(req_id)
                if req is None:
                    logger.warning(f"Requirement {req_id} not found, may have been deleted")
                    continue
                ctx.job_etl_service.embed_requirement_one(repo, req)
            req_success += 1
        except Exception:
            logger.exception("Failed requirement embedding req_id=%s", req_id)

    logger.info(f"Embedding batch completed: {job_success} jobs, {req_success} reqs")
    return job_success + req_success


def run_embedding_extraction(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """
    Run embedding extraction for jobs.

    Args:
        ctx: Application context
        stop_event: Event to signal shutdown
        limit: Maximum jobs to process

    Returns:
        Total number of items processed (facets, jobs, and requirements)
    """
    facet_count = _run_facet_embedding_batch(ctx, stop_event, limit)
    embed_count = _run_embedding_batch(ctx, stop_event, limit)
    return facet_count + embed_count


def generate_resume_embedding(ctx: AppContext, resume_fingerprint: str) -> bool:
    """
    Generate embeddings for a resume.
    
    Args:
        ctx: Application context
        resume_fingerprint: Resume fingerprint
    
    Returns:
        True if embedded, False if resume not found
    """
    from etl.orchestrator import JobETLService
    
    logger.info(f"Generating embeddings for resume: {resume_fingerprint}")

    with job_uow() as repo:
        embedded, fp = ctx.job_etl_service.embed_resume(repo, resume_fingerprint)

    return embedded
