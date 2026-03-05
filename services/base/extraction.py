"""
Extraction module - handles job and resume ETL.

This module provides extraction functionality that can be used by:
- main.py (backwards compatible)
- services/extraction/main.py (new microservice)
"""

import json
import logging
import threading
from typing import Optional

from core.config_loader import load_config
from core.app_context import AppContext
from database.uow import job_uow
from pipeline.runner import _load_resume_with_parser
from database.models import generate_file_fingerprint

logger = logging.getLogger(__name__)


import time
import traceback
import os

def _run_extraction_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 200):
    """Run extraction batch with per-job transactions."""
    with job_uow() as repo:
        job_ids = [j.id for j in repo.get_unextracted_jobs(limit)]

    logger.info(f"Found {len(job_ids)} jobs needing extraction")

    retry_intervals = [30, 60, 120]
    success_count = 0
    
    for job_id in job_ids:
        if stop_event.is_set():
            break
        
        job_title = None
        for attempt, wait_time in enumerate(retry_intervals):
            try:
                with job_uow() as repo:
                    job = repo.get_by_id(job_id)
                    if job is None:
                        logger.warning(f"Job {job_id} not found, may have been deleted")
                        break
                    job_title = job.title  # Capture before context exits
                    ctx.job_etl_service.extract_one(repo, job)
                success_count += 1
                break
            except Exception as e:
                response = getattr(e, 'response', None)
                if response:
                    try:
                        text = response.text or ""
                        http_details = f"HTTP {response.status_code}: {text[:500]}"
                    except Exception:
                        http_details = f"HTTP {response.status_code}"
                else:
                    http_details = None

                if job_title:
                    job_title_str = job_title[:50]
                else:
                    job_title_str = "unknown"
                exc_type = type(e).__name__
                exc_message = str(e)

                if attempt == len(retry_intervals) - 1:
                    logger.error(
                        "Extraction failed after %d retries, job_id=%s (title: %r): %s - %s. %s. Giving up.",
                        len(retry_intervals), job_id, job_title_str, exc_type, exc_message, http_details or "N/A"
                    )
                    # Mark job as failed to prevent infinite retries
                    try:
                        with job_uow() as repo:
                            repo.mark_extraction_failed(job_id, f"{exc_type}: {exc_message}")
                    except Exception as mark_err:
                        logger.warning("Failed to mark job %s as failed: %s", job_id, mark_err)
                else:
                    logger.warning(
                        "Extraction attempt %d/%d failed for job %s (title: %r): %s - %s. %s. Retrying in %ds...",
                        attempt + 1, len(retry_intervals), job_id, job_title_str, exc_type, exc_message, http_details or "N/A", wait_time
                    )
                    time.sleep(wait_time)

    logger.info(f"Extraction batch completed: {success_count}/{len(job_ids)} jobs")
    return success_count


def _run_facet_recovery_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """Recover failed/incomplete facet extraction and embeddings from previous runs."""
    max_retries = 5
    claim_timeout_minutes = 30
    recovered = 0

    # Step 1: Reset stale in_progress jobs
    with job_uow() as repo:
        reset_count = repo.reset_stale_facet_jobs(claim_timeout_minutes, max_retries)
        if reset_count > 0:
            logger.info(f"Facet recovery: reset {reset_count} stale in_progress jobs")
        recovered += reset_count

    # Step 2: Retry failed extractions
    with job_uow() as repo:
        failed_jobs = repo.get_jobs_with_failed_facets(limit, max_retries)
        failed_job_ids = [j.id for j in failed_jobs]

    if failed_job_ids:
        logger.info(f"Facet recovery: retrying {len(failed_job_ids)} failed extractions")
        for job_id in failed_job_ids:
            if stop_event.is_set():
                break
            try:
                with job_uow() as repo:
                    job = repo.get_by_id(job_id)
                    if job and job.facet_status == 'failed':
                        repo.update_job_facet_status(job.id, None)
                        recovered += 1
            except Exception:
                logger.exception(f"Facet recovery failed for job {job_id}")

    # Step 3: Retry missing embeddings
    with job_uow() as repo:
        jobs_missing_embeddings = repo.get_jobs_with_missing_facet_embeddings(limit, max_retries)
        missing_embedding_job_ids = [j.id for j in jobs_missing_embeddings]

    if missing_embedding_job_ids:
        logger.info(f"Facet recovery: retrying {len(missing_embedding_job_ids)} missing embeddings")
        for job_id in missing_embedding_job_ids:
            if stop_event.is_set():
                break
            try:
                with job_uow() as repo:
                    job = repo.get_by_id(job_id)
                    if job and job.facet_status == 'done':
                        ctx.job_etl_service.embed_facets_one(repo, job)
                        recovered += 1
            except Exception:
                logger.exception(f"Facet embedding recovery failed for job {job_id}")

    logger.info(f"Facet recovery batch completed: recovered={recovered}")
    return recovered


def _run_facet_extraction_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100) -> int:
    """Run facet extraction batch with atomic claiming."""
    worker_id = f"worker_{os.getpid()}"
    processed = 0

    while not stop_event.is_set():
        with job_uow() as repo:
            jobs = repo.get_and_claim_jobs_for_facet_extraction(limit, worker_id)
            if not jobs:
                break
            job_ids = [j.id for j in jobs]

        for job_id in job_ids:
            if stop_event.is_set():
                break
            try:
                with job_uow() as repo:
                    job = repo.get_by_id(job_id)
                    if job and job.facet_status == 'in_progress':
                        ctx.job_etl_service.extract_facets_one(repo, job)
                        processed += 1
            except Exception:
                error_msg = traceback.format_exc()
                try:
                    with job_uow() as repo:
                        repo.mark_job_facets_failed(job_id, error_msg)
                except Exception as mark_err:
                    logger.warning("Failed to mark job %s facets as failed: %s", job_id, mark_err)
                logger.exception("Facet extraction error job_id=%s", job_id)

    logger.info(f"Facet extraction batch completed: processed={processed}")
    return processed


def run_job_extraction(ctx: AppContext, stop_event: threading.Event, limit: int = 200) -> int:
    """
    Run job extraction - extract structured data from jobs.

    Args:
        ctx: Application context
        stop_event: Event to signal shutdown
        limit: Maximum jobs to process

    Returns:
        Number of jobs processed
    """
    recovery_count = _run_facet_recovery_batch(ctx, stop_event, limit)
    extraction_count = _run_extraction_batch(ctx, stop_event, limit)
    facet_count = _run_facet_extraction_batch(ctx, stop_event, limit)
    return recovery_count + extraction_count + facet_count





def run_resume_extraction(ctx: AppContext, resume_file: str) -> tuple[Optional[dict], str]:
    """
    Extract resume data from file.

    Args:
        ctx: Application context
        resume_file: Path to resume file

    Returns:
        Tuple of (resume_data, fingerprint)
    """
    logger.info(f"Extracting resume from {resume_file}")

    # Load and parse resume with error handling
    try:
        resume_data = _load_resume_with_parser(resume_file)
        if not resume_data:
            return None, ""

        # Generate fingerprint from parsed data
        fingerprint = generate_file_fingerprint(json.dumps(resume_data, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())
    except (FileNotFoundError, IOError, PermissionError) as e:
        logger.error(f"Failed to read resume file {resume_file}: {e}")
        return None, ""
    except Exception as e:
        logger.error(f"Failed to parse resume file {resume_file}: {e}")
        return None, ""

    return resume_data, fingerprint


def process_resume(ctx: AppContext, resume_file: str) -> tuple[bool, Optional[str]]:
    """
    Extract resume data (no embeddings).

    Args:
        ctx: Application context
        resume_file: Path to resume file

    Returns:
        Tuple of (extracted: bool, fingerprint: Optional[str])
    """
    logger.info(f"Extracting resume: {resume_file}")

    with job_uow() as repo:
        extracted, fingerprint, _ = ctx.job_etl_service.extract_resume(repo, resume_file)

    return extracted, fingerprint
