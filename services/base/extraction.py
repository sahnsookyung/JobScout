"""
Extraction module - handles job and resume ETL.

This module provides extraction functionality for the extraction
microservice and shared extraction helpers used by the current runtime.
"""

import logging
import threading
from typing import Optional

from core.app_context import AppContext
from database.uow import job_uow
from etl.resume.loader import load_resume_with_parser
from database.models import SYSTEM_OWNER_ID, generate_file_fingerprint

logger = logging.getLogger(__name__)


def _format_http_error(e: Exception) -> str:
    """Format HTTP error details for logging."""
    response = getattr(e, 'response', None)
    if response:
        try:
            text = response.text or ""
            return f"HTTP {response.status_code}: {text[:500]}"
        except Exception:
            return f"HTTP {response.status_code}"
    return "N/A"


def _mark_job_retryable(job_id: int, exc_type: str, exc_message: str) -> None:
    """Persist retryable failure state for backoff-based reprocessing."""
    try:
        with job_uow() as repo:
            repo.mark_extraction_retryable_failed(job_id, f"{exc_type}: {exc_message}")
    except Exception as mark_err:
        logger.warning("Failed to mark job %s as retryable failed: %s", job_id, mark_err)


def _mark_job_failed(job_id: int, exc_type: str, exc_message: str) -> None:
    """Compatibility wrapper: failed extraction remains retryable."""
    _mark_job_retryable(job_id, exc_type, exc_message)


def _on_extraction_error(
    e: Exception,
    job_id: int,
    job_title: Optional[str],
    attempt: int,
    retry_intervals: list,
    wait_time: int,
    stop_event: threading.Event,
) -> bool:
    """Log a failed attempt and decide whether the retry loop should stop.

    Returns True if the caller should break out of the retry loop.
    """
    http_details = _format_http_error(e)
    job_title_str = job_title[:50] if job_title else "unknown"
    exc_type = type(e).__name__
    exc_message = str(e)
    is_last_attempt = attempt == len(retry_intervals) - 1

    if is_last_attempt:
        logger.error(
            "Extraction failed after %d immediate retries, job_id=%s (title: %r): %s - %s. %s. Deferring to queue retry.",
            len(retry_intervals), job_id, job_title_str, exc_type, exc_message, http_details,
        )
        _mark_job_retryable(job_id, exc_type, exc_message)
        return True
    else:
        _mark_job_retryable(job_id, exc_type, exc_message)
        logger.warning(
            "Extraction attempt %d/%d failed for job %s (title: %r): %s - %s. %s. Retrying in %ds...",
            attempt + 1, len(retry_intervals), job_id, job_title_str, exc_type, exc_message, http_details, wait_time,
        )
        return stop_event.wait(wait_time)


def _extract_single_job(
    ctx: AppContext,
    job_id: int,
    retry_intervals: list,
    stop_event: threading.Event,
) -> bool:
    """Extract a single job with retries. Returns True if successful."""
    job_title = None

    for attempt, wait_time in enumerate(retry_intervals):
        if stop_event.is_set():
            break

        try:
            with job_uow() as repo:
                job = repo.get_by_id(job_id)
                if job is None:
                    logger.warning("Job %s not found, may have been deleted", job_id)
                    return False
                repo.mark_extraction_in_progress(job_id)
                job_title = job.title
                ctx.job_etl_service.extract_one(repo, job)
            return True

        except Exception as e:
            if _on_extraction_error(e, job_id, job_title, attempt, retry_intervals, wait_time, stop_event):
                break

    return False


def _run_extraction_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 200):
    """Run extraction batch with per-job transactions."""
    with job_uow() as repo:
        job_ids = [j.id for j in repo.get_unextracted_jobs(limit)]

    if job_ids:
        logger.info("Found %d jobs needing extraction", len(job_ids))
    else:
        logger.info("No jobs need extraction — all already processed")

    retry_intervals = [30, 60, 120]
    success_count = 0

    for job_id in job_ids:
        if stop_event.is_set():
            break

        if _extract_single_job(ctx, job_id, retry_intervals, stop_event):
            success_count += 1

    logger.info("Extraction batch completed: %d/%d jobs", success_count, len(job_ids))
    return success_count


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
    return _run_extraction_batch(ctx, stop_event, limit)
    

def run_resume_extraction(
    ctx: AppContext, 
    resume_file: str, 
    known_fingerprint: Optional[str] = None
) -> tuple[Optional[dict], str]:
    """
    Extract resume data from file.

    Args:
        ctx: Application context (unused - kept for API consistency)
        resume_file: Path to resume file
        known_fingerprint: Optional pre-computed fingerprint from raw file bytes.
                          If provided, skips re-computation for efficiency.

    Returns:
        Tuple of (resume_data, fingerprint)
    """
    del ctx  # Unused parameter - kept for API consistency
    logger.info("Extracting resume from %s", resume_file)

    # Use provided fingerprint OR compute from raw file bytes
    if known_fingerprint:
        fingerprint = known_fingerprint
    else:
        # Standalone usage - read file and hash raw bytes
        try:
            with open(resume_file, 'rb') as f:
                file_bytes = f.read()
            fingerprint = generate_file_fingerprint(file_bytes)
        except (FileNotFoundError, IOError, PermissionError):
            logger.exception("Failed to read resume file %s", resume_file)
            return None, ""

    # Load and parse resume with error handling
    try:
        resume_data = load_resume_with_parser(resume_file)
        if not resume_data:
            return None, fingerprint
    except (FileNotFoundError, IOError, PermissionError):
        logger.exception("Failed to read resume file %s", resume_file)
        return None, fingerprint
    except Exception:
        logger.exception("Failed to parse resume file %s", resume_file)
        return None, fingerprint

    return resume_data, fingerprint


def extract_resume(
    ctx: AppContext, 
    resume_file: str, 
    known_fingerprint: Optional[str] = None,
    force_re_extraction: bool = False,
    owner_id: str = "",
) -> tuple[bool, Optional[str]]:
    """
    Extract resume data (no embeddings).

    Args:
        ctx: Application context
        resume_file: Path to resume file
        known_fingerprint: Optional pre-computed fingerprint from raw file bytes.
                          If provided, skips re-computation for efficiency.

    Returns:
        Tuple of (extracted: bool, fingerprint: Optional[str])
    """
    logger.info(f"Extracting resume: {resume_file}")
    owner_id = owner_id or SYSTEM_OWNER_ID

    with job_uow() as repo:
        if force_re_extraction:
            extracted, fingerprint, _ = ctx.job_etl_service.process_resume(
                repo,
                resume_file,
                force_re_extraction=True,
                owner_id=owner_id,
            )
        else:
            extracted, fingerprint, _ = ctx.job_etl_service.extract_resume_stage(
                repo,
                resume_file,
                known_fingerprint,
                owner_id=owner_id,
            )

    return extracted, fingerprint
