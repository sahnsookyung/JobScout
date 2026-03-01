"""JobScout Main Driver - Refactored with Unit of Work pattern.

Three CLI commands:
- job-etl:    Scrapes and extracts jobs (steps 1-4)
- resume-etl: Extracts and embeds resume (step 5)
- matching:   Runs matching pipeline (requires jobs + resume in DB)
"""

import time
import logging
import signal
import sys
import os
import json
import argparse
import threading
import traceback
from typing import Optional, List

from core.config_loader import load_config
from core.app_context import AppContext
from core.matcher import MatcherService, MatchResultDTO, JobMatchDTO, JobEvidenceDTO, RequirementMatchDTO, JobRequirementDTO, penalty_details_from_orm
from core.scorer import ScoringService
from core.scorer.persistence import save_match_to_db
from etl.resume import ResumeProfiler, ResumeParser
from etl.resume.embedding_store import JobRepositoryAdapter
from database.uow import job_uow
from database.init_db import init_db
from pipeline.runner import run_matching_pipeline as run_matching_pipeline_shared

PIPELINE_API_URL = "http://localhost:8080/api/pipeline"




logger = logging.getLogger(__name__)

stop_event = threading.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received")
    stop_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def setup_logging():
    # Force logging configuration with timestamps
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True  # Force reconfiguration of logging
    )


def load_resume_data(resume_file_path: str) -> Optional[dict]:
    """Load and parse resume data from various formats.

    Supports: .json, .yaml, .yml, .txt, .docx, .pdf

    For JSON/YAML: Returns structured dict that can be used directly.
    For TXT/DOCX/PDF: Returns dict with 'raw_text' key containing extracted text
                      for LLM-based parsing.

    Args:
        resume_file_path: Path to resume file

    Returns:
        Dict with resume data, or None if loading/parsing fails
    """
    logger.info(f"Loading resume from {resume_file_path}")

    try:
        parser = ResumeParser()
        parsed = parser.parse(resume_file_path)

        if parsed.data is not None:
            # JSON/YAML formats - return structured data
            logger.info(f"Loaded structured resume from {parsed.format} file")
            return parsed.data
        else:
            # Text-based formats (TXT, DOCX, PDF) - wrap text for LLM processing
            logger.info(f"Loaded text resume from {parsed.format} file ({len(parsed.text)} chars)")
            return {"raw_text": parsed.text}

    except FileNotFoundError:
        logger.error(f"Resume file not found: {resume_file_path}")
        logger.error("→ Create one: cp resume.example.json " + resume_file_path)
        logger.error("→ Or set path in config.yaml: etl.resume.resume_file")
        return None
    except ValueError as e:
        supported = ', '.join(ResumeParser.get_supported_formats())
        logger.error(f"Failed to parse resume: {e}")
        logger.error(f"→ Supported formats: {supported}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error loading resume: {e}")
        return None


def load_user_wants_data(wants_file_path: str) -> List[str]:
    """Load user wants from a file.
    Each line is a separate want.
    """
    logger.info(f"Loading user wants from {wants_file_path}")
    try:
        with open(wants_file_path, 'r') as f:
            wants = [line.strip() for line in f if line.strip()]
            return wants
    except FileNotFoundError:
        logger.warning(f"User wants file not found: {wants_file_path}")
        logger.warning("→ Create one: cp wants.example.txt " + wants_file_path)
        logger.warning("→ Add one preference per line in natural language")
        return []
    except Exception as e:
        logger.error(f"Error reading user wants file: {e}")
        return []


def run_job_etl(ctx: AppContext, stop_event: threading.Event) -> None:
    """Run job ETL pipeline: gather jobs, extract, facet extract, embed.
    
    Steps:
    1. Gather Jobs (scraping)
    2. Extraction (structured data)
    3. Facet Extraction
    4. Embedding
    """
    logger.info("=" * 60)
    logger.info("STARTING JOB ETL PIPELINE")
    logger.info("=" * 60)

    step_start = time.time()
    logger.info("=== JOB ETL STEP 1: Gathering Jobs ===")
    total_jobs_gathered = 0

    for scraper_cfg in ctx.config.scrapers:
        if stop_event.is_set():
            break

        try:
            task_id = ctx.jobspy_client.submit_scrape(scraper_cfg)
            if not task_id:
                continue

            # Get per-scraper request timeout (for both submit and polling)
            request_timeout = getattr(scraper_cfg, 'request_timeout', None)

            jobs = ctx.jobspy_client.wait_for_result(
                task_id,
                request_timeout_s=request_timeout,
                stop_event=stop_event
            )

            if jobs:
                site_name = str(scraper_cfg.site_type)
                logger.info(f"Processing {len(jobs)} jobs for {site_name}")
                for job in jobs:
                    if stop_event.is_set():
                        break
                    try:
                        with job_uow() as repo:
                            ctx.job_etl_service.ingest_one(repo, job, site_name)
                    except Exception:
                        logger.exception("Ingest failed for site=%s", site_name)
                total_jobs_gathered += len(jobs)
        except Exception as e:
            logger.error(f"Error processing scraper {scraper_cfg.site_type}: {e}")
            logger.error("→ Check if JobSpy service is running")
            logger.error("→ Verify scraper configuration in config.yaml")

    step_elapsed = time.time() - step_start
    logger.info(f"Job ETL Step 1 completed: Gathered {total_jobs_gathered} jobs in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 2: Extraction - per-job transactions
    step_start = time.time()
    logger.info("=== JOB ETL STEP 2: Running Extraction Batch ===")
    _run_extraction_batch(ctx, stop_event, limit=200)
    step_elapsed = time.time() - step_start
    logger.info(f"Job ETL Step 2 completed: Extraction batch finished in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 3: Facet Extraction - per-job transactions (extract text only)
    step_start = time.time()
    logger.info("=== JOB ETL STEP 3: Running Facet Extraction Batch ===")
    _run_facet_extraction_batch(ctx, stop_event, limit=100)
    step_elapsed = time.time() - step_start
    logger.info(f"Job ETL Step 3 completed: Facet extraction batch finished in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 3a: Facet Embedding - embed extracted facets (separate batch for efficiency)
    step_start = time.time()
    logger.info("=== JOB ETL STEP 3a: Running Facet Embedding Batch ===")
    _run_facet_embedding_batch(ctx, stop_event, limit=100)
    step_elapsed = time.time() - step_start
    logger.info(f"Job ETL Step 3a completed: Facet embedding batch finished in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 4: Embedding - per-job and per-requirement transactions
    step_start = time.time()
    logger.info("=== JOB ETL STEP 4: Running Embedding Batch ===")
    _run_embedding_batch(ctx, stop_event, limit=100)
    step_elapsed = time.time() - step_start
    logger.info(f"Job ETL Step 4 completed: Embedding batch finished in {step_elapsed:.2f}s")

    logger.info("=" * 60)
    logger.info("JOB ETL PIPELINE COMPLETED")
    logger.info("=" * 60)


def run_resume_etl(ctx: AppContext, stop_event: threading.Event) -> None:
    """Run resume ETL pipeline: extract and embed resume.
    
    Uses fingerprint-based change detection - only re-processes if resume changed.
    """
    logger.info("=" * 60)
    logger.info("STARTING RESUME ETL PIPELINE")
    logger.info("=" * 60)

    step_start = time.time()
    logger.info("=== RESUME ETL STEP 1: Processing Resume ===")
    _run_resume_etl(ctx, stop_event)
    step_elapsed = time.time() - step_start
    logger.info(f"Resume ETL Step 1 completed in {step_elapsed:.2f}s")

    logger.info("=" * 60)
    logger.info("RESUME ETL PIPELINE COMPLETED")
    logger.info("=" * 60)


def _run_resume_etl(ctx: AppContext, stop_event: threading.Event) -> None:
    """Run resume ETL with fingerprint-based change detection.

    Returns:
        None - matching pipeline will query DB for latest resume independently.
    """
    # Check if resume config is available
    etl_config = ctx.config.etl
    if not etl_config:
        logger.info("No ETL config, skipping resume ETL")
        return
    
    # Support both old path (etl.resume.resume_file) and new path (etl.resume.resume_file)
    if etl_config.resume:
        resume_file = etl_config.resume.resume_file
    elif etl_config.resume_file:
        resume_file = etl_config.resume_file  # Backward compatibility
    else:
        logger.info("No resume file configured, skipping resume ETL")
        return

    if not resume_file:
        logger.info("No resume file configured, skipping resume ETL")
        return
        
    if not os.path.isabs(resume_file):
        resume_file = os.path.join(os.getcwd(), resume_file)

    try:
        with job_uow() as repo:
            ctx.job_etl_service.process_resume(repo, resume_file)
    except Exception as e:
        logger.error(f"Failed to process resume: {e}")


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
        
        for attempt, wait_time in enumerate(retry_intervals):
            try:
                with job_uow() as repo:
                    job = repo.get_by_id(job_id)
                    if job is None:
                        logger.warning(f"Job {job_id} not found, may have been deleted")
                        break
                    ctx.job_etl_service.extract_one(repo, job)
                success_count += 1
                break
            except Exception as e:
                response = getattr(e, 'response', None)
                if response:
                    try:
                        http_details = f"HTTP {response.status_code}: {response.text[:500]}"
                    except Exception:
                        http_details = f"HTTP {response.status_code}"
                else:
                    http_details = None

                job_title = getattr(job, 'title', None)  # type: ignore[union-attr]
                if job_title:
                    job_title = job_title[:50]
                else:
                    job_title = "unknown"
                exc_type = type(e).__name__
                exc_message = str(e)

                if attempt == len(retry_intervals) - 1:
                    logger.error(
                        "Extraction failed after %d retries, job_id=%s (title: %r): %s - %s. %s. Giving up.",
                        len(retry_intervals), job_id, job_title, exc_type, exc_message, http_details or "N/A"
                    )
                else:
                    logger.warning(
                        "Extraction attempt %d/%d failed for job %s (title: %r): %s - %s. %s. Retrying in %ds...",
                        attempt + 1, len(retry_intervals), job_id, job_title, exc_type, exc_message, http_details or "N/A", wait_time
                    )
                    time.sleep(wait_time)

    logger.info(f"Extraction batch completed: {success_count}/{len(job_ids)} jobs")


def _run_facet_extraction_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100):
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


def _run_facet_embedding_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100):
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
                if job and job.facet_status == 'done':
                    ctx.job_etl_service.embed_facets_one(repo, job)
                    processed += 1
        except Exception:
            logger.exception("Facet embedding error job_id=%s", job_id)

    logger.info(f"Facet embedding batch completed: processed={processed}")


def _run_embedding_batch(ctx: AppContext, stop_event: threading.Event, limit: int = 100):
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
                if req:
                    ctx.job_etl_service.embed_requirement_one(repo, req)
            req_success += 1
        except Exception:
            logger.exception("Failed requirement embedding req_id=%s", req_id)

    logger.info(f"Embedding batch completed: {job_success} jobs, {req_success} reqs")


def run_matching_pipeline(ctx: AppContext, stop_event: threading.Event) -> None:
    """Run the matching pipeline using the shared pipeline module.
    
    This is a wrapper around the shared pipeline.runner.run_matching_pipeline
    that maintains backward compatibility with the existing main.py interface.
    """
    result = run_matching_pipeline_shared(ctx, stop_event)
    
    if not result.success:
        logger.error(f"Matching pipeline failed: {result.error}")
    else:
        logger.info(f"Matching pipeline succeeded: {result.matches_count} matches, {result.saved_count} saved")


def run_internal_sequential_cycle(mode: str = 'all', stop_event: threading.Event = None, config=None) -> None:
    if stop_event is None:
        stop_event = threading.Event()

    if config is None:
        config = load_config()

    cycle_start = time.time()

    # Build context once - no DB session attached
    ctx = AppContext.build(config)

    # Job ETL Phase
    if mode in ('job-etl', 'all'):
        logger.info("Running Job ETL phase")
        try:
            run_job_etl(ctx, stop_event)
            if not stop_event.is_set() and ctx.job_etl_service:
                ctx.job_etl_service.unload_models()
        except Exception as e:
            logger.error(f"Error in Job ETL phase: {e}", exc_info=True)

        if stop_event.is_set():
            logger.info("Shutdown requested after Job ETL phase")
            # Clean up JobSpyClient session for ETL phase
            try:
                if ctx.jobspy_client:
                    ctx.jobspy_client.close()
            except Exception as e:
                logger.warning(f"Error closing JobSpy client: {e}")
            return

    # Resume ETL Phase
    if mode in ('resume-etl', 'all'):
        logger.info("Running Resume ETL phase")
        try:
            run_resume_etl(ctx, stop_event)
            if not stop_event.is_set() and ctx.job_etl_service:
                ctx.job_etl_service.unload_models()
        except Exception as e:
            logger.error(f"Error in Resume ETL phase: {e}", exc_info=True)

    # Matching Phase
    if mode in ('matching', 'all'):
        logger.info("Running Matching phase")
        try:
            run_matching_pipeline(ctx, stop_event)
            if not stop_event.is_set() and ctx.job_etl_service:
                ctx.job_etl_service.unload_models()
        except Exception as e:
            logger.error(f"Error in Matching phase: {e}", exc_info=True)

    # Clean up JobSpyClient session
    try:
        if ctx.jobspy_client:
            ctx.jobspy_client.close()
    except Exception as e:
        logger.warning(f"Error closing JobSpy client: {e}")

    cycle_elapsed = time.time() - cycle_start
    logger.info(f"=== Cycle Completed in {cycle_elapsed:.2f}s ===")


def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="JobScout Main Driver")
    parser.add_argument(
        '--mode', 
        type=str, 
        choices=['all', 'job-etl', 'resume-etl', 'matching'], 
        default='all',
        help='Pipeline mode to run: all (job-etl + resume-etl + matching), job-etl, resume-etl, or matching'
    )
    args = parser.parse_args()

    mode = args.mode
    logger.info(f"Main driver starting in {mode.upper()} mode...")

    if mode == 'all':
        logger.info("Pipeline: Job ETL -> Resume ETL -> Matching")
    elif mode == 'job-etl':
        logger.info("Pipeline: Job ETL ONLY (gather, extract, facet, embed)")
    elif mode == 'resume-etl':
        logger.info("Pipeline: Resume ETL ONLY (extract, embed resume)")
    elif mode == 'matching':
        logger.info("Pipeline: Matching ONLY (match + score jobs)")

    # Initialize DB
    init_db()

    # Initial config load (will be reloaded each cycle for hot-reload support)
    initial_config = load_config()
    initial_interval = initial_config.schedule.interval_seconds

    cycle_count = 0
    while not stop_event.is_set():
        cycle_count += 1
        cycle_start = time.time()
        logger.info(f"=== Starting Cycle #{cycle_count} ({mode.upper()}) ===")

        # Reload config each cycle for hot-reload support
        # Use the same config instance for scheduling and dependency wiring
        config = load_config()
        interval = config.schedule.interval_seconds
        if interval <= 0:
            logger.error(f"Invalid interval: {interval}. Using default 3600s.")
            interval = 3600

        try:
            # No lock needed - DB handles concurrency
            run_internal_sequential_cycle(mode=mode, stop_event=stop_event, config=config)
                
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)

        cycle_elapsed = time.time() - cycle_start
        if not stop_event.is_set():
            logger.info(f"=== Cycle #{cycle_count} completed in {cycle_elapsed:.2f}s. Sleeping for {interval} seconds... ===")
            # Use stop_event.wait() for responsive shutdown
            stop_event.wait(interval)


if __name__ == "__main__":
    main()
