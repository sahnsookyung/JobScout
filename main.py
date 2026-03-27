"""Legacy monolithic helpers.

The supported runtime is the split microservice topology started via Docker
Compose and the web backend. This module remains only as a compatibility
surface for a few internal imports while the last legacy callers are removed.
Running it directly as a CLI is intentionally unsupported.
"""

import time
import logging
import signal
import sys
import os
import json
import threading
import traceback
from typing import Optional, List

from core.config_loader import load_config
from core.app_context import AppContext
from core.logging_utils import (
    is_nul_filter_active,
    setup_logging as setup_shared_logging,
)
setup_shared_logging(level=logging.INFO)
from core.matcher import MatcherService, MatchResultDTO, JobMatchDTO, JobEvidenceDTO, RequirementMatchDTO, JobRequirementDTO, penalty_details_from_orm
from core.scorer import ScoringService
from core.scorer.persistence import save_match_to_db

from etl.resume import ResumeProfiler, ResumeParser
from etl.resume.embedding_store import JobRepositoryAdapter
from database.uow import job_uow
from database.init_db import init_db
from web.backend.services.clients import extraction_client, orchestrator_client

PIPELINE_API_URL = "http://localhost:8080/api/pipeline"




logger = logging.getLogger(__name__)
logger.debug("NUL log sanitization active=%s", is_nul_filter_active())

stop_event = threading.Event()
DEFAULT_DEV_OWNER_ID = os.getenv(
    "DEV_BYPASS_USER_ID",
    "00000000-0000-0000-0000-000000000001",
)


def signal_handler(sig, frame):
    logger.info("Shutdown signal received")
    stop_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


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

    # Recovery is now handled automatically by the extraction microservice.

    if stop_event.is_set():
        return

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

    # Step 2: Extraction
    step_start = time.time()
    if not os.getenv("ORCHESTRATOR_URL", "").strip():
        logger.error("ORCHESTRATOR_URL not configured — cannot run extraction stage")
        return
    logger.info("=== JOB ETL STEP 2: Triggering Orchestrator Extraction Stage ===")
    try:
        res = orchestrator_client.start_stage("extract", limit=200)
        logger.info(f"Orchestrator extraction response: {res}")
        if not res.get("success"):
            logger.error("Extraction failed, aborting pipeline")
            return
        stage_result = orchestrator_client.wait_for_completion(
            res.get("task_id", ""), timeout=600.0
        )
        if stage_result.get("status") != "completed":
            logger.error("Extraction failed, aborting pipeline: %s", stage_result)
            return
    except Exception as e:
        logger.error(f"Extraction stage failed: {e}")
        return
    step_elapsed = time.time() - step_start
    logger.info(f"Job ETL Step 2 completed in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 3: Embeddings
    step_start = time.time()
    logger.info("=== JOB ETL STEP 3: Triggering Orchestrator Embedding Stage ===")
    try:
        res = orchestrator_client.start_stage("embed", limit=100)
        logger.info(f"Orchestrator embedding response: {res}")
        if not res.get("success"):
            logger.error("Embeddings failed, aborting pipeline")
            return
        stage_result = orchestrator_client.wait_for_completion(
            res.get("task_id", ""), timeout=600.0
        )
        if stage_result.get("status") != "completed":
            logger.error("Embeddings failed, aborting pipeline: %s", stage_result)
            return
    except Exception as e:
        logger.error(f"Embedding stage failed: {e}")
        return
    step_elapsed = time.time() - step_start
    logger.info(f"Job ETL Step 3 completed in {step_elapsed:.2f}s")

    logger.info("=" * 60)
    logger.info("JOB ETL PIPELINE COMPLETED")
    logger.info("=" * 60)


def run_resume_etl(ctx: AppContext) -> None:
    """Run resume ETL pipeline: extract and embed resume.
    
    Uses fingerprint-based change detection - only re-processes if resume changed.
    """
    logger.info("=" * 60)
    logger.info("STARTING RESUME ETL PIPELINE")
    logger.info("=" * 60)

    step_start = time.time()
    logger.info("=== RESUME ETL STEP 1: Processing Resume ===")
    _run_resume_etl(ctx)
    step_elapsed = time.time() - step_start
    logger.info(f"Resume ETL Step 1 completed in {step_elapsed:.2f}s")

    logger.info("=" * 60)
    logger.info("RESUME ETL PIPELINE COMPLETED")
    logger.info("=" * 60)


def _run_resume_etl(ctx: AppContext) -> bool:
    """Run resume ETL with fingerprint-based change detection.

    Returns:
        True if extraction succeeded, False otherwise.
    """
    # Check if resume config is available
    etl_config = ctx.config.etl
    if not etl_config:
        logger.info("No ETL config, skipping resume ETL")
        return False

    # Support both old path (etl.resume.resume_file) and new path (etl.resume.resume_file)
    if etl_config.resume:
        resume_file = etl_config.resume.resume_file
    elif etl_config.resume_file:
        resume_file = etl_config.resume_file  # Backward compatibility
    else:
        logger.info("No resume file configured, skipping resume ETL")
        return False

    if not resume_file:
        logger.info("No resume file configured, skipping resume ETL")
        return False
        
    if not os.path.isabs(resume_file):
        resume_file = os.path.join(os.getcwd(), resume_file)

    if not os.getenv("EXTRACTION_URL", "").strip():
        logger.error("EXTRACTION_URL not configured — cannot run resume extraction")
        return False

    try:
        force_re_extraction = bool(
            etl_config.resume and etl_config.resume.force_re_extraction
        )
        res = extraction_client.extract_resume(
            resume_file=resume_file,
            owner_id=DEFAULT_DEV_OWNER_ID,
            force_re_extraction=force_re_extraction,
        )
        logger.info(f"Extraction microservice response for resume: {res}")
        return res.get("success", False)
    except Exception as e:
        logger.error(f"Failed to trigger resume extraction: {e}")
        return False





def run_matching_pipeline(
    ctx: AppContext,
    stop_event_local: Optional[threading.Event] = None,
) -> tuple[bool, str]:
    """Run the matching pipeline via the orchestrator microservice.

    Returns:
        Tuple of (success: bool, task_id: str)
        - success: True if matching completed successfully, False otherwise
        - task_id: The task ID for reference (empty string on immediate failure)
    """
    if not os.getenv("ORCHESTRATOR_URL", "").strip():
        logger.error("ORCHESTRATOR_URL not configured — cannot run matching pipeline")
        return False, ""

    logger.info("Triggering orchestrator microservice for matching pipeline...")
    try:
        res = orchestrator_client.start_matching()
        logger.info(f"Orchestrator microservice response: {res}")

        if not res.get("success"):
            logger.error(f"Orchestrator failed to start: {res.get('message')}")
            return False, ""

        task_id = res.get("task_id", "")
        logger.info(f"Started orchestration task {task_id}. Waiting for completion...")

        return _wait_for_orchestrator_result(task_id)

    except Exception as e:
        logger.error(f"Failed to trigger orchestrator microservice: {e}")
        return False, ""


def _wait_for_orchestrator_result(task_id: str) -> tuple[bool, str]:
    """Wait for orchestrator task completion and return result."""
    result = orchestrator_client.wait_for_completion(task_id, timeout=600.0)
    status = result.get("status", "unknown")
    if status == "completed":
        matches_count = result.get("result", {}).get("matches_count", 0)
        logger.info(f"Matching pipeline completed successfully with {matches_count} matches")
        return True, task_id
    elif status == "failed":
        error = result.get("result", {}).get("error", "Unknown error")
        logger.error(f"Matching pipeline failed: {error}")
        return False, task_id
    elif status == "cancelled":
        logger.warning("Matching pipeline cancelled")
        return False, task_id
    else:  # timeout
        logger.error("Matching pipeline timed out waiting for completion")
        return False, task_id


def run_internal_sequential_cycle(mode: str = 'all', stop_event: threading.Event = None, config=None) -> None:
    if stop_event is None:
        stop_event = threading.Event()

    if config is None:
        config = load_config()

    cycle_start = time.time()

    # Build context once - no DB session attached
    ctx = AppContext.build(config)

    try:
        # Job ETL Phase
        if mode in ('job-etl', 'all'):
            _run_job_etl_phase(ctx, stop_event)
            if stop_event.is_set():
                logger.info("Shutdown requested after Job ETL phase")
                return

        # Resume ETL Phase
        if mode in ('resume-etl', 'all'):
            _run_resume_etl_phase(ctx, stop_event)

        # Matching Phase
        if mode in ('matching', 'all'):
            _run_matching_phase(ctx, stop_event)
    finally:
        # Clean up JobSpyClient session
        _cleanup_jobspy_client(ctx)

    cycle_elapsed = time.time() - cycle_start
    logger.info(f"=== Cycle Completed in {cycle_elapsed:.2f}s ===")


def _run_job_etl_phase(ctx: AppContext, stop_event: threading.Event) -> None:
    """Run Job ETL phase."""
    logger.info("Running Job ETL phase")
    try:
        run_job_etl(ctx, stop_event)
        if not stop_event.is_set() and ctx.job_etl_service:
            ctx.job_etl_service.unload_models()
    except Exception as e:
        logger.error(f"Error in Job ETL phase: {e}", exc_info=True)


def _run_resume_etl_phase(ctx: AppContext, stop_event: threading.Event) -> None:
    """Run Resume ETL phase."""
    logger.info("Running Resume ETL phase")
    try:
        run_resume_etl(ctx)
        if not stop_event.is_set() and ctx.job_etl_service:
            ctx.job_etl_service.unload_models()
    except Exception as e:
        logger.error(f"Error in Resume ETL phase: {e}", exc_info=True)


def _run_matching_phase(ctx: AppContext, stop_event: threading.Event) -> None:
    """Run Matching phase."""
    logger.info("Running Matching phase")
    try:
        success, task_id = run_matching_pipeline(ctx, stop_event)
        if success:
            logger.info(f"Matching phase completed successfully (task: {task_id})")
        else:
            logger.error(f"Matching phase failed (task: {task_id})")
        if not stop_event.is_set() and ctx.job_etl_service:
            ctx.job_etl_service.unload_models()
    except Exception as e:
        logger.error(f"Error in Matching phase: {e}", exc_info=True)


def _cleanup_jobspy_client(ctx: AppContext) -> None:
    """Clean up JobSpyClient session."""
    try:
        if ctx.jobspy_client:
            ctx.jobspy_client.close()
    except Exception as e:
        logger.warning(f"Error closing JobSpy client: {e}")


def main():
    raise SystemExit(
        "The monolithic `python main.py` runtime has been removed. "
        "Start JobScout with the split microservice stack instead, for example "
        "`./scripts/setup_local_env/start.sh --split`."
    )


if __name__ == "__main__":
    main()
