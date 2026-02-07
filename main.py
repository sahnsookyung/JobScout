"""JobScout Main Driver - Refactored with Unit of Work pattern."""

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
from etl.resume import ResumeProfiler
from database.uow import job_uow
from database.init_db import init_db



logger = logging.getLogger(__name__)

stop_event = threading.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received")
    stop_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def load_resume_data(resume_file_path: str) -> Optional[dict]:
    logger.info(f"Loading resume from {resume_file_path}")
    try:
        with open(resume_file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Resume file not found: {resume_file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in resume file: {e}")
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
        return []
    except Exception as e:
        logger.error(f"Error reading user wants file: {e}")
        return []


def run_etl_pipeline(ctx: AppContext, stop_event: threading.Event) -> None:
    logger.info("=" * 60)
    logger.info("STARTING ETL PIPELINE")
    logger.info("=" * 60)

    step_start = time.time()
    logger.info("=== ETL STEP 1: Gathering Jobs ===")
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

    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 1 completed: Gathered {total_jobs_gathered} jobs in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 2: Extraction - per-job transactions
    step_start = time.time()
    logger.info("=== ETL STEP 2: Running Extraction Batch ===")
    _run_extraction_batch(ctx, stop_event, limit=200)
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 2 completed: Extraction batch finished in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 3: Facet Extraction - per-job transactions
    step_start = time.time()
    logger.info("=== ETL STEP 3: Running Facet Extraction Batch ===")
    _run_facet_extraction_batch(ctx, stop_event, limit=100)
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 3 completed: Facet extraction batch finished in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 4: Embedding - per-job and per-requirement transactions
    step_start = time.time()
    logger.info("=== ETL STEP 4: Running Embedding Batch ===")
    _run_embedding_batch(ctx, stop_event, limit=100)
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 4 completed: Embedding batch finished in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 5: Resume Processing - with fingerprint-based change detection
    step_start = time.time()
    logger.info("=== ETL STEP 5: Processing Resume ===")
    _run_resume_etl(ctx, stop_event)
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 5 completed in {step_elapsed:.2f}s")

    logger.info("=" * 60)
    logger.info("ETL PIPELINE COMPLETED")
    logger.info("=" * 60)


def _run_resume_etl(ctx: AppContext, stop_event: threading.Event) -> None:
    """Run resume ETL with fingerprint-based change detection.

    Returns:
        None - matching pipeline will query DB for latest resume independently.
    """
    # Check if resume file is configured
    if not ctx.config.etl or not hasattr(ctx.config.etl, 'resume_file') or not ctx.config.etl.resume_file:
        logger.info("No resume file configured, skipping resume ETL")
        return

    resume_file = ctx.config.etl.resume_file
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

    success_count = 0
    for job_id in job_ids:
        if stop_event.is_set():
            break
        try:
            with job_uow() as repo:
                job = repo.get_by_id(job_id)
                if job is None:
                    logger.warning(f"Job {job_id} not found, may have been deleted")
                    continue
                ctx.job_etl_service.extract_one(repo, job)
            success_count += 1
        except Exception:
            logger.exception("Failed extraction job_id=%s", job_id)

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
    """Run the matching pipeline as a self-contained operation.
    
    This function queries the database for the latest stored resume,
    loads the raw resume data from file, performs matching and scoring,
    then saves the results. It can run standalone without requiring
    ETL to have run in the same process.
    """
    logger.info("=" * 60)
    logger.info("STARTING MATCHING PIPELINE")
    logger.info("=" * 60)

    matching_config = ctx.config.matching
    if not matching_config or not matching_config.enabled:
        logger.info("=== MATCHING PIPELINE: Skipped (disabled in config) ===")
        return

    step_start = time.time()
    logger.info("=== MATCHING STEP 6: Loading Resume & Preparing Evidence ===")

    # Step 1: Verify resume file exists
    resume_file = ctx.config.etl.resume_file if (ctx.config.etl and ctx.config.etl.resume_file) else None
    if not resume_file:
        logger.error("No resume file configured in ETL config")
        return
    
    if not os.path.isabs(resume_file):
        resume_file = os.path.join(os.getcwd(), resume_file)
    
    if not os.path.exists(resume_file):
        logger.error(f"Resume file not found: {resume_file}")
        return

    # Step 2: Load raw resume data
    resume_data = load_resume_data(resume_file)
    if not resume_data:
        logger.error("Failed to load resume data")
        return

    # Step 3: Query for latest stored resume fingerprint
    with job_uow() as repo:
        resume_fingerprint = repo.resume.get_latest_stored_resume_fingerprint()
    
    if not resume_fingerprint:
        logger.error("No resume found in database. Run ETL first.")
        return

    # Load user wants BEFORE entering UOW (AI service calls are slow, don't hold DB transaction)
    user_wants = []
    user_want_embeddings = []
    if matching_config.user_wants_file:
        wants_file = matching_config.user_wants_file
        if not os.path.isabs(wants_file):
            wants_file = os.path.join(os.getcwd(), wants_file)
        if not os.path.exists(wants_file):
            logger.warning(f"User wants file not found: {wants_file}")
        else:
            user_wants = load_user_wants_data(wants_file)
            if user_wants:
                logger.info(f"Loaded {len(user_wants)} user wants from {matching_config.user_wants_file}")
                for want_text in user_wants:
                    embedding = ctx.ai_service.generate_embedding(want_text)
                    user_want_embeddings.append(embedding)

    # Step 4: Run matching and scoring within UOW, then convert to DTOs
    job_facet_embeddings_map = {}
    match_dtos = []
    
    with job_uow() as repo:
        structured_resume = repo.resume.get_structured_resume_by_fingerprint(resume_fingerprint)

        if not structured_resume:
            logger.error(f"Resume not found in database for fingerprint: {resume_fingerprint[:16]}...")
            logger.error("Make sure ETL Step 5 (resume processing) completed successfully")
            return

        logger.info(f"Loaded resume from database (fingerprint: {resume_fingerprint[:16]}...)")
        logger.info(f"Resume experience: {structured_resume.total_experience_years} years")

        # Create matcher
        matcher = MatcherService(
            resume_profiler=ResumeProfiler(ai_service=ctx.ai_service),
            config=matching_config.matcher
        )

        step_elapsed = time.time() - step_start
        logger.info(f"MATCHING Step 6 completed: Resume loaded in {step_elapsed:.2f}s")

        if stop_event.is_set():
            return

        step_start = time.time()
        logger.info("=== MATCHING STEP 7: Running MatcherService (Vector Retrieval) ===")

        preliminary_matches = matcher.match_resume_two_stage(
            repo=repo,
            resume_data=resume_data,
        )

        step_elapsed = time.time() - step_start
        logger.info(f"MATCHING Step 7 completed: Matched against {len(preliminary_matches)} jobs in {step_elapsed:.2f}s")

        if stop_event.is_set():
            return

        step_start = time.time()
        logger.info("=== MATCHING STEP 8: Running ScorerService (Rule-based Scoring) ===")

        scorer = ScoringService(repo=repo, config=matching_config.scorer)

        if user_want_embeddings:
            logger.info("=== Using Fit/Want scoring with user wants embeddings ===")
            for preliminary in preliminary_matches:
                job_id = str(preliminary.job.id)
                if job_id not in job_facet_embeddings_map:
                    job_facet_embeddings_map[job_id] = repo.get_job_facet_embeddings(preliminary.job.id)

            scored_matches = scorer.score_matches(
                preliminary_matches=preliminary_matches,
                result_policy=matching_config.result_policy,
                user_want_embeddings=user_want_embeddings,
                job_facet_embeddings_map=job_facet_embeddings_map,
                match_type="requirements_only"
            )
        else:
            logger.info("=== Using Fit-only scoring ===")
            scored_matches = scorer.score_matches(
                preliminary_matches=preliminary_matches,
                result_policy=matching_config.result_policy,
                match_type="requirements_only"
            )

        # Convert ORM objects to DTOs before exiting UOW context
        for match in scored_matches:
            # Extract requirement matches
            requirement_matches_dtos = []
            for req in match.matched_requirements:
                evidence_dto = None
                if req.evidence:
                    evidence_dto = JobEvidenceDTO(
                        text=getattr(req.evidence, 'text', ''),
                        source_section=getattr(req.evidence, 'source_section', None),
                        tags=getattr(req.evidence, 'tags', {}),
                    )
                requirement_matches_dtos.append(RequirementMatchDTO(
                    requirement=JobRequirementDTO(
                        id=str(req.requirement.id),
                        req_type=getattr(req.requirement, 'req_type', 'required'),
                    ),
                    evidence=evidence_dto,
                    similarity=req.similarity,
                    is_covered=req.is_covered,
                ))
            
            # Extract missing requirements
            missing_requirements_dtos = []
            for req in match.missing_requirements:
                missing_requirements_dtos.append(RequirementMatchDTO(
                    requirement=JobRequirementDTO(
                        id=str(req.requirement.id),
                        req_type=getattr(req.requirement, 'req_type', 'required'),
                    ),
                    similarity=req.similarity,
                    is_covered=False,
                ))

            dto = MatchResultDTO(
                job=JobMatchDTO(
                    id=str(match.job.id),
                    title=getattr(match.job, 'title', 'Unknown'),
                    company=getattr(match.job, 'company', 'Unknown'),
                    location_text=getattr(match.job, 'location_text', ''),
                    is_remote=getattr(match.job, 'is_remote', False),
                    content_hash=getattr(match.job, 'content_hash', ''),
                ),
                    overall_score=match.overall_score if match.overall_score is not None else 0.0,
                fit_score=match.fit_score if match.fit_score is not None else 0.0,
                want_score=match.want_score if match.want_score is not None else 0.0,
                job_similarity=match.job_similarity if match.job_similarity is not None else 0.0,
                jd_required_coverage=match.jd_required_coverage,
                jd_preferences_coverage=match.jd_preferences_coverage,
                requirement_matches=requirement_matches_dtos,
                missing_requirements=missing_requirements_dtos,
                resume_fingerprint=match.resume_fingerprint,
                fit_components=getattr(match, 'fit_components', {}),
                want_components=getattr(match, 'want_components', {}),
                base_score=getattr(match, 'base_score', 0.0),
                penalties=getattr(match, 'penalties', 0.0),
                penalty_details=penalty_details_from_orm(
                    getattr(match, 'penalty_details', []),
                    total_penalties=getattr(match, 'penalties', 0.0),
                    preferences_boost=getattr(match, 'preferences_boost', 0.0)
                ),
                preferences_boost=getattr(match, 'preferences_boost', 0.0),
                fit_weight=getattr(match, 'fit_weight', 0.7),
                want_weight=getattr(match, 'want_weight', 0.3),
                match_type=getattr(match, 'match_type', 'requirements_only'),
            )
            match_dtos.append(dto)

    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 8 completed: Scored {len(match_dtos)} matches in {step_elapsed:.2f}s")

    if match_dtos:
        logger.info("Top 5 Matches:")
        for i, dto in enumerate(match_dtos[:5], 1):
            logger.info(f"  {i}. {dto.job.title} @ {dto.job.company}: overall={dto.overall_score:.1f}/100 (fit={dto.fit_score:.1f}, want={dto.want_score:.1f})")

    if stop_event.is_set():
        return

    # Step 9: Save matches with per-match transactions
    step_start = time.time()
    logger.info("=== MATCHING STEP 9: Saving Matches to Database ===")
    saved_count = _save_matches_batch(match_dtos, resume_fingerprint, matching_config)
    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 9 completed: Saved {saved_count} matches in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    # Step 10: Send notifications with per-match flag persistence
    send_notifications(ctx, match_dtos, saved_count, resume_data, resume_fingerprint, stop_event)

    logger.info("=" * 60)
    logger.info("MATCHING PIPELINE COMPLETED")
    logger.info("=" * 60)


def _save_matches_batch(scored_match_dtos: List[MatchResultDTO], resume_fingerprint: str, matching_config) -> int:
    """Save matches to database with per-match transactions.
    
    Takes DTOs instead of ORM objects to allow database session to be closed
    after the matching/scoring phase completes.
    """
    saved_count = 0
    for dto in scored_match_dtos:
        try:
            with job_uow() as repo:
                existing = repo.get_existing_match(dto.job.id, resume_fingerprint)
                
                if existing and existing.status == 'active':
                    if existing.job_content_hash != dto.job.content_hash:
                        existing.status = 'stale'
                        existing.invalidated_reason = "Job content updated"
                        logger.info(f"Invalidated match for job {dto.job.id} due to content change")
                        # Continue to save new match below (do not skip)
                    elif not matching_config.recalculate_existing:
                        logger.debug(f"Skipping existing match for job {dto.job.id}")
                        continue

                save_match_to_db(
                    scored_match=dto,
                    repo=repo,
                )
                saved_count += 1
        except Exception:
            logger.exception("Failed saving match job_id=%s", dto.job.id)

    return saved_count


def send_notifications(
    ctx: AppContext,
    scored_match_dtos: List[MatchResultDTO],
    saved_count: int,
    resume_data: dict,
    resume_fingerprint: str,
    stop_event: threading.Event
) -> None:
    """Send notifications for scored matches.
    
    Takes DTOs instead of ORM objects to work with data extracted
    after the matching/scoring phase completed.
    """
    notification_config = ctx.config.notifications

    if not notification_config or not notification_config.enabled:
        logger.info("=== NOTIFICATION STEP: Skipped (disabled in config) ===")
        return

    if saved_count == 0:
        logger.info("=== NOTIFICATION STEP: Skipped (no matches to notify) ===")
        return

    if not ctx.notification_service:
        logger.warning("=== NOTIFICATION STEP: No notification service available ===")
        return

    step_start = time.time()
    logger.info("=== MATCHING STEP 9: Sending Notifications ===")

    try:
        user_id = notification_config.user_id or resume_data.get('email') or 'default_user'

        enabled_channels = [
            channel_name for channel_name, channel_config
            in notification_config.channels.items()
            if channel_config.enabled
        ]

        if not enabled_channels:
            logger.warning("No notification channels configured")
            return

        high_score_matches = [
            dto for dto in scored_match_dtos
            if dto.overall_score is not None and dto.overall_score >= notification_config.min_score_threshold
        ]

        notified_count = 0
        for dto in high_score_matches:
            if stop_event.is_set():
                break

            if notification_config.notify_on_new_match:
                job_dto = dto.job

                # Get match record first
                try:
                    with job_uow() as repo:
                        match_record = repo.get_existing_match(
                            job_dto.id,
                            resume_fingerprint
                        )

                        if not match_record or not match_record.id:
                            logger.warning(f"No match record found for job {job_dto.id}, skipping notification")
                            continue

                        if match_record.notified:
                            logger.debug(f"Match already notified for job {job_dto.id}, skipping")
                            continue

                        match_id = match_record.id
                except Exception:
                    logger.exception("Failed to get match record for job_id=%s", job_dto.id)
                    continue

                # Send notification FIRST (at-least-once semantics)
                # Notification service deduplicates via should_send_notification()
                try:
                    ctx.notification_service.notify_new_match(
                        user_id=user_id,
                        match_id=str(match_id),
                        job_title=job_dto.title,
                        company=job_dto.company,
                        score=float(dto.overall_score),
                        location=job_dto.location_text,
                        is_remote=job_dto.is_remote,
                        channels=enabled_channels
                    )
                    notified_count += 1
                except Exception as e:
                    # If send fails, don't persist flag - will retry next cycle
                    logger.error(f"Failed to send notification for match {match_id}: {e}")
                    continue

                # Persist notified flag AFTER successful send
                # If this fails, notification was sent but flag not persisted
                # On retry, notification service deduplication prevents duplicate
                try:
                    with job_uow() as repo:
                        match_record = repo.get_existing_match(job_dto.id, resume_fingerprint)
                        if match_record:
                            match_record.notified = True
                except Exception as e:
                    logger.error(f"Failed to persist notified flag for match {match_id}: {e}")
                    # Continue - at-least-once means we accept this inconsistency

        if notification_config.notify_on_batch_complete:
            try:
                # NotificationService handles sync vs async internally
                ctx.notification_service.notify_batch_complete(
                    user_id=user_id,
                    total_matches=saved_count,
                    high_score_matches=len(high_score_matches),
                    channels=enabled_channels
                )
            except Exception as e:
                logger.error(f"Failed to send batch summary: {e}")

        step_elapsed = time.time() - step_start
        logger.info(f"MATCHING Step 9 completed: Sent {notified_count} notifications in {step_elapsed:.2f}s")
    except Exception as e:
        logger.error(f"Error in notification step: {e}", exc_info=True)


def run_internal_sequential_cycle(mode: str = 'all', stop_event: threading.Event = None, config=None) -> None:
    if stop_event is None:
        stop_event = threading.Event()

    if config is None:
        config = load_config()

    cycle_start = time.time()

    # Build context once - no DB session attached
    ctx = AppContext.build(config)

    # ETL Phase
    if mode in ('etl', 'all'):
        logger.info("Running ETL phase")
        try:
            run_etl_pipeline(ctx, stop_event)
            if not stop_event.is_set() and ctx.job_etl_service:
                ctx.job_etl_service.unload_models()
        except Exception as e:
            logger.error(f"Error in ETL phase: {e}", exc_info=True)

        if stop_event.is_set():
            logger.info("Shutdown requested after ETL phase")
            # Clean up JobSpyClient session for ETL phase
            try:
                if ctx.jobspy_client:
                    ctx.jobspy_client.close()
            except Exception as e:
                logger.warning(f"Error closing JobSpy client: {e}")
            return

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
    parser.add_argument('--mode', type=str, choices=['all', 'etl', 'matching'], default='all',
                      help='Pipeline mode to run: all (default), etl, or matching')
    args = parser.parse_args()

    mode = args.mode
    logger.info(f"Main driver starting in {mode.upper()} mode...")

    if mode == 'all':
        logger.info("Pipeline: ETL (Steps 1-4) -> Matching (Steps 5-9)")
    elif mode == 'etl':
        logger.info("Pipeline: ETL ONLY (Steps 1-4)")
    elif mode == 'matching':
        logger.info("Pipeline: Matching ONLY (Steps 5-9)")

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
