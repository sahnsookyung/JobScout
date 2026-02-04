"""JobScout Main Driver - Refactored with SOLID-lite principles."""

import time
import logging
import signal
import sys
import os
import json
import argparse
import threading
from typing import Optional, List

from core.config_loader import load_config
from core.app_context import AppContext
from core.matcher import MatcherService
from core.scorer import ScoringService
from database.database import db_session_scope
from database.init_db import init_db
from database.models import generate_resume_fingerprint, generate_preferences_fingerprint
from core.job_cache import init_job_cache, get_job_cache

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


def load_preferences_data(preferences_file_path: str) -> Optional[dict]:
    logger.info(f"Loading preferences from {preferences_file_path}")
    try:
        with open(preferences_file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Preferences file not found: {preferences_file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in preferences file: {e}")
        return None


def load_user_wants_data(wants_file_path: str) -> List[str]:
    """
    Load user wants from a file.
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
                    ctx.orchestrator.process_incoming_job(job, site_name)
                total_jobs_gathered += len(jobs)
        except Exception as e:
            logger.error(f"Error processing scraper {scraper_cfg.site_type}: {e}")

    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 1 completed: Gathered {total_jobs_gathered} jobs in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    step_start = time.time()
    logger.info("=== ETL STEP 2: Running Extraction Batch ===")
    ctx.orchestrator.run_extraction_batch(limit=200)
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 2 completed: Extraction batch finished in {step_elapsed:.2f}s")

    step_start = time.time()
    logger.info("=== ETL STEP 3: Unloading Extraction Model ===")
    ctx.orchestrator.unload_models()
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 3 completed: Model unloaded in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    step_start = time.time()
    logger.info("=== ETL STEP 4: Running Embedding Batch ===")
    ctx.orchestrator.run_embedding_batch(limit=100)
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 4 completed: Embedding batch finished in {step_elapsed:.2f}s")

    logger.info("=" * 60)
    logger.info("ETL PIPELINE COMPLETED")
    logger.info("=" * 60)


def run_matching_pipeline(ctx: AppContext, stop_event: threading.Event) -> None:
    logger.info("=" * 60)
    logger.info("STARTING MATCHING PIPELINE")
    logger.info("=" * 60)

    matching_config = ctx.config.matching
    if not matching_config or not matching_config.enabled:
        logger.info("=== MATCHING PIPELINE: Skipped (disabled in config) ===")
        return

    step_start = time.time()
    logger.info("=== MATCHING STEP 5: Loading Resume & Extracting Evidence ===")

    resume_file = matching_config.resume_file
    if not os.path.isabs(resume_file):
        resume_file = os.path.join(os.getcwd(), resume_file)

    resume_data = load_resume_data(resume_file)
    if not resume_data:
        logger.error(f"Failed to load resume from {matching_config.resume_file}")
        return

    resume_fingerprint = generate_resume_fingerprint(resume_data)
    logger.info(f"Resume fingerprint: {resume_fingerprint[:16]}...")

    if matching_config.invalidate_on_resume_change:
        invalidated_count = ctx.repo.invalidate_matches_for_resume(
            resume_fingerprint,
            "Resume reloaded"
        )
        if invalidated_count > 0:
            logger.info(f"Invalidated {invalidated_count} existing matches for resume changes")

    matcher = MatcherService(
        repo=ctx.repo,
        ai_service=ctx.ai_service,
        config=matching_config.matcher
    )

    evidence_units = matcher.extract_resume_evidence(resume_data)
    
    # NEW: Extract comprehensive structured resume profile
    structured_profile = matcher.extract_structured_resume(resume_data)
    if structured_profile:
        years_msg = f"Total experience: {structured_profile.calculated_total_years} years"
        if structured_profile.claimed_total_years:
            years_msg += f" (claimed: {structured_profile.claimed_total_years})"
        logger.info(years_msg)
        
        # Save structured resume to database for later retrieval during scoring
        is_valid, validation_msg = structured_profile.validate_experience_claim()
        ctx.repo.save_structured_resume(
            resume_fingerprint=resume_fingerprint,
            extracted_data=structured_profile.raw_data,
            calculated_total_years=structured_profile.calculated_total_years,
            claimed_total_years=structured_profile.claimed_total_years,
            experience_validated=is_valid,
            validation_message=validation_msg,
            extraction_confidence=structured_profile.raw_data.get('extraction', {}).get('confidence'),
            extraction_warnings=structured_profile.raw_data.get('extraction', {}).get('warnings', [])
        )
        logger.info(f"Saved structured resume to database")
    
    # Legacy: Also extract years from individual evidence units (for backward compatibility)
    matcher.extract_years_for_evidence(evidence_units)
    units_with_years = [u for u in evidence_units if u.years_value is not None]
    total_years_units = [u for u in units_with_years if u.is_total_years_claim]
    logger.info(f"Also extracted years from {len(units_with_years)} evidence units ({len(total_years_units)} total claims)")
    
    matcher.embed_evidence_units(evidence_units)

    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 5 completed: Extracted {len(evidence_units)} evidence units in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    step_start = time.time()
    logger.info("=== MATCHING STEP 6: Running MatcherService (Vector Retrieval) ===")

    preferences_data = None
    preferences_file_hash = None
    if matching_config.mode == "with_preferences" and matching_config.preferences_file:
        pref_file = matching_config.preferences_file
        if not os.path.isabs(pref_file):
            pref_file = os.path.join(os.getcwd(), pref_file)

        preferences_data = load_preferences_data(pref_file)
        if preferences_data:
            preferences_file_hash = generate_preferences_fingerprint(preferences_data)
            logger.info(f"Loaded preferences with hash: {preferences_file_hash[:16]}...")

    jobs_to_match = matcher.get_jobs_for_matching(limit=matching_config.matcher.batch_size)
    logger.info(f"Found {len(jobs_to_match)} jobs ready for matching")

    if matching_config.invalidate_on_job_change:
        invalidated_total = 0
        for job in jobs_to_match:
            existing_match = ctx.repo.get_existing_match(job.id, resume_fingerprint)
            # Only invalidate if job content actually changed (via content_hash comparison)
            if existing_match and existing_match.job_content_hash != job.content_hash:
                count = ctx.repo.invalidate_matches_for_job(job.id, "Job content updated")
                invalidated_total += count
        if invalidated_total > 0:
            logger.info(f"Invalidated {invalidated_total} stale matches for job updates")

    preliminary_matches = matcher.match_resume_to_jobs(
        evidence_units=evidence_units,
        jobs=jobs_to_match,
        resume_data=resume_data,
        preferences=preferences_data
    )

    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 6 completed: Matched against {len(preliminary_matches)} jobs in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    step_start = time.time()
    logger.info("=== MATCHING STEP 7: Running ScorerService (Rule-based Scoring) ===")

    scorer = ScoringService(repo=ctx.repo, config=matching_config.scorer)

    user_wants = []
    user_want_embeddings = []
    if hasattr(matching_config, 'user_wants_file') and matching_config.user_wants_file:
        wants_file = matching_config.user_wants_file
        if not os.path.isabs(wants_file):
            wants_file = os.path.join(os.getcwd(), wants_file)
        user_wants = load_user_wants_data(wants_file)
        if user_wants:
            logger.info(f"Loaded {len(user_wants)} user wants from {matching_config.user_wants_file}")
            for want_text in user_wants:
                embedding = ctx.ai_service.generate_embedding(want_text)
                user_want_embeddings.append(embedding)

    if user_want_embeddings:
        logger.info("=== Using Fit/Want scoring with user wants embeddings ===")
        job_facet_embeddings_map = {}
        for preliminary in preliminary_matches:
            job_id = str(preliminary.job.id)
            if job_id not in job_facet_embeddings_map:
                job_facet_embeddings_map[job_id] = ctx.repo.get_job_facet_embeddings(preliminary.job.id)

        scored_matches = scorer.score_matches(
            preliminary_matches=preliminary_matches,
            result_policy=matching_config.result_policy,
            user_want_embeddings=user_want_embeddings,
            job_facet_embeddings_map=job_facet_embeddings_map,
            match_type=matching_config.mode
        )
    else:
        logger.info("=== Using Fit-only scoring ===")
        scored_matches = scorer.score_matches(
            preliminary_matches=preliminary_matches,
            result_policy=matching_config.result_policy,
            match_type=matching_config.mode
        )

    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 7 completed: Scored {len(scored_matches)} matches in {step_elapsed:.2f}s")

    if scored_matches:
        logger.info("Top 5 Matches:")
        for i, match in enumerate(scored_matches[:5], 1):
            job = match.job
            logger.info(f"  {i}. {job.title} @ {job.company}: overall={match.overall_score:.1f}/100 (fit={match.fit_score:.1f}, want={match.want_score:.1f})")

    if stop_event.is_set():
        return

    step_start = time.time()
    logger.info("=== MATCHING STEP 8: Saving Matches to Database ===")

    saved_count = 0
    for scored_match in scored_matches:
        if not matching_config.recalculate_existing:
            existing = ctx.repo.get_existing_match(
                scored_match.job.id,
                scored_match.resume_fingerprint
            )
            if existing and existing.status == 'active':
                logger.debug(f"Skipping existing match for job {scored_match.job.id}")
                continue

        match_record = scorer.save_match_to_db(
            scored_match=scored_match,
            preferences_file_hash=preferences_file_hash
        )
        if match_record:
            saved_count += 1

    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 8 completed: Saved {saved_count} matches in {step_elapsed:.2f}s")

    if stop_event.is_set():
        return

    send_notifications(ctx, scored_matches, saved_count, resume_data)

    logger.info("=" * 60)
    logger.info("MATCHING PIPELINE COMPLETED")
    logger.info("=" * 60)


def send_notifications(ctx: AppContext, scored_matches, saved_count: int, resume_data: dict) -> None:
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
            match for match in scored_matches 
            if match.overall_score >= notification_config.min_score_threshold
        ]
        
        notified_count = 0
        for scored_match in high_score_matches:
            if notification_config.notify_on_new_match:
                job = scored_match.job
                match_record = ctx.repo.get_existing_match(
                    job.id, 
                    scored_match.resume_fingerprint
                )
                if match_record and match_record.id and not match_record.notified:
                    # Set notified flag BEFORE sending to ensure at-most-once delivery
                    match_record.notified = True
                    
                    # Commit immediately - if this fails, we never sent notification
                    try:
                        ctx.session.commit()
                        logger.debug(f"Persisted notified flag for match {match_record.id}")
                    except Exception as commit_err:
                        logger.error(f"Failed to persist notified flag for match {match_record.id}, skipping notification: {commit_err}")
                        # Don't send notification if we couldn't persist the flag
                        continue
                    
                    # Only send notification AFTER successful commit
                    # This ensures at-most-once delivery
                    try:
                        ctx.notification_service.notify_new_match(
                            user_id=user_id,
                            match_id=str(match_record.id),
                            job_title=job.title,
                            company=job.company,
                            score=float(scored_match.overall_score),
                            location=job.location_text,
                            is_remote=job.is_remote or False,
                            channels=enabled_channels
                        )
                        notified_count += 1
                    except Exception as e:
                        # Log error but don't try to unset flag - we accepted at-most-once
                        logger.error(f"Failed to send notification for match {match_record.id}: {e}")
        
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
    
    # ETL Phase with its own transaction scope
    if mode in ('etl', 'all'):
        logger.info("Running ETL phase with isolated transaction")
        etl_ctx = None
        with db_session_scope() as session:
            etl_ctx = AppContext.build(config, session)
            # Initialize job cache (separate from notification Redis)
            if config.cache and config.cache.enabled:
                init_job_cache(
                    redis_url=config.cache.redis_url,
                    password=config.cache.password
                )
                cache = get_job_cache()
                if cache and cache.is_available:
                    stats = cache.get_cache_stats()
                    logger.info(f"Job cache initialized: {stats.get('ttl_human', '2 weeks')} TTL")
                else:
                    logger.warning("Job cache enabled but Redis unavailable")
            run_etl_pipeline(etl_ctx, stop_event)
            if not stop_event.is_set():
                etl_ctx.orchestrator.unload_models()
            # Session commits here on successful exit
        
        # Clean up JobSpyClient session for ETL phase
        if etl_ctx and etl_ctx.jobspy_client:
            etl_ctx.jobspy_client.close()
        
        if stop_event.is_set():
            logger.info("Shutdown requested after ETL phase")
            return
    
    # Matching Phase with its own transaction scope
    if mode in ('matching', 'all'):
        logger.info("Running Matching phase with isolated transaction")
        matching_ctx = None
        with db_session_scope() as session:
            matching_ctx = AppContext.build(config, session)
            # Initialize job cache (separate from notification Redis)
            if config.cache and config.cache.enabled:
                init_job_cache(
                    redis_url=config.cache.redis_url,
                    password=config.cache.password
                )
                cache = get_job_cache()
                if cache and cache.is_available:
                    stats = cache.get_cache_stats()
                    logger.info(f"Job cache initialized: {stats.get('ttl_human', '2 weeks')} TTL")
                else:
                    logger.warning("Job cache enabled but Redis unavailable")
            run_matching_pipeline(matching_ctx, stop_event)
            if not stop_event.is_set():
                matching_ctx.orchestrator.unload_models()
            # Session commits here on successful exit
        
        # Clean up JobSpyClient session for Matching phase
        if matching_ctx and matching_ctx.jobspy_client:
            matching_ctx.jobspy_client.close()
    
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

