import time
import logging
import signal
import sys
import os
import json
import requests
import argparse

from tenacity import retry, stop_after_attempt, wait_fixed
from core.config_loader import load_config, MatchingConfig
from database.database import db_session_scope
from database.init_db import init_db
from database.repository import JobRepository
from database.models import generate_resume_fingerprint
from core.ai_service import OpenAIService
from core.matcher_service import MatcherService
from core.scorer_service import ScoringService
from notification.service import NotificationService
from etl.orchestrator import JobETLOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)



def submit_scraping_job(scraper_config, api_url):
    """Submit a job to the JobSpy API."""
    payload = scraper_config.model_dump(exclude_none=True)
    
    # ensure is_remote is set if not present (defaulting to False for now as per test_api)
    if 'is_remote' not in payload:
        payload['is_remote'] = False
        
    logger.info(f"Submitting job for {payload.get('site_type')}")
    try:
        response = requests.post(f"{api_url}/scrape", json=payload, timeout=10)
        response.raise_for_status()
        return response.json().get("task_id")
    except Exception as e:
        logger.error(f"Failed to submit job: {e}")
        return None

def poll_job_status(task_id, api_url):
    """Poll the API for job completion."""
    waited = 0
    poll_interval = 10
    
    while running:
        try:
            response = requests.get(f"{api_url}/status/{task_id}", timeout=10)
            if response.status_code == 200:
                result = response.json()
                status = result.get("status")
                
                if status == "completed":
                    logger.info(f"Job {task_id} completed. Found {result.get('count')} jobs.")
                    return result.get("data", [])
                elif status == "failed":
                    logger.error(f"Job {task_id} failed: {result.get('error')}")
                    return None
            
        except Exception as e:
            logger.warning(f"Error checking status for {task_id}: {e}")
            
        if waited >= 300: # 5 minutes timeout per job for now
             logger.warning(f"Timeout waiting for job {task_id}")
             return None
             
        time.sleep(poll_interval)
        waited += poll_interval
    return None

def load_resume_data(resume_file_path: str) -> dict | None:
    """Load resume data from JSON file."""
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

def load_preferences_data(preferences_file_path: str) -> dict | None:
    """Load preferences data from JSON file."""
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

def run_etl_pipeline(config, session, orchestrator):
    """
    ETL Pipeline only:
    1. Gather Jobs from scrapers
    2. Run Extraction on pending jobs
    3. Unload Extraction Model
    4. Run Embedding on pending jobs
    """
    logger.info("=" * 60)
    logger.info("STARTING ETL PIPELINE")
    logger.info("=" * 60)
    
    # API URL for JobSpy
    api_url = "http://jobspy-service:8000"
    if config.jobspy and config.jobspy.url:
        api_url = config.jobspy.url
    
    # --- Step 1: Gather ---
    step_start = time.time()
    logger.info("=== ETL STEP 1: Gathering Jobs ===")
    total_jobs_gathered = 0
    for scraper_cfg in config.scrapers:
        if not running: break

        task_id = submit_scraping_job(scraper_cfg, api_url)
        if not task_id:
            continue

        jobs = poll_job_status(task_id, api_url)
        if jobs:
            site_name = str(scraper_cfg.site_type)
            logger.info(f"Processing {len(jobs)} jobs for {site_name}")
            for job in jobs:
                orchestrator.process_incoming_job(job, site_name)
            total_jobs_gathered += len(jobs)

            # Commit after each scraper batch
            session.commit()
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 1 completed: Gathered {total_jobs_gathered} jobs in {step_elapsed:.2f}s")

    if not running: return

    # --- Step 2: Extract ---
    step_start = time.time()
    logger.info("=== ETL STEP 2: Running Extraction Batch ===")
    orchestrator.run_extraction_batch(limit=200)
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 2 completed: Extraction batch finished in {step_elapsed:.2f}s")

    # --- Step 3: Unload Extraction Model ---
    step_start = time.time()
    logger.info("=== ETL STEP 3: Unloading Extraction Model ===")
    orchestrator.unload_models()
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 3 completed: Model unloaded in {step_elapsed:.2f}s")

    # --- Step 4: Embed ---
    if not running: return
    step_start = time.time()
    logger.info("=== ETL STEP 4: Running Embedding Batch ===")
    orchestrator.run_embedding_batch(limit=100)
    step_elapsed = time.time() - step_start
    logger.info(f"ETL Step 4 completed: Embedding batch finished in {step_elapsed:.2f}s")
    
    logger.info("=" * 60)
    logger.info("ETL PIPELINE COMPLETED")
    logger.info("=" * 60)


def run_matching_pipeline(config, session, repo, ai_service):
    """
    Matching Pipeline only:
    5. Load Resume & Extract Evidence (REUs)
    6. Run MatcherService (Vector Retrieval)
    7. Run ScorerService (Rule-based Scoring)  
    8. Save Matches to DB
    9. Send Notifications
    """
    logger.info("=" * 60)
    logger.info("STARTING MATCHING PIPELINE")
    logger.info("=" * 60)
    
    matching_config = config.matching
    if not matching_config or not matching_config.enabled:
        logger.info("=== MATCHING PIPELINE: Skipped (disabled in config) ===")
        return
    
    # --- Step 5: Load Resume & Extract Evidence ---
    step_start = time.time()
    logger.info("=== MATCHING STEP 5: Loading Resume & Extracting Evidence ===")
    
    # Load resume data
    resume_file = matching_config.resume_file
    if not os.path.isabs(resume_file):
        resume_file = os.path.join(os.getcwd(), resume_file)
    
    resume_data = load_resume_data(resume_file)
    if not resume_data:
        logger.error(f"Failed to load resume from {matching_config.resume_file}")
        logger.info("=== MATCHING PIPELINE: Aborted (no resume) ===")
        return
    
    # Generate fingerprint
    resume_fingerprint = generate_resume_fingerprint(resume_data)
    logger.info(f"Resume fingerprint: {resume_fingerprint[:16]}...")
    
    # Invalidate old matches if needed
    if matching_config.invalidate_on_resume_change:
        invalidated_count = repo.invalidate_matches_for_resume(
            resume_fingerprint, 
            "Resume reloaded"
        )
        if invalidated_count > 0:
            logger.info(f"Invalidated {invalidated_count} existing matches for resume changes")
    
    # Initialize MatcherService
    matcher = MatcherService(
        repo=repo,
        ai_service=ai_service,
        config=matching_config.matcher
    )
    
    # Extract and embed resume evidence
    evidence_units = matcher.extract_resume_evidence(resume_data)
    matcher.embed_evidence_units(evidence_units)
    
    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 5 completed: Extracted {len(evidence_units)} evidence units in {step_elapsed:.2f}s")
    
    if not running: return
    
    # --- Step 6: Run MatcherService ---
    step_start = time.time()
    logger.info("=== MATCHING STEP 6: Running MatcherService (Vector Retrieval) ===")
    
    # Load preferences if needed
    preferences_data = None
    preferences_file_hash = None
    if matching_config.mode == "with_preferences" and matching_config.preferences_file:
        pref_file = matching_config.preferences_file
        if not os.path.isabs(pref_file):
            pref_file = os.path.join(os.getcwd(), pref_file)
        
        preferences_data = load_preferences_data(pref_file)
        if preferences_data:
            from database.models import generate_preferences_fingerprint
            preferences_file_hash = generate_preferences_fingerprint(preferences_data)
            logger.info(f"Loaded preferences with hash: {preferences_file_hash[:16]}...")
    
    # Get jobs for matching
    jobs_to_match = matcher.get_jobs_for_matching(limit=matching_config.matcher.batch_size)
    logger.info(f"Found {len(jobs_to_match)} jobs ready for matching")
    
    # Invalidate stale matches
    if matching_config.invalidate_on_job_change:
        invalidated_total = 0
        for job in jobs_to_match:
            existing_match = repo.get_existing_match(job.id, resume_fingerprint)
            if existing_match and existing_match.calculated_at < job.last_seen_at:
                count = repo.invalidate_matches_for_job(job.id, "Job content updated")
                invalidated_total += count
        if invalidated_total > 0:
            logger.info(f"Invalidated {invalidated_total} stale matches for job updates")
    
    # Perform matching
    preliminary_matches = matcher.match_resume_to_jobs(
        evidence_units=evidence_units,
        jobs=jobs_to_match,
        resume_data=resume_data,
        preferences=preferences_data
    )
    
    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 6 completed: Matched against {len(preliminary_matches)} jobs in {step_elapsed:.2f}s")
    
    if not running: return
    
    # --- Step 7: Run ScorerService ---
    step_start = time.time()
    logger.info("=== MATCHING STEP 7: Running ScorerService (Rule-based Scoring) ===")
    
    scorer = ScoringService(repo=repo, config=matching_config.scorer)
    scored_matches = scorer.score_matches(
        preliminary_matches=preliminary_matches,
        match_type=matching_config.mode
    )
    
    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 7 completed: Scored {len(scored_matches)} matches in {step_elapsed:.2f}s")
    
    # Log top matches
    if scored_matches:
        logger.info("Top 5 Matches:")
        for i, match in enumerate(scored_matches[:5], 1):
            job = match.job
            logger.info(f"  {i}. {job.title} @ {job.company}: {match.overall_score:.1f}/100 "
                      f"(coverage: {match.required_coverage*100:.0f}% required, "
                      f"{match.preferred_coverage*100:.0f}% preferred)")
    
    if not running: return
    
    # --- Step 8: Save Matches ---
    step_start = time.time()
    logger.info("=== MATCHING STEP 8: Saving Matches to Database ===")
    
    saved_count = 0
    for scored_match in scored_matches:
        if not matching_config.recalculate_existing:
            existing = repo.get_existing_match(
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
        saved_count += 1
    
    step_elapsed = time.time() - step_start
    logger.info(f"MATCHING Step 8 completed: Saved {saved_count} matches in {step_elapsed:.2f}s")
    
    if not running: return
    
    # --- Step 9: Send Notifications ---
    notification_config = config.notifications
    if notification_config and notification_config.enabled and saved_count > 0:
        step_start = time.time()
        logger.info("=== MATCHING STEP 9: Sending Notifications ===")
        
        try:
            notification_service = NotificationService(
                repo=repo,
                redis_url=notification_config.redis_url
            )
            
            user_id = notification_config.user_id or resume_data.get('email') or 'default_user'
            
            enabled_channels = [
                channel_name for channel_name, channel_config 
                in notification_config.channels.items()
                if channel_config.enabled
            ]
            
            if enabled_channels:
                high_score_matches = [
                    match for match in scored_matches 
                    if match.overall_score >= notification_config.min_score_threshold
                ]
                
                notified_count = 0
                for scored_match in high_score_matches:
                    if notification_config.notify_on_new_match:
                        job = scored_match.job
                        match_record = repo.get_existing_match(
                            job.id, 
                            scored_match.resume_fingerprint
                        )
                        if match_record and match_record.id and not match_record.notified:
                            try:
                                notification_service.notify_new_match(
                                    user_id=user_id,
                                    match_id=str(match_record.id),
                                    job_title=job.title,
                                    company=job.company,
                                    score=float(scored_match.overall_score),
                                    location=job.location_text,
                                    is_remote=job.is_remote or False,
                                    channels=enabled_channels
                                )
                                match_record.notified = True
                                repo.db.commit()
                                notified_count += 1
                            except Exception as e:
                                logger.error(f"Failed to send notification for match {match_record.id}: {e}")
                
                if notification_config.notify_on_batch_complete:
                    try:
                        notification_service.notify_batch_complete(
                            user_id=user_id,
                            total_matches=saved_count,
                            high_score_matches=len(high_score_matches),
                            channels=enabled_channels
                        )
                    except Exception as e:
                        logger.error(f"Failed to send batch summary: {e}")
                
                step_elapsed = time.time() - step_start
                logger.info(f"MATCHING Step 9 completed: Sent {notified_count} notifications in {step_elapsed:.2f}s")
            else:
                logger.warning("No notification channels configured")
        except Exception as e:
            logger.error(f"Error in notification step: {e}", exc_info=True)
    else:
        logger.info("=== NOTIFICATION STEP: Skipped (disabled or no matches) ===")
    
    logger.info("=" * 60)
    logger.info("MATCHING PIPELINE COMPLETED")
    logger.info("=" * 60)


def run_full_pipeline(config, session):
    """
    Execute both ETL and Matching pipelines sequentially.
    """
    # Setup AI & Data Layers
    repo = JobRepository(session)
    
    llm_config = {}
    if config.etl and config.etl.llm:
        llm_config = {
            'base_url': config.etl.llm.base_url,
            'api_key': config.etl.llm.api_key,
            'model_config': {
                'extraction_model': config.etl.llm.extraction_model,
                'embedding_model': config.etl.llm.embedding_model,
            }
        }
    ai_service = OpenAIService(**llm_config)
    
    orchestrator = JobETLOrchestrator(repo, ai_service)
    
    # Run ETL Pipeline
    run_etl_pipeline(config, session, orchestrator)
    
    if not running: return
    
    # Run Matching Pipeline
    run_matching_pipeline(config, session, repo, ai_service)
    
    if not running: return
    
    # Final cleanup: Unload embedding model
    step_start = time.time()
    logger.info("=== FINAL STEP: Unloading Embedding Model ===")
    orchestrator.unload_models()
    step_elapsed = time.time() - step_start
    logger.info(f"Final cleanup completed: Model unloaded in {step_elapsed:.2f}s")


def run_internal_sequential_cycle(mode='all'):
    """
    Execute pipeline based on specified mode.
    
    Args:
        mode: 'etl', 'matching', or 'all' (default)
    """
    cycle_start = time.time()
    config = load_config()

    with db_session_scope() as session:
        repo = JobRepository(session)
        
        llm_config = {}
        if config.etl and config.etl.llm:
            llm_config = {
                'base_url': config.etl.llm.base_url,
                'api_key': config.etl.llm.api_key,
                'model_config': {
                    'extraction_model': config.etl.llm.extraction_model,
                    'embedding_model': config.etl.llm.embedding_model,
                }
            }
        ai_service = OpenAIService(**llm_config)
        
        orchestrator = JobETLOrchestrator(repo, ai_service)
        
        if mode == 'etl':
            logger.info(f"Running in ETL-ONLY mode")
            run_etl_pipeline(config, session, orchestrator)
            # Unload models after ETL
            orchestrator.unload_models()
        elif mode == 'matching':
            logger.info(f"Running in MATCHING-ONLY mode")
            run_matching_pipeline(config, session, repo, ai_service)
            # Unload models after matching
            orchestrator.unload_models()
        else:  # mode == 'all'
            logger.info(f"Running in FULL PIPELINE mode")
            run_full_pipeline(config, session)

    cycle_elapsed = time.time() - cycle_start
    logger.info(f"=== Cycle Completed in {cycle_elapsed:.2f}s ===")

def main():
    parser = argparse.ArgumentParser(description="JobScout Main Driver")
    parser.add_argument('--mode', type=str, choices=['all', 'etl', 'matching'], default='all',
                      help='Pipeline mode to run: all (default), etl, or matching')
    args = parser.parse_args()
    
    mode = args.mode
    logger.info(f"Main driver starting in {mode.upper()} mode...")
    
    if mode == 'all':
        logger.info("Pipeline: ETL (Steps 1-4) → Matching (Steps 5-9) → Cleanup (Step 10)")
    elif mode == 'etl':
        logger.info("Pipeline: ETL ONLY (Steps 1-4)")
    elif mode == 'matching':
        logger.info("Pipeline: Matching ONLY (Steps 5-9)")

    # Initialize DB (with retry logic)
    init_db()

    config = load_config()
    interval = config.schedule.interval_seconds

    cycle_count = 0
    while running:
        cycle_count += 1
        cycle_start = time.time()
        logger.info(f"=== Starting Cycle #{cycle_count} ({mode.upper()}) ===")
        try:
            run_internal_sequential_cycle(mode=mode)
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)

        cycle_elapsed = time.time() - cycle_start
        if running:
            logger.info(f"=== Cycle #{cycle_count} completed in {cycle_elapsed:.2f}s. Sleeping for {interval} seconds... ===")
            # Sleep in chunks to allow responsive shutdown
            for _ in range(interval // 5):
                if not running: break
                time.sleep(5)

if __name__ == "__main__":
    main()
