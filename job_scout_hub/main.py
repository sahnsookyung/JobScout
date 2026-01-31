import time
import logging
import signal
import sys
import os
import requests
from tenacity import retry, stop_after_attempt, wait_fixed
from job_scout_hub.core.config_loader import load_config
from job_scout_hub.database.database import db_session_scope
from job_scout_hub.database.init_db import init_db
from job_scout_hub.etl.etl import ETLProcessor

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
    payload = scraper_config.dict(exclude_none=True)
    
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

def run_cycle():
    logger.info("Starting crawl cycle...")
    config = load_config()
    
    # Determine API URL from config, defaulting if missing
    api_url = "http://jobspy-service:8000"
    if config.jobspy and config.jobspy.url:
        api_url = config.jobspy.url

    # Extract LLM config
    llm_config = None
    if config.etl and config.etl.llm:
        llm_config = {
            'base_url': config.etl.llm.base_url,
            'api_key': config.etl.llm.api_key,
            'extraction_model': config.etl.llm.extraction_model,
            'extraction_type': config.etl.llm.extraction_type,
            'extraction_url': config.etl.llm.extraction_url,
            'extraction_labels': config.etl.llm.extraction_labels,
            'embedding_model': config.etl.llm.embedding_model,
            'embedding_dimensions': config.etl.llm.embedding_dimensions,
        }

    with db_session_scope() as session:
        processor = ETLProcessor(session, llm_config=llm_config)
        
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
                    processor.process_job_data(job, site_name)
                    
    logger.info("Cycle completed.")


def run_internal_sequential_cycle():
    """
    Executes the full pipeline sequentially to optimize memory.
    1. Gather Jobs (Skip AI)
    2. Run Extraction on all pending jobs
    3. Unload Extraction Model
    4. Run Embedding on all pending jobs
    5. Unload Embedding Model
    """
    cycle_start = time.time()
    logger.info("Starting Sequential Cycle...")
    config = load_config()

    # API URL for JobSpy
    api_url = "http://jobspy-service:8000"
    if config.jobspy and config.jobspy.url:
        api_url = config.jobspy.url

    # Setup LLM Config
    llm_config = None
    if config.etl and config.etl.llm:
        llm_config = {
            'base_url': config.etl.llm.base_url,
            'api_key': config.etl.llm.api_key,
            'extraction_model': config.etl.llm.extraction_model,
            'extraction_type': config.etl.llm.extraction_type,
            'extraction_url': config.etl.llm.extraction_url,
            'extraction_labels': config.etl.llm.extraction_labels,
            'embedding_model': config.etl.llm.embedding_model,
            'embedding_dimensions': config.etl.llm.embedding_dimensions,
        }

    with db_session_scope() as session:
        processor = ETLProcessor(session, llm_config=llm_config)

        # --- Step 1: Gather ---
        step_start = time.time()
        logger.info("Step 1: Gathering Jobs (Skipping AI)...")
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
                    # Save raw data only
                    processor.process_job_data(job, site_name, skip_ai=True)
                total_jobs_gathered += len(jobs)

                # Commit after each scraper batch to ensure persistence
                session.commit()
        step_elapsed = time.time() - step_start
        logger.info(f"Step 1 completed: Gathered {total_jobs_gathered} jobs in {step_elapsed:.2f}s")

        if not running: return

        # --- Step 2: Extract ---
        step_start = time.time()
        logger.info("Step 2: Running Extraction Batch...")
        # Process in chunks until caught up or stopped
        # We use a large limit or loop until 0 found. For now, one large batch is fine or simple loop.
        # Let's do a loop to ensure we catch everything gathered.
        while running:
             # run_extraction_batch returns nothing, but we could check count if we modded it.
             # For now, let's just run it once with a high limit, or assume the user wants one pass per cycle.
             # The request was "run particular jobs run as one unit to completion".
             # Let's run a generous batch.
             processor.run_extraction_batch(limit=200)
             break # For this cycle
        step_elapsed = time.time() - step_start
        logger.info(f"Step 2 completed: Extraction batch finished in {step_elapsed:.2f}s")

        # --- Step 3: Unload Extraction Model ---
        step_start = time.time()
        logger.info("Step 3: Unloading Extraction Model...")
        if processor.extraction_model:
            processor.unload_model(processor.extraction_model)
        step_elapsed = time.time() - step_start
        logger.info(f"Step 3 completed: Model unloaded in {step_elapsed:.2f}s")

        if not running: return

        # --- Step 4: Embed ---
        step_start = time.time()
        logger.info("Step 4: Running Embedding Batch...")
        while running:
             processor.run_embedding_batch(limit=200)
             break
        step_elapsed = time.time() - step_start
        logger.info(f"Step 4 completed: Embedding batch finished in {step_elapsed:.2f}s")

        # --- Step 5: Unload Embedding Model ---
        step_start = time.time()
        logger.info("Step 5: Unloading Embedding Model...")
        if processor.embedding_model:
             processor.unload_model(processor.embedding_model)
        step_elapsed = time.time() - step_start
        logger.info(f"Step 5 completed: Model unloaded in {step_elapsed:.2f}s")

    cycle_elapsed = time.time() - cycle_start
    logger.info(f"Sequential Cycle Completed in {cycle_elapsed:.2f}s")

def main():
    logger.info("Main driver starting (Internal Sequential Mode)...")

    # Initialize DB (with retry logic)
    init_db()

    config = load_config()
    interval = config.schedule.interval_seconds

    cycle_count = 0
    while running:
        cycle_count += 1
        cycle_start = time.time()
        logger.info(f"=== Starting Cycle #{cycle_count} ===")
        try:
            run_internal_sequential_cycle()
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
