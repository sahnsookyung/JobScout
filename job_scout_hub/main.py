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
    
    # Determine Mock Mode
    mock_mode = True
    if config.etl and config.etl.mock is False:
        mock_mode = False
    
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
        processor = ETLProcessor(session, mock_mode=mock_mode, llm_config=llm_config)
        
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

def main():
    logger.info("Main driver starting...")
    
    # Initialize DB (with retry logic)
    init_db()
    
    config = load_config()
    interval = config.schedule.interval_seconds
    
    while running:
        try:
            run_cycle()
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
        
        if running:
            logger.info(f"Sleeping for {interval} seconds...")
            # Sleep in chunks to allow responsive shutdown
            for _ in range(interval // 5):
                if not running: break
                time.sleep(5)

if __name__ == "__main__":
    main()
