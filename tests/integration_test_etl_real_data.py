import json
import logging
import os
from job_scout_hub.database.database import db_session_scope
from job_scout_hub.etl.etl import ETLProcessor
from job_scout_hub.database.models import JobPost, JobRequirementUnit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_test():
    # 1. Initialize DB
    logger.info("Initializing DB...")
    # We might need to wait for Postgres to be ready if running in docker-compose for the first time
    # But init_db has retries.
    from job_scout_hub.database.init_db import init_db
    init_db()

    # 2. Load Data
    json_path = "/app/api_responses.json"
    if not os.path.exists(json_path):
        # Fallback for local testing (running from repo root)
        if os.path.exists("api_responses.json"):
             json_path = "api_responses.json"
        elif os.path.exists("../api_responses.json"):
             json_path = "../api_responses.json"
    
    logger.info(f"Loading data from {json_path}...")
    with open(json_path, 'r') as f:
        data = json.load(f)

    # 3. Process Data
    with db_session_scope() as session:
        # Use localhost:11435 for Docker Ollama exposed to host
        llm_config = {
            "base_url": "http://localhost:11435/v1",
            "extraction_type": "ollama",
            "extraction_model": "qwen3:14b",
            "api_key": "ollama"
        }
        processor = ETLProcessor(session, mock_mode=False, llm_config=llm_config)
        
        for entry in data:
            site = entry.get('site')
            # The JSON structure has site as a string list representation "['tokyodev']" or list.
            if isinstance(site, str) and site.startswith("['"):
                 import ast
                 try:
                     site_list = ast.literal_eval(site)
                     site = site_list[0]
                 except (ValueError, SyntaxError):
                     pass
            elif isinstance(site, list):
                site = site[0]
            
            result_data = entry.get('result', {}).get('data', [])
            logger.info(f"Processing {len(result_data)} jobs for site {site}")
            
            for job in result_data:
                processor.process_job_data(job, str(site))
        
        # 4. Verify Insertion
        job_count = session.query(JobPost).count()
        req_count = session.query(JobRequirementUnit).count()
        logger.info(f"Verification: {job_count} JobPosts in DB.")
        logger.info(f"Verification: {req_count} JobRequirementUnits in DB.")

if __name__ == "__main__":
    run_test()
