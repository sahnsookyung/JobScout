import json
import logging
import os
import ast
from database.database import db_session_scope
from database.repository import JobRepository
from core.llm.openai_service import OpenAIService
from etl.orchestrator import JobETLService
from database.models import JobPost, JobRequirementUnit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_test():
    # 1. Initialize DB
    logger.info("Initializing DB...")
    from database.init_db import init_db
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
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 3. Process Data
    with db_session_scope() as session:
        # Layers
        repo = JobRepository(session)
        llm_config = {
            "base_url": "http://localhost:11434/v1",
            "api_key": "ollama",
            "model_config": {
                "extraction_model": "qwen3:14b"
            }
        }
        ai_service = OpenAIService(**llm_config)
        etl_service = JobETLService(ai_service=ai_service)

        for entry in data:
            site = entry.get('site')
            # The JSON structure has site as a string list representation "['tokyodev']" or list.
            if isinstance(site, str) and site.startswith("["):
                 try:
                     site_list = ast.literal_eval(site)
                     site = site_list[0] if site_list else None
                 except (ValueError, SyntaxError):
                     logger.warning(f"Failed to parse site string: {site}")
                     site = None
            elif isinstance(site, list):
                 site = site[0] if site else None

            if site is None:
                logger.warning("Skipping entry with invalid site")
                continue

            result_data = entry.get('result', {}).get('data', [])
            logger.info(f"Processing {len(result_data)} jobs for site {site}")

            for job in result_data:
                # Use ETL service to process incoming job (repo passed directly)
                etl_service.ingest_one(repo, job, str(site))
        
        # 4. Verify Insertion
        job_count = session.query(JobPost).count()
        req_count = session.query(JobRequirementUnit).count()
        logger.info(f"Verification: {job_count} JobPosts in DB.")
        logger.info(f"Verification: {req_count} JobRequirementUnits in DB.")

if __name__ == "__main__":
    run_test()
