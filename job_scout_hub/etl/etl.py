import hashlib
import json
import requests
import logging
import os
import time
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy import select
from job_scout_hub.database.models import JobPost, JobPostSource, JobPostContent, JobRequirementUnit, JobRequirementUnitEmbedding
from job_scout_hub.database.database import db_session_scope
from openai import OpenAI
from job_scout_hub.etl.schemas import EXTRACTION_SCHEMA

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLProcessor:
    def __init__(self, db: Session, llm_config: Optional[Dict[str, Any]] = None):
        self.db = db
        llm_config = llm_config or {}
        
        api_key = llm_config.get('api_key') or os.environ.get("OPENAI_API_KEY")
        base_url = llm_config.get('base_url')
        
        # Extraction configuration
        self.extraction_type = llm_config.get('extraction_type', 'openai')
        self.extraction_model = llm_config.get('extraction_model')
        
        if not self.extraction_model:
            self.extraction_model = 'qwen3:14b' if self.extraction_type == 'ollama' else Exception("No extraction model specified")

        self.extraction_labels = llm_config.get('extraction_labels', [])
        
        # Embedding configuration
        self.embedding_model = llm_config.get('embedding_model', 'qwen3-embedding:4b')
        self.embedding_dimensions = llm_config.get('embedding_dimensions', 1024)
        
        # Create client with optional base_url for local models
        client_kwargs = {}
        if api_key:
            client_kwargs['api_key'] = api_key
        if base_url:
            client_kwargs['base_url'] = base_url
            
        self.openai_client = OpenAI(**client_kwargs)

    def calculate_canonical_fingerprint(self, company: str, title: str, location_text: str) -> str:
        """
        Create a deterministic hash of the core immutable fields.
        Formula: SHA256(lowercase(Company) + lowercase(JobTitle) + lowercase(City/Location))
        """
        raw_string = f"{company.lower().strip()}|{title.lower().strip()}|{location_text.lower().strip()}"
        return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()

    def get_existing_job(self, fingerprint: str) -> Optional[JobPost]:
        stmt = select(JobPost).where(JobPost.canonical_fingerprint == fingerprint)
        result = self.db.execute(stmt).scalar_one_or_none()
        return result

    def extract_requirements_openai(self, description: str) -> Dict[str, Any]:
        """
        Extract requirements using LLM with structured JSON output.
        """
        start_time = time.time()
        try:
            # Prepare messages
            messages = [
                {"role": "system", "content": "You are a helpful assistant that extracts structured data from job descriptions."},
                {"role": "user", "content": f"Extract job requirements from the following job description into the requested JSON format.\n\nDescription:\n{description}"}
            ]

            # Different clients might handle 'response_format' differently.
            # OpenAI standard uses response_format={"type": "json_object"} or json_schema (in newer versions).
            # Ollama (via OpenAI client) supports 'format'="json" or guided decoding if using vLLM etc.
            # Given user request sample code: "extra_body": {"guided_json": json_schema}
            # The standard OpenAI library passing 'response_format' with schema is the most compatible way if the backend supports it.
            # If using standard Ollama, 'format="json"' enforces VALID JSON but not necessarily SCHEMA.
            # However, the user provided sample suggests they might be using vLLM or similar that supports 'guided_json'.
            # OR they want us to use standard structured outputs.
            # We will try the standard 'json_schema' approach for 'response_format' if supported,
            # or 'extra_body' as requested for specific backends.

            # Use 'extra_body' for vLLM/Ollama compatible guided decoding if needed,
            # OR standard response_format for OpenAI/compatible endpoints.

            # Attempting standard structured output (OpenAI compatible) first which works well with modern Ollama/vLLM

            response = self.openai_client.chat.completions.create(
                model=self.extraction_model,
                messages=messages,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "extraction_response",
                        "schema": EXTRACTION_SCHEMA
                    }
                },
                # Fallback specific params if the above doesn't work depends on the backend version
            )
            content = response.choices[0].message.content
            data = json.loads(content)

            # Ensure requirements list exists
            if 'requirements' not in data:
                data['requirements'] = []

            # Add ordinal
            for i, req in enumerate(data['requirements']):
                req['ordinal'] = i

            elapsed = time.time() - start_time
            logger.info(f"Extraction completed in {elapsed:.2f}s - extracted {len(data['requirements'])} requirements")
            return data

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"LLM extraction failed after {elapsed:.2f}s: {e}")
            raise

    def generate_embedding_openai(self, text: str) -> List[float]:
        start_time = time.time()
        try:
            response = self.openai_client.embeddings.create(
                input=text,
                model=self.embedding_model,
                dimensions=self.embedding_dimensions
            )
            result = response.data[0].embedding
            elapsed = time.time() - start_time
            logger.debug(f"Embedding generated in {elapsed:.2f}s - {len(result)} dimensions")
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Embedding failed after {elapsed:.2f}s: {e}")
            raise

    def _normalize_location(self, location: Any) -> str:
        location_text = "Unknown"
        if isinstance(location, dict):
            location_text = location.get('city') or location.get('country') or "Unknown"
            if isinstance(location_text, list): # Handle ["japan", "jp"]
                location_text = location_text[0]
        elif isinstance(location, str):
            location_text = location
        return str(location_text)

    def _get_or_create_job_post(self, title: str, company: str, location_text: str, job_data: Dict[str, Any]) -> JobPost:
        fingerprint = self.calculate_canonical_fingerprint(company, title, location_text)
        job_post = self.get_existing_job(fingerprint)
        
        if job_post:
            logger.info(f"Duplicate found for {title} at {company}. ID: {job_post.id}")
            job_post.last_seen_at = func.now()
        else:
            logger.info(f"New job found: {title} at {company}")
            job_post = JobPost(
                title=title,
                company=company,
                location_text=location_text,
                is_remote=job_data.get('is_remote'),
                canonical_fingerprint=fingerprint,
            )
            self.db.add(job_post)
            self.db.flush()
        return job_post
    
    def _get_or_create_source(self, job_post_id: Any, site_name: str, job_data: Dict[str, Any]):
        job_url = job_data.get('job_url')
        existing_source = self.db.execute(
            select(JobPostSource).where(
                JobPostSource.site == site_name,
                JobPostSource.job_url == job_url
            )
        ).scalar_one_or_none()
        
        if not existing_source:
             new_source = JobPostSource(
                 job_post_id=job_post_id,
                 site=site_name,
                 job_url=job_url,
                 job_url_direct=job_data.get('job_url_direct'),
                 date_posted=None,
             )
             self.db.add(new_source)

    def _get_or_create_content(self, job_post_id: Any, job_data: Dict[str, Any], skip_ai: bool = False):
        """
        Extracts and embeds requirements. Skips extraction if content already exists.
        """
        existing_content = self.db.execute(
            select(JobPostContent).where(JobPostContent.job_post_id == job_post_id)
        ).scalar_one_or_none()
        
        if not existing_content:
            content = JobPostContent(
                job_post_id=job_post_id,
                description=job_data.get('description'),
                skills_raw=json.dumps(job_data.get('skills')) if job_data.get('skills') else None,
                raw_payload=job_data
            )
            self.db.add(content)
            
            if not skip_ai:
                self._extract_and_embed_requirements(job_post_id, job_data)
            else:
                self.db.flush() # Ensure content is saved even if skipping AI

    def _extract_requirements(self, job_post: JobPost, description: str) -> List[Dict[str, Any]]:
        """
        Extract structured data from job description using LLM.
        Updates job_post with structural fields (min_years_experience, etc.).
        Returns list of requirement dictionaries.
        """
        extracted_data = self.extract_requirements_openai(description)

        # Update structural fields on job_post
        job_post.min_years_experience = extracted_data.get('min_years_experience')
        job_post.requires_degree = extracted_data.get('requires_degree')
        job_post.security_clearance = extracted_data.get('security_clearance')

        return extracted_data.get('requirements', [])

    def _embed_job_and_requirements(self, job_post: JobPost, description: str, requirements: List[Dict[str, Any]]):
        """
        Generate embeddings for job post and requirements.
        Creates coarse embedding for JobPost and contextualized embeddings for each requirement.
        """
        # Generate Coarse Embedding (Whole Job Context)
        coarse_text = f"{job_post.title} at {job_post.company}: {description[:1000]}"
        job_post.summary_embedding = self.generate_embedding_openai(coarse_text)

        # Create requirement units and embeddings
        for req in requirements:
            jru = JobRequirementUnit(
                job_post_id=job_post.id,
                req_type=req.get('req_type', 'required'),
                text=req.get('text', ''),
                tags={'skills': req.get('skills', [])},
                ordinal=req.get('ordinal', 0)
            )
            self.db.add(jru)
            self.db.flush()

            # Contextualize Requirement Embedding
            context_text = f"Job Role: {job_post.title} at {job_post.company}. Requirement: {req.get('text', '')}"
            vector = self.generate_embedding_openai(context_text)

            embedding = JobRequirementUnitEmbedding(
                job_requirement_unit_id=jru.id,
                embedding=vector
            )
            self.db.add(embedding)

    def _extract_and_embed_requirements(self, job_post_id: Any, job_data: Dict[str, Any]):
        """
        Main orchestration method for extraction and embedding.
        Calls _extract_requirements and _embed_job_and_requirements in sequence.
        """
        description = job_data.get('description')
        if not description:
            return

        # Get JobPost for updating and context
        job_post = self.db.execute(select(JobPost).where(JobPost.id == job_post_id)).scalar_one()

        # Step 1: Extract requirements and update structural fields
        requirements = self._extract_requirements(job_post, description)

        # Step 2: Generate embeddings
        self._embed_job_and_requirements(job_post, description, requirements)

    def process_job_data(self, job_data: Dict[str, Any], site_name: str, skip_ai: bool = False):
        """
        Main entry point for a single job entry from scraper.
        """
        title = job_data.get('title')
        company = job_data.get('company_name')
        if not title or not company:
            logger.warning("Skipping job with missing title or company")
            return

        location_text = self._normalize_location(job_data.get('location'))
        job_post = self._get_or_create_job_post(title, company, location_text, job_data)
        self._get_or_create_source(job_post.id, site_name, job_data)
        
        # Always create content (it stores the raw description)
        self._get_or_create_content(job_post.id, job_data, skip_ai=skip_ai)

    def run_extraction_batch(self, limit: int = 100):
        """
        Scan for jobs that have content but no key structural fields (e.g. min_years_experience is None).
        run extraction on them.
        """
        batch_start = time.time()
        logger.info("Starting extraction batch...")
        # Find jobs where description exists but extracted fields are null
        # We'll use 'min_years_experience' as a proxy for "not extracted yet"
        # Since we initialized it to NULL in schema.
        stmt = select(JobPost).join(JobPostContent).where(
            JobPost.min_years_experience == None,
            JobPostContent.description != None
        ).limit(limit)

        jobs_to_process = self.db.execute(stmt).scalars().all()
        logger.info(f"Found {len(jobs_to_process)} jobs needing extraction")

        success_count = 0
        for job in jobs_to_process:
            job_start = time.time()
            try:
                # Re-fetch content to get description
                content = self.db.execute(
                    select(JobPostContent).where(JobPostContent.job_post_id == job.id)
                ).scalar_one()

                logger.info(f"Extracting for job {job.id}: {job.title}")
                self._extract_requirements(job, content.description)
                self.db.commit() # Commit after each successful extraction
                success_count += 1
                job_elapsed = time.time() - job_start
                logger.info(f"Job {job.id} extraction completed and committed in {job_elapsed:.2f}s")
            except Exception as e:
                job_elapsed = time.time() - job_start
                logger.error(f"Failed to extract for job {job.id} after {job_elapsed:.2f}s: {e}")
                self.db.rollback()

        batch_elapsed = time.time() - batch_start
        logger.info(f"Extraction batch completed: {success_count}/{len(jobs_to_process)} jobs processed in {batch_elapsed:.2f}s")

    def run_embedding_batch(self, limit: int = 100):
        """
        Scan for jobs/requirements that need embeddings.
        1. Jobs with no summary_embedding
        2. RequirementUnits with no embeddings
        """
        batch_start = time.time()
        logger.info("Starting embedding batch...")

        # 1. Jobs missing summary embedding
        stmt_jobs = select(JobPost).join(JobPostContent).where(
            JobPost.summary_embedding == None,
            JobPostContent.description != None
        ).limit(limit)
        jobs = self.db.execute(stmt_jobs).scalars().all()

        logger.info(f"Found {len(jobs)} jobs needing summary embedding")
        job_success_count = 0
        for job in jobs:
            job_start = time.time()
            try:
                content = self.db.execute(
                    select(JobPostContent).where(JobPostContent.job_post_id == job.id)
                ).scalar_one()

                coarse_text = f"{job.title} at {job.company}: {content.description[:1000]}"
                job.summary_embedding = self.generate_embedding_openai(coarse_text)
                self.db.commit()
                job_success_count += 1
                job_elapsed = time.time() - job_start
                logger.info(f"Job {job.id} embedding completed and committed in {job_elapsed:.2f}s")
            except Exception as e:
                job_elapsed = time.time() - job_start
                logger.error(f"Failed job embedding {job.id} after {job_elapsed:.2f}s: {e}")
                self.db.rollback()

        # 2. Requirements missing embeddings
        # We need to join back to JobPost to get context (Title/Company)
        # Check for non-existent embedding relation
        stmt_reqs = select(JobRequirementUnit).outerjoin(JobRequirementUnitEmbedding).where(
            JobRequirementUnitEmbedding.id == None
        ).limit(limit * 10) # Process more requirements per batch

        reqs = self.db.execute(stmt_reqs).scalars().all()
        logger.info(f"Found {len(reqs)} requirements needing embedding")

        req_success_count = 0
        for req in reqs:
            req_start = time.time()
            try:
                job = self.db.execute(select(JobPost).where(JobPost.id == req.job_post_id)).scalar_one()
                context_text = f"Job Role: {job.title} at {job.company}. Requirement: {req.text}"
                vector = self.generate_embedding_openai(context_text)

                embedding = JobRequirementUnitEmbedding(
                    job_requirement_unit_id=req.id,
                    embedding=vector
                )
                self.db.add(embedding)
                self.db.commit()
                req_success_count += 1
                req_elapsed = time.time() - req_start
                logger.debug(f"Requirement {req.id} embedding completed and committed in {req_elapsed:.2f}s")
            except Exception as e:
                req_elapsed = time.time() - req_start
                logger.error(f"Failed req embedding {req.id} after {req_elapsed:.2f}s: {e}")
                self.db.rollback()

        batch_elapsed = time.time() - batch_start
        logger.info(f"Embedding batch completed: {job_success_count}/{len(jobs)} jobs, {req_success_count}/{len(reqs)} requirements processed in {batch_elapsed:.2f}s")

    def unload_model(self, model_name: str):
        """
        Unload a model from Ollama to free memory.
        """
        if not model_name:
            return

        # Access base_url from openai_client if possible, or default to internal docker host
        # Since we use openai_client with base_url, we can try to parse it
        # But simpler is to rely on config or default standard Ollama port
        # We need the direct HTTP API, not OpenAI compatible one for this specific 'generate' endpoint 
        # (though newer versions might support it differently, standard way is /api/generate)
        
        # We'll try to guess the host from the client's base_url
        base_url = str(self.openai_client.base_url).rstrip('v1/').rstrip('/')
        if not base_url or base_url == "https://api.openai.com": 
            # If using real OpenAI, we can't unload. 
            # If default, assume local ollama
            base_url = "http://ollama:11434"
            
        url = f"{base_url}/api/generate"
        payload = {"model": model_name, "keep_alive": 0}
        
        try:
            logger.info(f"Unloading model: {model_name} via {url}")
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            logger.warning(f"Failed to unload model {model_name}: {e}")

from sqlalchemy.sql import func
