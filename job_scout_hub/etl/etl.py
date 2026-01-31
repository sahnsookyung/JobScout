import hashlib
import json
import requests
import logging
import random
import os
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
    def __init__(self, db: Session, mock_mode: bool = True, llm_config: Optional[Dict[str, Any]] = None):
        self.db = db
        self.mock_mode = mock_mode
        self.openai_client = None
        
        if not self.mock_mode:
            # Extract config values with defaults
            llm_config = llm_config or {}
            api_key = llm_config.get('api_key') or os.environ.get("OPENAI_API_KEY")
            base_url = llm_config.get('base_url')
            
            # Extraction configuration
            self.extraction_type = llm_config.get('extraction_type', 'openai')
            self.extraction_model = llm_config.get('extraction_model')
            
            if not self.extraction_model:
                self.extraction_model = 'qwen3:14b' if self.extraction_type == 'ollama' else 'gpt-4o-mini' 

            self.extraction_labels = llm_config.get('extraction_labels', [])
            
            # Embedding configuration
            self.embedding_model = llm_config.get('embedding_model', 'text-embedding-3-small')
            self.embedding_dimensions = llm_config.get('embedding_dimensions', 768)
            
            if not api_key and not base_url:
                logger.warning("No API key or base_url found. Falling back to mock mode.")
                self.mock_mode = True
            else:
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

    def extract_requirements_mock(self, description: str) -> List[Dict[str, Any]]:
        """
        Mock LLM extraction. Returns dummy requirements.
        """
        # Simple heuristic for testing: split by newlines and take a few
        lines = [l.strip() for l in description.split('\n') if len(l.strip()) > 20]
        units = []
        for i, line in enumerate(lines[:5]): # Take top 5 lines as "requirements" for now
            units.append({
                "req_type": "responsibility" if "work" in line.lower() else "required",
                "text": line[:500], # Truncate if too long
                "tags": {"mock": True},
                "ordinal": i
            })
        return units

    def extract_requirements_openai(self, description: str) -> List[Dict[str, Any]]:
        """
        Extract requirements using LLM with structured JSON output.
        """
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
            requirements = data.get('requirements', [])
            
            # Add ordinal
            for i, req in enumerate(requirements):
                req['ordinal'] = i
            return requirements
            
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            logger.info("Falling back to mock extraction due to failure")
            return self.extract_requirements_mock(description)


    def generate_embedding_mock(self, _: str) -> List[float]:
        """
        Mock embedding generation. Returns random 768-dim vector.
        """
        # pgvector (vector(768)) requires exactly 768 dimensions
        return [random.random() for _ in range(1024)] # Match qwen3-embedding:4b dimensions

    # Note: If changing to text-embedding-3-small, dimension is 1536 by default. 
    # If keeping 768 in DB, use dimensions=768 param in API call (supported in v3 models).
    def generate_embedding_openai(self, text: str) -> List[float]:
        try:
            response = self.openai_client.embeddings.create(
                input=text,
                model=self.embedding_model,
                dimensions=self.embedding_dimensions
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"OpenAI embedding failed: {e}")
            return self.generate_embedding_mock(text)

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

    def _get_or_create_content(self, job_post_id: Any, job_data: Dict[str, Any]):
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
            self._extract_and_embed_requirements(job_post_id, job_data)

    def _extract_and_embed_requirements(self, job_post_id: Any, job_data: Dict[str, Any]):
        if not job_data.get('description'):
            return

        if self.mock_mode:
            requirements = self.extract_requirements_mock(job_data.get('description'))
        elif self.extraction_type == 'gliner':
            # Deprecated: Fallback to OpenAI/Ollama logic or Mock
            logger.warning("GLiNER extraction type is deprecated. Using default LLM extraction.")
            requirements = self.extract_requirements_openai(job_data.get('description'))
        else:  # 'openai' or default
            requirements = self.extract_requirements_openai(job_data.get('description'))
            
        for req in requirements:
            jru = JobRequirementUnit(
                job_post_id=job_post_id,
                req_type=req.get('req_type', 'required'),
                text=req.get('text', ''),
                tags={'skills': req.get('skills', [])},
                ordinal=req.get('ordinal', 0)
            )
            self.db.add(jru)
            self.db.flush() 
            
            if self.mock_mode:
                vector = self.generate_embedding_mock(req.get('text', ''))
            else:
                vector = self.generate_embedding_openai(req.get('text', ''))
            
            embedding = JobRequirementUnitEmbedding(
                job_requirement_unit_id=jru.id,
                embedding=vector
            )
            self.db.add(embedding)

    def process_job_data(self, job_data: Dict[str, Any], site_name: str):
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
        self._get_or_create_content(job_post.id, job_data)

from sqlalchemy.sql import func
