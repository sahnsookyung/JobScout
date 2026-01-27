import hashlib
import json
import logging
import random
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy import select
from main_driver.models import JobPost, JobPostSource, JobPostContent, JobRequirementUnit, JobRequirementUnitEmbedding
from main_driver.database import db_session_scope

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLProcessor:
    def __init__(self, db: Session):
        self.db = db

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
        In production, this would call OpenAI/Anthropic.
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

    def generate_embedding_mock(self, _: str) -> List[float]:
        """
        Mock embedding generation. Returns random 768-dim vector.
        """
        return [random.random() for _ in range(768)]

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

        requirements = self.extract_requirements_mock(job_data.get('description'))
        for req in requirements:
            jru = JobRequirementUnit(
                job_post_id=job_post_id,
                req_type=req['req_type'],
                text=req['text'],
                tags=req['tags'],
                ordinal=req['ordinal']
            )
            self.db.add(jru)
            self.db.flush() 
            
            vector = self.generate_embedding_mock(req['text'])
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
