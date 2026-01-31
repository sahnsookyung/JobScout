import logging
import json
import copy
from typing import List, Optional, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy import select, func

from database.models import (
    JobPost, JobPostSource, 
    JobRequirementUnit, JobRequirementUnitEmbedding
)

logger = logging.getLogger(__name__)

class JobRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_fingerprint(self, fingerprint: str) -> Optional[JobPost]:
        stmt = select(JobPost).where(JobPost.canonical_fingerprint == fingerprint)
        return self.db.execute(stmt).scalar_one_or_none()
    
    def get_by_id(self, job_post_id: Any) -> JobPost:
        stmt = select(JobPost).where(JobPost.id == job_post_id)
        return self.db.execute(stmt).scalar_one()

    def create_job_post(self, job_data: dict, fingerprint: str, location_text: str) -> JobPost:
        job_post = JobPost(
            title=job_data['title'],
            company=job_data['company_name'],
            location_text=location_text,
            is_remote=job_data.get('is_remote'),
            canonical_fingerprint=fingerprint,
            # Initialize empty payload if needed
            raw_payload={} 
        )
        self.db.add(job_post)
        self.db.flush()  # Generate ID
        return job_post

    def get_or_create_source(self, job_post_id: Any, site_name: str, job_data: Dict[str, Any]):
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

    def save_job_content(self, job_post_id: Any, job_data: Dict[str, Any]):
        """
        Populates the content fields (description, raw_payload) for a job post.
        Originally this was 'get_or_create_content'.
        """
        job_post = self.get_by_id(job_post_id)
        
        # Only update if description is not already set (preserve existing behavior)
        # or you might want to overwrite if the new crawl is 'fresher'. 
        # Here we assume we fill it if it's missing.
        if not job_post.description:
            job_post.description = job_data.get('description')
            
            if job_data.get('skills'):
                job_post.skills_raw = json.dumps(job_data.get('skills'))
            
            # Merge or set raw_payload
            job_post.raw_payload = job_data
            
            # Map other optional fields if present in job_data
            # (e.g. if your scraper provides company_url, etc.)
            if job_data.get('company_url'):
                job_post.company_url = job_data.get('company_url')

    def update_timestamp(self, job_post: JobPost):
        job_post.last_seen_at = func.now()

    # --- Extraction Helpers ---

    def get_unextracted_jobs(self, limit: int = 100) -> List[JobPost]:
        """
        Get jobs that have content (description) but is_extracted is False.
        Now queries JobPost directly.
        """
        stmt = select(JobPost).where(
            JobPost.is_extracted == False,
            JobPost.description != None
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def mark_as_extracted(self, job_post: JobPost):
        job_post.is_extracted = True

    def save_requirements(self, job_post: JobPost, requirements: List[Dict[str, Any]]):
        for req in requirements:
            tags = {
                'skills': req.get('related_skills', []),
                'category': req.get('category'),
                'proficiency': req.get('proficiency')
            }

            jru = JobRequirementUnit(
                job_post_id=job_post.id,
                req_type=req.get('req_type', 'must_have'),
                text=req.get('text', ''),
                tags=tags,
                ordinal=req.get('ordinal', 0)
            )
            self.db.add(jru)
        
        self.db.flush()

    def update_job_metadata(self, job_post: JobPost, metadata: Dict[str, Any]):
        # Structural Fields
        job_post.min_years_experience = metadata.get('min_years_experience')
        job_post.requires_degree = metadata.get('requires_degree')
        job_post.security_clearance = metadata.get('security_clearance')
        job_post.job_level = metadata.get('seniority_level')

        # Conditional Salary
        if job_post.salary_min is None and metadata.get('salary_min') is not None:
            job_post.salary_min = metadata.get('salary_min')
        if job_post.salary_max is None and metadata.get('salary_max') is not None:
            job_post.salary_max = metadata.get('salary_max')
        if job_post.currency is None and metadata.get('currency') is not None:
            job_post.currency = metadata.get('currency')

        # Remote Policy Check
        remote_policy = metadata.get('remote_policy', 'Unspecified')
        if remote_policy in ['Remote (Local)', 'Remote (Global)']:
            job_post.is_remote = True
        elif remote_policy == 'On-site':
            job_post.is_remote = False

    def update_content_metadata(self, job_post_id: Any, metadata: Dict[str, Any]):
        """
        Updates content-related metadata (AI summary, tech stack).
        Now operates directly on JobPost.
        """
        job_post = self.get_by_id(job_post_id)

        # Save tech_stack
        if metadata.get('tech_stack'):
            job_post.skills_raw = ",".join(metadata['tech_stack'])

        # Save specific metadata to raw_payload
        payload = job_post.raw_payload or {}
        new_payload = copy.deepcopy(payload)

        if metadata.get('job_summary'):
            new_payload['ai_job_summary'] = metadata['job_summary']
        if metadata.get('thought_process'):
            new_payload['ai_thought_process'] = metadata['thought_process']
        if metadata.get('visa_sponsorship_available') is not None:
            new_payload['visa_sponsorship_available'] = metadata['visa_sponsorship_available']

        job_post.raw_payload = new_payload

    # --- Embedding Helpers ---

    def get_unembedded_jobs(self, limit: int = 100) -> List[JobPost]:
        """
        Get jobs that have content but no summary_embedding.
        Now queries JobPost directly.
        """
        stmt = select(JobPost).where(
            JobPost.summary_embedding == None,
            JobPost.description != None
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def get_unembedded_requirements(self, limit: int = 1000) -> List[JobRequirementUnit]:
        """
        Get requirements that do not have an embedding row.
        """
        stmt = select(JobRequirementUnit).outerjoin(JobRequirementUnitEmbedding).where(
            JobRequirementUnitEmbedding.job_requirement_unit_id == None
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def save_job_embedding(self, job_post: JobPost, embedding: List[float]):
        job_post.summary_embedding = embedding
        job_post.is_embedded = True

    def save_requirement_embedding(self, req_id: Any, embedding: List[float]):
        emb_row = JobRequirementUnitEmbedding(
            job_requirement_unit_id=req_id,
            embedding=embedding
        )
        self.db.add(emb_row)

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()
