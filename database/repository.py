from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select, func, and_
from database.models import (
    JobPost, JobPostSource, JobPostContent, 
    JobRequirementUnit, JobRequirementUnitEmbedding
)
import logging

logger = logging.getLogger(__name__)

class JobRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_by_fingerprint(self, fingerprint: str) -> Optional[JobPost]:
        stmt = select(JobPost).where(JobPost.canonical_fingerprint == fingerprint)
        return self.db.execute(stmt).scalar_one_or_none()

    def create_job_post(self, job_data: dict, fingerprint: str) -> JobPost:
        job_post = JobPost(
            title=job_data['title'],
            company=job_data['company_name'],
            location_text=job_data['location_text'],
            is_remote=job_data.get('is_remote'),
            canonical_fingerprint=fingerprint,
        )
        self.db.add(job_post)
        self.db.flush() # Generate ID
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

    def get_or_create_content(self, job_post_id: Any, job_data: Dict[str, Any]):
        existing_content = self.db.execute(
            select(JobPostContent).where(JobPostContent.job_post_id == job_post_id)
        ).scalar_one_or_none()
        
        if not existing_content:
            import json
            content = JobPostContent(
                job_post_id=job_post_id,
                description=job_data.get('description'),
                skills_raw=json.dumps(job_data.get('skills')) if job_data.get('skills') else None,
                raw_payload=job_data
            )
            self.db.add(content)

    def update_timestamp(self, job_post: JobPost):
        job_post.last_seen_at = func.now()

    # --- Extraction Helpers ---

    def get_unextracted_jobs(self, limit: int = 100) -> List[JobPost]:
        """
        Get jobs that have content but is_extracted is False.
        """
        stmt = select(JobPost).join(JobPostContent).where(
            JobPost.is_extracted == False,
            JobPostContent.description != None
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def get_content_for_job(self, job_post_id: Any) -> JobPostContent:
        return self.db.execute(
            select(JobPostContent).where(JobPostContent.job_post_id == job_post_id)
        ).scalar_one()

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
            self.db.flush() # Get ID for embedding later if needed immediately (though orchestrator might handle that separately)

    def update_job_metadata(self, job_post: JobPost, metadata: Dict[str, Any]):
        # Structural Fields
        job_post.min_years_experience = metadata.get('min_years_experience')
        job_post.requires_degree = metadata.get('requires_degree')
        job_post.security_clearance = metadata.get('security_clearance')
        job_post.job_level = metadata.get('seniority_level')

        # Conditional Salary (Only if NULL in DB and NOT NULL in Extraction)
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
        content_row = self.db.execute(select(JobPostContent).where(JobPostContent.job_post_id == job_post_id)).scalar_one()
        
        # Save tech_stack
        if metadata.get('tech_stack'):
            content_row.skills_raw = ",".join(metadata['tech_stack'])
        
        # Save specific metadata to raw_payload
        import copy
        payload = content_row.raw_payload or {}
        new_payload = copy.deepcopy(payload)
        
        if metadata.get('job_summary'):
            new_payload['ai_job_summary'] = metadata['job_summary']
        if metadata.get('thought_process'):
            new_payload['ai_thought_process'] = metadata['thought_process']
        if metadata.get('visa_sponsorship_available') is not None:
            new_payload['visa_sponsorship_available'] = metadata['visa_sponsorship_available']
        
        content_row.raw_payload = new_payload

    # --- Embedding Helpers ---

    def get_unembedded_jobs(self, limit: int = 100) -> List[JobPost]:
        """
        Get jobs that have content but no summary_embedding.
        (Alternatively we could use is_embedded flag, but null check is robust).
        """
        stmt = select(JobPost).join(JobPostContent).where(
            JobPost.summary_embedding == None,
            JobPostContent.description != None
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
        job_post.is_embedded = True # Update flag if we use it

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
