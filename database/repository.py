import logging
import json
import copy
import hashlib
import re
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import select, delete, and_, or_, func

from database.models import (
    JobPost, JobPostSource, 
    JobRequirementUnit, JobRequirementUnitEmbedding,
    JobMatch, JobMatchRequirement, generate_resume_fingerprint,
    StructuredResume
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
        now = datetime.now(timezone.utc)
        job_post = JobPost(
            title=job_data['title'],
            company=job_data['company_name'],
            location_text=location_text,
            is_remote=job_data.get('is_remote'),
            canonical_fingerprint=fingerprint,
            first_seen_at=now,
            last_seen_at=now,
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

    def _calculate_content_hash(self, job_data: Dict[str, Any]) -> str:
        """Calculate a hash of job content for change detection."""
        # Create a normalized string of the content that affects matching
        content_parts = [
            job_data.get('description', ''),
            json.dumps(job_data.get('skills', []), sort_keys=True),
            job_data.get('title', ''),
            job_data.get('company_name', '')
        ]
        content_str = '|'.join(str(part) for part in content_parts)
        return hashlib.sha256(content_str.encode('utf-8')).hexdigest()[:32]

    def save_job_content(self, job_post_id: Any, job_data: Dict[str, Any]):
        """
        Populates the content fields (description, raw_payload) for a job post.
        Updates content_hash when content changes.
        """
        job_post = self.get_by_id(job_post_id)
        
        # Calculate new content hash
        new_content_hash = self._calculate_content_hash(job_data)
        
        # Check if content actually changed
        content_changed = job_post.content_hash != new_content_hash
        
        # Only update if description is not already set or if content changed
        if not job_post.description or content_changed:
            job_post.description = job_data.get('description')
            
            if job_data.get('skills'):
                job_post.skills_raw = json.dumps(job_data.get('skills'))
            
            # Merge or set raw_payload
            job_post.raw_payload = job_data
            
            # Map other optional fields if present in job_data
            if job_data.get('company_url'):
                job_post.company_url = job_data.get('company_url')
            
            # Update content hash when content changes
            if content_changed:
                job_post.content_hash = new_content_hash
                logger.debug(f"Updated content hash for job {job_post_id}: {new_content_hash[:16]}...")

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

    def _extract_years_from_requirement(self, text: str) -> Tuple[Optional[int], Optional[str]]:
        """Extract years requirement from text."""
        if not text:
            return None, None
        
        text_lower = text.lower()
        
        # Pattern: "X+ years" or "X years" with optional context
        patterns = [
            r'(?:at least |minimum |)(\d+)\+?\s*(?:years?|yrs?)\s+(?:of\s+)?(?:experience\s+(?:in|with|using)\s+)?([^,.;]+)',
            r'(?:at least |minimum |)(\d+)\+?\s*(?:years?|yrs?)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                years = int(match.group(1))
                context = match.group(2).strip() if len(match.groups()) > 1 and match.group(2) else None
                if context:
                    # Clean up context
                    context = re.sub(r'\s+', ' ', context)
                    context = re.sub(r'^(?:of|in|with|using|experience)\s+', '', context)
                return years, context
        
        return None, None

    def save_requirements(self, job_post: JobPost, requirements: List[Dict[str, Any]]):
        # Map AI extraction values to database values
        req_type_mapping = {
            'must_have': 'required',
            'nice_to_have': 'preferred',
            'responsibility': 'responsibility',
            'benefit': 'benefit'
        }
        
        for req in requirements:
            tags = {
                'skills': req.get('related_skills', []),
                'category': req.get('category'),
                'proficiency': req.get('proficiency')
            }

            # Map the req_type from AI extraction schema to database schema
            raw_req_type = req.get('req_type', 'must_have')
            mapped_req_type = req_type_mapping.get(raw_req_type, 'required')

            # Extract years from requirement text
            req_text = req.get('text', '')
            min_years, years_context = self._extract_years_from_requirement(req_text)

            jru = JobRequirementUnit(
                job_post_id=job_post.id,
                req_type=mapped_req_type,
                text=req_text,
                tags=tags,
                ordinal=req.get('ordinal', 0),
                min_years=min_years,
                years_context=years_context
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

    def save_structured_resume(
        self,
        resume_fingerprint: str,
        extracted_data: Dict[str, Any],
        calculated_total_years: Optional[float],
        claimed_total_years: Optional[float],
        experience_validated: bool,
        validation_message: str,
        extraction_confidence: Optional[float] = None,
        extraction_warnings: List[str] = None
    ) -> StructuredResume:
        """
        Save or update structured resume extraction.

        Uses resume_fingerprint for deduplication - if a resume with the same
        fingerprint exists, it will be updated with new extraction data.
        """
        # Check if resume already exists
        from sqlalchemy import select
        stmt = select(StructuredResume).where(
            StructuredResume.resume_fingerprint == resume_fingerprint
        )
        existing = self.db.execute(stmt).scalar_one_or_none()

        if existing:
            # Update existing
            existing.extracted_data = extracted_data
            existing.calculated_total_years = calculated_total_years
            existing.claimed_total_years = claimed_total_years
            existing.experience_validated = experience_validated
            existing.validation_message = validation_message
            existing.extraction_confidence = extraction_confidence
            existing.extraction_warnings = extraction_warnings or []
            resume_record = existing
        else:
            # Create new
            resume_record = StructuredResume(
                resume_fingerprint=resume_fingerprint,
                extracted_data=extracted_data,
                calculated_total_years=calculated_total_years,
                claimed_total_years=claimed_total_years,
                experience_validated=experience_validated,
                validation_message=validation_message,
                extraction_confidence=extraction_confidence,
                extraction_warnings = extraction_warnings or []
            )
            self.db.add(resume_record)

        self.db.flush()
        return resume_record
    
    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        sections: List[Dict[str, Any]]
    ) -> List[Any]:
        """
        Save embeddings for resume sections.
        
        Args:
            resume_fingerprint: Fingerprint of the resume
            sections: List of dicts with keys: section_type, section_index, source_text, source_data, embedding
        
        Returns:
            List of created ResumeSectionEmbedding records
        """
        from database.models import ResumeSectionEmbedding
        
        # First, delete existing embeddings for this resume
        self.db.execute(
            delete(ResumeSectionEmbedding).where(
                ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint
            )
        )
        
        # Create new embeddings
        records = []
        for section in sections:
            record = ResumeSectionEmbedding(
                resume_fingerprint=resume_fingerprint,
                section_type=section['section_type'],
                section_index=section['section_index'],
                source_text=section['source_text'],
                source_data=section['source_data'],
                embedding=section['embedding']
            )
            self.db.add(record)
            records.append(record)
        
        self.db.flush()
        return records
    
    def get_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        section_type: Optional[str] = None
    ) -> List[Any]:
        """
        Retrieve section embeddings for a resume.
        
        Args:
            resume_fingerprint: Fingerprint of the resume
            section_type: Optional filter by section type (experience|project|skill|summary|education)
        
        Returns:
            List of ResumeSectionEmbedding records
        """
        from database.models import ResumeSectionEmbedding
        from sqlalchemy import select
        
        stmt = select(ResumeSectionEmbedding).where(
            ResumeSectionEmbedding.resume_fingerprint == resume_fingerprint
        )
        
        if section_type:
            stmt = stmt.where(ResumeSectionEmbedding.section_type == section_type)
        
        stmt = stmt.order_by(
            ResumeSectionEmbedding.section_type,
            ResumeSectionEmbedding.section_index
        )
        
        return self.db.execute(stmt).scalars().all()
    
    def find_similar_resume_sections(
        self,
        query_embedding: List[float],
        section_type: Optional[str] = None,
        top_k: int = 10
    ) -> List[Any]:
        """
        Find resume sections most similar to a query embedding using vector similarity.
        
        Uses pgvector's <=> operator (cosine distance) for efficient similarity search.
        """
        from database.models import ResumeSectionEmbedding
        from sqlalchemy import select
        
        stmt = select(
            ResumeSectionEmbedding,
            ResumeSectionEmbedding.embedding.cosine_distance(query_embedding).label('distance')
        )
        
        if section_type:
            stmt = stmt.where(ResumeSectionEmbedding.section_type == section_type)
        
        stmt = stmt.order_by('distance').limit(top_k)
        
        results = self.db.execute(stmt).all()
        return [(row.ResumeSectionEmbedding, 1.0 - row.distance) for row in results]
    
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

    # --- Match Helpers ---

    def get_embedded_jobs_for_matching(self, limit: int = 100) -> List[JobPost]:
        """
        Get jobs that are ready for matching.
        Jobs must have been embedded (have embeddings).
        """
        stmt = select(JobPost).where(
            JobPost.is_embedded == True
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def get_existing_match(
        self, 
        job_post_id: Any, 
        resume_fingerprint: str
    ) -> Optional[JobMatch]:
        """
        Check if a match already exists for this job-resume combination.
        """
        stmt = select(JobMatch).where(
            JobMatch.job_post_id == job_post_id,
            JobMatch.resume_fingerprint == resume_fingerprint
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_matches_for_resume(
        self, 
        resume_fingerprint: str,
        min_score: Optional[float] = None,
        status: str = 'active'
    ) -> List[JobMatch]:
        """
        Get all matches for a specific resume fingerprint.
        """
        stmt = select(JobMatch).where(
            JobMatch.resume_fingerprint == resume_fingerprint,
            JobMatch.status == status
        )
        
        if min_score is not None:
            stmt = stmt.where(JobMatch.overall_score >= min_score)
        
        stmt = stmt.order_by(JobMatch.overall_score.desc())
        return self.db.execute(stmt).scalars().all()

    def invalidate_matches_for_job(
        self, 
        job_post_id: Any,
        reason: str = "Job content changed"
    ) -> int:
        """
        Invalidate all existing matches for a job when content changes.
        Returns number of matches invalidated.
        """
        stmt = select(JobMatch).where(
            JobMatch.job_post_id == job_post_id,
            JobMatch.status == 'active'
        )
        matches = self.db.execute(stmt).scalars().all()
        
        count = 0
        for match in matches:
            match.status = 'stale'
            match.invalidated_reason = reason
            count += 1
        
        if count > 0:
            logger.info(f"Invalidated {count} matches for job {job_post_id}: {reason}")
        
        return count

    def invalidate_matches_for_resume(
        self, 
        resume_fingerprint: str,
        reason: str = "Resume changed"
    ) -> int:
        """
        Invalidate all existing matches for a resume when it changes.
        Returns number of matches invalidated.
        """
        stmt = select(JobMatch).where(
            JobMatch.resume_fingerprint == resume_fingerprint,
            JobMatch.status == 'active'
        )
        matches = self.db.execute(stmt).scalars().all()
        
        count = 0
        for match in matches:
            match.status = 'stale'
            match.invalidated_reason = reason
            count += 1
        
        if count > 0:
            logger.info(f"Invalidated {count} matches for resume fingerprint: {reason}")
        
        return count

    def get_stale_matches(self, limit: int = 100) -> List[JobMatch]:
        """
        Get matches that need recalculation.
        """
        stmt = select(JobMatch).where(
            JobMatch.status == 'stale'
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def get_top_jobs_by_summary_embedding(
        self, 
        resume_embedding: List[float], 
        limit: int, 
        tenant_id: Optional[Any] = None,
        require_remote: Optional[bool] = None
    ) -> List[JobPost]:
        """
        Retrieve top-K candidate jobs using vector similarity against summary_embedding.
        
        Stage 1 of two-stage matching pipeline. Retrieves jobs where:
        - is_embedded == True
        - summary_embedding IS NOT NULL
        
        Orders by cosine distance ascending (most similar first).
        
        Args:
            resume_embedding: The resume embedding vector to compare against
            limit: Maximum number of jobs to return (candidate_pool_size_k)
            tenant_id: Optional tenant filter
            require_remote: Optional remote-only filter
            
        Returns:
            List of JobPost objects ordered by similarity (best first)
        """
        # Base query: embedded jobs with non-null embeddings
        stmt = select(JobPost).where(
            JobPost.is_embedded == True,
            JobPost.summary_embedding != None
        )
        
        # Apply optional filters
        if tenant_id is not None:
            stmt = stmt.where(JobPost.tenant_id == tenant_id)
        
        if require_remote is not None:
            stmt = stmt.where(JobPost.is_remote == require_remote)
        
        # Order by cosine distance (ascending = most similar first)
        stmt = stmt.order_by(JobPost.summary_embedding.cosine_distance(resume_embedding)).limit(limit)
        
        return self.db.execute(stmt).scalars().all()

    def get_jobs_for_matching(
        self,
        limit: Optional[int] = None,
        is_embedded: bool = True
    ) -> List[JobPost]:
        """
        Fetch jobs that are ready for matching.
        
        Jobs must have been extracted (have requirements) and embedded.
        
        Args:
            limit: Maximum number of jobs to return
            is_embedded: If True, only return embedded jobs (default: True)
        
        Returns:
            List of JobPost objects ready for matching
        """
        stmt = select(JobPost)
        
        if is_embedded:
            stmt = stmt.where(JobPost.is_embedded == True)
        
        if limit is not None:
            stmt = stmt.limit(limit)
        
        return self.db.execute(stmt).scalars().all()

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()
