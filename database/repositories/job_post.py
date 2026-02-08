import logging
import json
import hashlib
import re
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, delete, func, text, update
from sqlalchemy import and_, or_

from sqlalchemy.dialects.postgresql import insert

from database.models import (
    JobPost, JobPostSource,
    JobRequirementUnit, JobRequirementUnitEmbedding,
    JobFacetEmbedding, JobBenefit
)
from database.repositories.base import BaseRepository
from core.utils import cosine_similarity_from_distance

logger = logging.getLogger(__name__)


class JobPostRepository(BaseRepository):
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
            raw_payload={}
        )
        self.db.add(job_post)
        self.db.flush()
        return job_post

    def get_or_create_source(self, job_post_id: Any, site_name: str, job_data: Dict[str, Any]) -> None:
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
        content_parts = [
            job_data.get('description', ''),
            json.dumps(job_data.get('skills', []), sort_keys=True),
            job_data.get('title', ''),
            job_data.get('company_name', '')
        ]
        content_str = '|'.join(str(part) for part in content_parts)
        return hashlib.sha256(content_str.encode('utf-8')).hexdigest()[:32]

    def save_job_content(self, job_post_id: Any, job_data: Dict[str, Any]) -> None:
        job_post = self.get_by_id(job_post_id)

        new_content_hash = self._calculate_content_hash(job_data)

        content_changed = job_post.content_hash != new_content_hash

        if not job_post.description or content_changed:
            job_post.description = job_data.get('description')

            if job_data.get('skills'):
                job_post.skills_raw = json.dumps(job_data.get('skills'))

            job_post.raw_payload = job_data

            if job_data.get('company_url'):
                job_post.company_url = job_data.get('company_url')

            if content_changed:
                job_post.content_hash = new_content_hash
                logger.debug(f"Updated content hash for job {job_post_id}: {new_content_hash[:16]}...")

    def update_timestamp(self, job_post: JobPost) -> None:
        job_post.last_seen_at = func.now()

    def get_unextracted_jobs(self, limit: int = 100) -> List[JobPost]:
        stmt = select(JobPost).where(
            JobPost.is_extracted.is_(False),
            JobPost.description != None
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def mark_as_extracted(self, job_post: JobPost) -> None:
        job_post.is_extracted = True

    def _extract_years_from_requirement(self, text: str) -> Tuple[Optional[int], Optional[str]]:
        if not text:
            return None, None

        text_lower = text.lower()

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
                    context = re.sub(r'\s+', ' ', context)
                    context = re.sub(r'^(?:of|in|with|using|experience)\s+', '', context)
                return years, context

        return None, None

    def save_requirements(self, job_post: JobPost, requirements: List[Dict[str, Any]]) -> None:
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

            raw_req_type = req.get('req_type', 'must_have')
            mapped_req_type = req_type_mapping.get(raw_req_type, 'required')

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

    def save_benefits(self, job_post: JobPost, benefits: List[Dict[str, Any]]) -> None:
        category_mapping = {
            'health_insurance': 'health_insurance',
            'pension': 'pension',
            'pto': 'pto',
            'remote_work': 'remote_work',
            'parental_leave': 'parental_leave',
            'learning_budget': 'learning_budget',
            'equipment': 'equipment',
            'wellness': 'wellness',
            'other': 'other'
        }

        for benefit in benefits:
            jb = JobBenefit(
                job_post_id=job_post.id,
                category=category_mapping.get(benefit.get('category', 'other'), 'other'),
                text=benefit.get('text', ''),
                ordinal=benefit.get('ordinal', 0)
            )
            self.db.add(jb)

        self.db.flush()

    def update_job_metadata(self, job_post: JobPost, metadata: Dict[str, Any]) -> None:
        job_post.min_years_experience = metadata.get('min_years_experience')
        job_post.requires_degree = metadata.get('requires_degree')
        job_post.security_clearance = metadata.get('security_clearance')
        job_post.job_level = metadata.get('seniority_level')

        if job_post.salary_min is None and metadata.get('salary_min') is not None:
            job_post.salary_min = metadata.get('salary_min')
        if job_post.salary_max is None and metadata.get('salary_max') is not None:
            job_post.salary_max = metadata.get('salary_max')
        if job_post.currency is None and metadata.get('currency') is not None:
            job_post.currency = metadata.get('currency')

        remote_policy = metadata.get('remote_policy', 'Unspecified')
        if remote_policy in ['Remote (Local)', 'Remote (Global)']:
            job_post.is_remote = True
        elif remote_policy == 'On-site':
            job_post.is_remote = False

    def update_content_metadata(self, job_post_id: Any, metadata: Dict[str, Any]) -> None:
        import copy
        job_post = self.get_by_id(job_post_id)

        if metadata.get('tech_stack'):
            job_post.skills_raw = ",".join(metadata['tech_stack'])

        payload = job_post.raw_payload or {}
        new_payload = copy.deepcopy(payload)

        if metadata.get('job_summary'):
            new_payload['ai_job_summary'] = metadata['job_summary']
        if metadata.get('thought_process'):
            new_payload['ai_thought_process'] = metadata['thought_process']
        if metadata.get('visa_sponsorship_available') is not None:
            new_payload['visa_sponsorship_available'] = metadata['visa_sponsorship_available']

        job_post.raw_payload = new_payload

    def get_unembedded_jobs(self, limit: int = 100) -> List[JobPost]:
        stmt = select(JobPost).where(
            JobPost.summary_embedding == None,
            JobPost.description != None
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def get_unembedded_requirements(self, limit: int = 1000) -> List[JobRequirementUnit]:
        stmt = select(JobRequirementUnit).outerjoin(JobRequirementUnitEmbedding).where(
            JobRequirementUnitEmbedding.job_requirement_unit_id == None
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def get_requirement_by_id(self, req_id: Any) -> Optional[JobRequirementUnit]:
        stmt = select(JobRequirementUnit).where(JobRequirementUnit.id == req_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def save_job_embedding(self, job_post: JobPost, embedding: List[float]) -> None:
        job_post.summary_embedding = embedding
        job_post.is_embedded = True

    def save_requirement_embedding(self, req_id: Any, embedding: List[float]) -> None:
        emb_row = JobRequirementUnitEmbedding(
            job_requirement_unit_id=req_id,
            embedding=embedding
        )
        self.db.add(emb_row)

    def get_embedded_jobs_for_matching(self, limit: int = 100) -> List[JobPost]:
        stmt = select(JobPost).where(
            JobPost.is_embedded.is_(True)
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def get_top_jobs_by_summary_embedding(
        self,
        resume_embedding: List[float],
        limit: int,
        tenant_id: Optional[Any] = None,
        require_remote: Optional[bool] = None
    ) -> List[Tuple[JobPost, float]]:
        distance_expr = JobPost.summary_embedding.cosine_distance(resume_embedding).label("distance")

        stmt = select(JobPost, distance_expr).where(
            JobPost.is_embedded.is_(True),
            JobPost.summary_embedding != None
        )

        if tenant_id is not None:
            stmt = stmt.where(JobPost.tenant_id == tenant_id)

        if require_remote is not None:
            stmt = stmt.where(JobPost.is_remote == require_remote)

        stmt = stmt.order_by(distance_expr).limit(limit)

        rows = self.db.execute(stmt).all()
        return [(row[0], cosine_similarity_from_distance(row._mapping['distance'])) for row in rows]

    def get_jobs_needing_facet_extraction(self, limit: int = 100) -> List[JobPost]:
        stmt = select(JobPost).where(
            JobPost.is_embedded.is_(True)
        ).outerjoin(JobFacetEmbedding).where(
            JobFacetEmbedding.id == None
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def save_job_facet_embedding(
        self,
        job_post_id: Any,
        facet_key: str,
        facet_text: str,
        embedding: List[float],
        content_hash: str
    ) -> JobFacetEmbedding:
        stmt = insert(JobFacetEmbedding).values(
            job_post_id=job_post_id,
            facet_key=facet_key,
            facet_text=facet_text,
            embedding=embedding,
            content_hash=content_hash
        ).on_conflict_do_update(
            index_elements=['job_post_id', 'facet_key'],
            set_={
                'facet_text': facet_text,
                'embedding': embedding,
                'content_hash': content_hash
            }
        )
        self.db.execute(stmt)
        return self.db.execute(
            select(JobFacetEmbedding).where(
                JobFacetEmbedding.job_post_id == job_post_id,
                JobFacetEmbedding.facet_key == facet_key
            )
        ).scalar_one_or_none()

    def get_job_facet_embeddings(self, job_post_id: Any) -> Dict[str, List[float]]:
        stmt = select(JobFacetEmbedding).where(
            JobFacetEmbedding.job_post_id == job_post_id
        )
        results = self.db.execute(stmt).scalars().all()
        return {r.facet_key: r.embedding for r in results}

    def delete_all_facet_embeddings_for_job(self, job_post_id: Any) -> None:
        self.db.execute(
            delete(JobFacetEmbedding).where(
                JobFacetEmbedding.job_post_id == job_post_id
            )
        )

    def get_and_claim_jobs_for_facet_extraction(
        self,
        limit: int = 100,
        worker_id: str = "default",
        claim_timeout_minutes: int = 30,
        max_retries: int = 5
    ) -> List[JobPost]:
        now = datetime.now(timezone.utc)
        timeout_threshold = now - timedelta(minutes=claim_timeout_minutes)

        self.db.execute(
            update(JobPost).where(
                and_(
                    JobPost.facet_status == 'in_progress',
                    JobPost.facet_claimed_at < timeout_threshold
                )
            ).values(facet_status='pending')
        )

        self.db.execute(
            update(JobPost).where(
                and_(
                    JobPost.facet_status == 'pending',
                    JobPost.facet_retry_count >= max_retries,
                    JobPost.description.isnot(None)
                )
            ).values(facet_status='quarantined')
        )

        claim_stmt = (
            text("""
                WITH pending AS (
                    SELECT id FROM job_post
                    WHERE is_embedded = true
                      AND facet_status = 'pending'
                      AND description IS NOT NULL
                      AND (facet_extraction_hash IS NULL OR facet_extraction_hash != content_hash)
                      AND facet_retry_count < :max_retries
                    ORDER BY id
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE job_post
                SET facet_status = 'in_progress',
                    facet_claimed_by = :worker_id,
                    facet_claimed_at = :now,
                    facet_retry_count = facet_retry_count + 1
                WHERE id IN (SELECT id FROM pending)
                RETURNING id
            """)
            .bindparams(max_retries=max_retries, worker_id=worker_id, now=now, limit=limit)
        )

        result = self.db.execute(claim_stmt)
        claimed_ids = [row[0] for row in result.fetchall()]

        if not claimed_ids:
            return []

        return self.db.execute(
            select(JobPost).where(JobPost.id.in_(claimed_ids))
        ).scalars().all()

    def mark_job_facets_extracted(self, job_post_id: Any, content_hash: str) -> None:
        self.db.execute(
            update(JobPost)
            .where(JobPost.id == job_post_id)
            .values(
                facet_status='done',
                facet_extraction_hash=content_hash,
                facet_claimed_by=None,
                facet_claimed_at=None,
                facet_last_error=None
            )
        )

    def mark_job_facets_failed(self, job_post_id: Any, error: str = None) -> None:
        self.db.execute(
            update(JobPost)
            .where(JobPost.id == job_post_id)
            .values(
                facet_status='pending',
                facet_claimed_by=None,
                facet_claimed_at=None,
                facet_last_error=error
            )
        )
