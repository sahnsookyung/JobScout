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
    JobBenefit
)
from database.repositories.base import BaseRepository
from core.utils import cosine_similarity_from_distance

logger = logging.getLogger(__name__)

EXTRACTION_RETRY_DELAYS_SECONDS = [60, 300, 900, 3600, 14400]
EMBEDDING_RETRY_DELAYS_SECONDS = [60, 300, 900, 3600, 14400]
STAGE_IN_PROGRESS_STALE_MINUTES = 30


class JobPostRepository(BaseRepository):
    @staticmethod
    def _compute_next_retry_at(attempts: int, schedule: List[int]) -> datetime:
        delay_seconds = schedule[min(max(attempts - 1, 0), len(schedule) - 1)]
        return datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

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
            if job_post.description and job_post.extraction_status == 'no_description':
                job_post.extraction_status = 'pending'
                logger.info(
                    "Resurrected job %s: description arrived, reset to pending",
                    job_post_id,
                )

            if job_data.get('skills'):
                job_post.skills_raw = json.dumps(job_data.get('skills'))

            job_post.raw_payload = job_data

            if job_data.get('company_url'):
                job_post.company_url = job_data.get('company_url')

            if content_changed:
                job_post.content_hash = new_content_hash
                if job_post.description:
                    job_post.is_extracted = False
                    job_post.extraction_status = 'pending'
                    job_post.extraction_last_error = None
                    job_post.extraction_next_retry_at = None
                    job_post.is_embedded = False
                    job_post.embedding_status = 'pending'
                    job_post.embedding_last_error = None
                    job_post.embedding_next_retry_at = None
                    job_post.summary_embedding = None
                    job_post.canonical_job_summary = None
                    job_post.canonical_job_summary_hash = None
                logger.debug(f"Updated content hash for job {job_post_id}: {new_content_hash[:16]}...")

    def update_timestamp(self, job_post: JobPost) -> None:
        job_post.last_seen_at = func.now()

    def get_unextracted_jobs(self, limit: int = 100) -> List[JobPost]:
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(minutes=STAGE_IN_PROGRESS_STALE_MINUTES)
        stmt = select(JobPost).where(
            JobPost.description.isnot(None),
            or_(
                and_(
                    JobPost.extraction_status.in_(["pending", "failed_retryable"]),
                    or_(
                        JobPost.extraction_next_retry_at.is_(None),
                        JobPost.extraction_next_retry_at <= now,
                    ),
                ),
                and_(
                    JobPost.extraction_status == "in_progress",
                    or_(
                        JobPost.extraction_last_attempt_at.is_(None),
                        JobPost.extraction_last_attempt_at <= stale_cutoff,
                    ),
                ),
            ),
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def mark_as_extracted(self, job_post: JobPost) -> None:
        job_post.is_extracted = True
        job_post.extraction_status = 'succeeded'
        job_post.extraction_attempts = (job_post.extraction_attempts or 0) + 1
        job_post.extraction_last_error = None
        job_post.extraction_last_attempt_at = datetime.now(timezone.utc)
        job_post.extraction_next_retry_at = None

    def mark_extraction_in_progress(self, job_post_id: Any) -> None:
        stmt = update(JobPost).where(
            JobPost.id == job_post_id
        ).values(
            extraction_status='in_progress',
            extraction_last_attempt_at=datetime.now(timezone.utc),
            extraction_next_retry_at=None,
        )
        self.db.execute(stmt)

    def mark_extraction_retryable_failed(self, job_post_id: Any, error: str) -> None:
        """Mark job extraction as retryable failure."""
        job_post = self.get_by_id(job_post_id)
        attempts = (job_post.extraction_attempts or 0) + 1
        now = datetime.now(timezone.utc)
        stmt = update(JobPost).where(
            JobPost.id == job_post_id
        ).values(
            is_extracted=False,
            extraction_status='failed_retryable',
            extraction_attempts=attempts,
            extraction_last_error=error,
            extraction_last_attempt_at=now,
            extraction_next_retry_at=self._compute_next_retry_at(
                attempts, EXTRACTION_RETRY_DELAYS_SECONDS
            ),
        )
        self.db.execute(stmt)

    def mark_extraction_failed(self, job_post_id: Any, error: str) -> None:
        """Mark job extraction as terminally failed.

        Args:
            job_post_id: Job ID
            error: Error message
        """
        job_post = self.get_by_id(job_post_id)
        attempts = (job_post.extraction_attempts or 0) + 1
        stmt = update(JobPost).where(
            JobPost.id == job_post_id
        ).values(
            is_extracted=False,
            extraction_status='failed_terminal',
            extraction_attempts=attempts,
            extraction_last_error=error,
            extraction_last_attempt_at=datetime.now(timezone.utc),
            extraction_next_retry_at=None,
        )
        self.db.execute(stmt)

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
        self.db.execute(
            delete(JobRequirementUnit).where(JobRequirementUnit.job_post_id == job_post.id)
        )

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
        self.db.execute(
            delete(JobBenefit).where(JobBenefit.job_post_id == job_post.id)
        )

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
        if metadata.get('canonical_job_summary'):
            job_post.canonical_job_summary = metadata['canonical_job_summary']
        if metadata.get('canonical_job_summary_version') is not None:
            job_post.canonical_job_summary_version = metadata['canonical_job_summary_version']
        if metadata.get('canonical_job_summary_hash'):
            job_post.canonical_job_summary_hash = metadata['canonical_job_summary_hash']
        if metadata.get('thought_process'):
            new_payload['ai_thought_process'] = metadata['thought_process']
        if metadata.get('visa_sponsorship_available') is not None:
            new_payload['visa_sponsorship_available'] = metadata['visa_sponsorship_available']

        job_post.raw_payload = new_payload

    def get_unembedded_jobs(self, limit: int = 100) -> List[JobPost]:
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(minutes=STAGE_IN_PROGRESS_STALE_MINUTES)
        stmt = select(JobPost).where(
            JobPost.description.isnot(None),
            JobPost.extraction_status == 'succeeded',
            JobPost.summary_embedding.is_(None),
            or_(
                and_(
                    JobPost.embedding_status.in_(["pending", "failed_retryable"]),
                    or_(
                        JobPost.embedding_next_retry_at.is_(None),
                        JobPost.embedding_next_retry_at <= now,
                    ),
                ),
                and_(
                    JobPost.embedding_status == "in_progress",
                    or_(
                        JobPost.embedding_last_attempt_at.is_(None),
                        JobPost.embedding_last_attempt_at <= stale_cutoff,
                    ),
                ),
            ),
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
        job_post.embedding_status = 'succeeded'
        job_post.embedding_attempts = (job_post.embedding_attempts or 0) + 1
        job_post.embedding_last_error = None
        job_post.embedding_last_attempt_at = datetime.now(timezone.utc)
        job_post.embedding_next_retry_at = None

    def save_requirement_embedding(self, req_id: Any, embedding: List[float]) -> None:
        emb_row = JobRequirementUnitEmbedding(
            job_requirement_unit_id=req_id,
            embedding=embedding
        )
        self.db.add(emb_row)

    def mark_embedding_in_progress(self, job_post_id: Any) -> None:
        stmt = update(JobPost).where(
            JobPost.id == job_post_id
        ).values(
            embedding_status='in_progress',
            embedding_last_attempt_at=datetime.now(timezone.utc),
            embedding_next_retry_at=None,
        )
        self.db.execute(stmt)

    def bulk_mark_embedding_in_progress(self, job_post_ids: List[Any]) -> None:
        """Mark multiple jobs as embedding in_progress in a single UPDATE statement."""
        if not job_post_ids:
            return
        stmt = update(JobPost).where(
            JobPost.id.in_(job_post_ids)
        ).values(
            embedding_status='in_progress',
            embedding_last_attempt_at=datetime.now(timezone.utc),
            embedding_next_retry_at=None,
        )
        self.db.execute(stmt)

    def mark_embedding_retryable_failed(self, job_post_id: Any, error: str) -> None:
        """Mark job embedding as failed while keeping it eligible for retry."""
        job_post = self.get_by_id(job_post_id)
        attempts = (job_post.embedding_attempts or 0) + 1
        now = datetime.now(timezone.utc)
        stmt = update(JobPost).where(
            JobPost.id == job_post_id
        ).values(
            is_embedded=False,
            embedding_status='failed_retryable',
            embedding_attempts=attempts,
            embedding_last_error=error,
            embedding_last_attempt_at=now,
            embedding_next_retry_at=self._compute_next_retry_at(
                attempts, EMBEDDING_RETRY_DELAYS_SECONDS
            ),
        )
        self.db.execute(stmt)

    def mark_embedding_failed(self, job_post_id: Any, error: str) -> None:
        """Mark job embedding as terminally failed (no automatic retry)."""
        job_post = self.get_by_id(job_post_id)
        attempts = (job_post.embedding_attempts or 0) + 1
        stmt = update(JobPost).where(
            JobPost.id == job_post_id
        ).values(
            is_embedded=False,
            embedding_status='failed_terminal',
            embedding_attempts=attempts,
            embedding_last_error=error,
            embedding_last_attempt_at=datetime.now(timezone.utc),
            embedding_next_retry_at=None,
        )
        self.db.execute(stmt)

    def get_embedded_jobs_for_matching(self, limit: int = 100) -> List[JobPost]:
        stmt = select(JobPost).where(
            JobPost.is_embedded.is_(True)
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def get_top_jobs_by_summary_embedding(
        self,
        resume_embedding: List[float],
        limit: Optional[int] = None,
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

        stmt = stmt.order_by(distance_expr)
        if limit is not None:
            stmt = stmt.limit(limit)

        rows = self.db.execute(stmt).all()
        return [(row[0], cosine_similarity_from_distance(row._mapping['distance'])) for row in rows]

    def get_top_jobs_by_lexical_query(
        self,
        lexical_query: str,
        *,
        resume_embedding: List[float],
        limit: Optional[int] = None,
        tenant_id: Optional[Any] = None,
        require_remote: Optional[bool] = None,
    ) -> List[Tuple[JobPost, float, float]]:
        if not lexical_query.strip():
            return []

        document_text = func.concat_ws(
            " ",
            func.coalesce(JobPost.title, ""),
            func.coalesce(JobPost.canonical_job_summary, ""),
            func.coalesce(JobPost.description, ""),
            func.coalesce(JobPost.skills_raw, ""),
            func.coalesce(JobPost.company_description, ""),
            func.coalesce(JobPost.work_from_home_type, ""),
        )
        document = func.to_tsvector("simple", document_text)
        query = func.to_tsquery("simple", lexical_query)
        lexical_rank = func.ts_rank_cd(document, query).label("lexical_rank")
        distance_expr = JobPost.summary_embedding.cosine_distance(resume_embedding).label("distance")

        stmt = select(JobPost, lexical_rank, distance_expr).where(
            JobPost.is_embedded.is_(True),
            JobPost.summary_embedding != None,
            document.op("@@")(query),
        )

        if tenant_id is not None:
            stmt = stmt.where(JobPost.tenant_id == tenant_id)

        if require_remote is not None:
            stmt = stmt.where(JobPost.is_remote == require_remote)

        stmt = stmt.order_by(lexical_rank.desc(), distance_expr)
        if limit is not None:
            stmt = stmt.limit(limit)

        rows = self.db.execute(stmt).all()
        return [
            (
                row[0],
                float(row._mapping["lexical_rank"] or 0.0),
                cosine_similarity_from_distance(row._mapping["distance"]),
            )
            for row in rows
        ]

    def quarantine_null_description_jobs(self, older_than_days: int = 7) -> int:
        """Mark stale pending jobs with null descriptions as 'no_description'.

        Jobs saved without a description self-heal when re-scraped. Those that
        are never re-scraped (e.g. posting removed) stay pending forever. After
        older_than_days, move them to 'no_description' so they stop polluting
        pending counts. They are resurrected automatically if a re-scrape ever
        delivers the description (see save_job_content).

        Args:
            older_than_days: Jobs older than this are quarantined.

        Returns:
            Number of jobs marked as 'no_description'.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        result = self.db.execute(
            update(JobPost).where(
                and_(
                    JobPost.description.is_(None),
                    JobPost.extraction_status == 'pending',
                    JobPost.first_seen_at < cutoff,
                )
            ).values(extraction_status='no_description')
        )
        return result.rowcount
