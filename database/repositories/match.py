import logging
from typing import List, Optional, Any
from sqlalchemy import select

from database.models import JobMatch
from database.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class MatchRepository(BaseRepository):
    def get_existing_match(
        self,
        job_post_id: Any,
        resume_fingerprint: str
    ) -> Optional[JobMatch]:
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
        stmt = select(JobMatch).where(
            JobMatch.status == 'stale'
        ).limit(limit)
        return self.db.execute(stmt).scalars().all()

    def batch_invalidate_matches_for_jobs(
        self,
        job_ids: List[Any],
        reason: str = "Job content changed"
    ) -> int:
        if not job_ids:
            return 0

        stmt = select(JobMatch).where(
            JobMatch.job_post_id.in_(job_ids),
            JobMatch.status == 'active'
        )
        matches = self.db.execute(stmt).scalars().all()

        count = 0
        for match in matches:
            match.status = 'stale'
            match.invalidated_reason = reason
            count += 1

        if count > 0:
            logger.info(f"Batch invalidated {count} matches for {len(job_ids)} jobs: {reason}")

        return count
