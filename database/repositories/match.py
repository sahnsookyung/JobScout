import logging
from typing import List, Optional, Any
from sqlalchemy import String, and_, cast, func, or_, select
from sqlalchemy.orm import joinedload

from database.models import (
    JobMatch,
    JobMatchRequirement,
    JobPost,
    SYSTEM_OWNER_ID,
)
from database.repositories.base import BaseRepository

logger = logging.getLogger(__name__)


class MatchRepository(BaseRepository):
    def resume_has_persisted_matches(self, resume_fingerprint: str) -> bool:
        stmt = select(JobMatch.id).where(
            JobMatch.resume_fingerprint == resume_fingerprint
        ).limit(1)
        return self.db.execute(stmt).scalar_one_or_none() is not None

    def _invalidate_matches(self, matches: List[JobMatch], reason: str) -> int:
        count = 0
        for match in matches:
            match.status = 'stale'
            match.invalidated_reason = reason
            count += 1
        return count

    def get_existing_match(
        self,
        job_post_id: Any,
        resume_fingerprint: str,
        load_job_post: bool = False,
        owner_id: Any = SYSTEM_OWNER_ID,
    ) -> Optional[JobMatch]:
        stmt = select(JobMatch).where(
            JobMatch.owner_id == owner_id,
            JobMatch.job_post_id == job_post_id,
            JobMatch.resume_fingerprint == resume_fingerprint
        )
        if load_job_post:
            stmt = stmt.options(joinedload(JobMatch.job_post))
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
            stmt = stmt.where(JobMatch.fit_score >= min_score)

        stmt = stmt.order_by(JobMatch.fit_score.desc())
        return self.db.execute(stmt).scalars().all()

    def get_visible_active_matches_for_resume(
        self,
        resume_fingerprint: str,
        *,
        load_job_post: bool = False,
    ) -> List[JobMatch]:
        stmt = select(JobMatch).where(
            JobMatch.resume_fingerprint == resume_fingerprint,
            JobMatch.status == 'active',
            JobMatch.is_hidden.is_(False),
        )
        if load_job_post:
            stmt = stmt.options(joinedload(JobMatch.job_post))
        return self.db.execute(stmt).scalars().all()

    def _reusable_match_filters(
        self,
        resume_fingerprint: str,
        *,
        tenant_id: Any | None = None,
    ):
        filters = [
            JobMatch.resume_fingerprint == resume_fingerprint,
            JobMatch.job_content_hash == JobPost.content_hash,
            JobPost.status == 'active',
            JobPost.is_extracted.is_(True),
            JobPost.is_embedded.is_(True),
            JobPost.summary_embedding.isnot(None),
        ]
        if tenant_id is not None:
            filters.append(JobPost.tenant_id == tenant_id)
        return filters

    def get_reusable_matches_for_resume(
        self,
        resume_fingerprint: str,
        *,
        tenant_id: Any | None = None,
    ) -> List[JobMatch]:
        """Return content-fresh persisted matches that do not need rescoring."""
        stmt = (
            select(JobMatch)
            .join(JobPost, JobPost.id == JobMatch.job_post_id)
            .where(*self._reusable_match_filters(resume_fingerprint, tenant_id=tenant_id))
            .options(
                joinedload(JobMatch.job_post),
                joinedload(JobMatch.requirement_matches).joinedload(
                    JobMatchRequirement.requirement
                ),
            )
        )
        return list(self.db.execute(stmt).unique().scalars().all())

    def count_reusable_matches_for_resume(
        self,
        resume_fingerprint: str,
        *,
        tenant_id: Any | None = None,
    ) -> int:
        stmt = (
            select(func.count(JobMatch.id))
            .join(JobPost, JobPost.id == JobMatch.job_post_id)
            .where(*self._reusable_match_filters(resume_fingerprint, tenant_id=tenant_id))
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def count_pending_matching_jobs(
        self,
        resume_fingerprint: str,
        *,
        tenant_id: Any | None = None,
        candidate_preferences: Optional[dict[str, Any]] = None,
    ) -> int:
        reusable_exists = (
            select(JobMatch.id)
            .where(
                JobMatch.job_post_id == JobPost.id,
                JobMatch.resume_fingerprint == resume_fingerprint,
                JobMatch.job_content_hash == JobPost.content_hash,
            )
            .exists()
        )
        stmt = select(func.count(JobPost.id)).where(
            JobPost.status == 'active',
            JobPost.is_extracted.is_(True),
            JobPost.is_embedded.is_(True),
            JobPost.summary_embedding.isnot(None),
            ~reusable_exists,
        )
        if tenant_id is not None:
            stmt = stmt.where(JobPost.tenant_id == tenant_id)
        stmt = self._apply_candidate_preference_filters(stmt, candidate_preferences)
        return int(self.db.execute(stmt).scalar() or 0)

    @staticmethod
    def _normalize_preference_text(value: Any) -> str:
        if value is None:
            return ""
        return " ".join(str(value).strip().lower().split())

    def _apply_candidate_preference_filters(self, stmt, preferences: Optional[dict[str, Any]]):
        """Apply SQL-compatible hard preference filters to backlog counts."""
        if not preferences:
            return stmt

        remote_mode = self._normalize_preference_text(preferences.get("remote_mode")) or "any"
        work_mode_text = func.lower(func.coalesce(JobPost.work_from_home_type, ""))
        location_text = func.lower(func.coalesce(JobPost.location_text, ""))

        if remote_mode == "remote":
            stmt = stmt.where(or_(JobPost.is_remote.is_(True), work_mode_text.like("%remote%")))
        elif remote_mode == "hybrid":
            stmt = stmt.where(
                or_(
                    JobPost.is_remote.is_(True),
                    work_mode_text.like("%remote%"),
                    work_mode_text.like("%hybrid%"),
                    location_text.like("%hybrid%"),
                )
            )
        elif remote_mode == "onsite":
            stmt = stmt.where(
                or_(
                    ~or_(JobPost.is_remote.is_(True), work_mode_text.like("%remote%")),
                    work_mode_text.like("%hybrid%"),
                    location_text.like("%hybrid%"),
                )
            )

        target_locations = [
            self._normalize_preference_text(value)
            for value in (preferences.get("target_locations") or [])
            if self._normalize_preference_text(value)
        ]
        if target_locations:
            location_filters = [
                location_text.like(f"%{target.replace('%', '')}%")
                for target in target_locations
            ]
            if any("remote" in target for target in target_locations):
                location_filters.append(JobPost.is_remote.is_(True))
                location_filters.append(work_mode_text.like("%remote%"))
            stmt = stmt.where(or_(*location_filters))

        salary_min = preferences.get("salary_min")
        if salary_min is not None:
            try:
                requested_floor = float(salary_min)
            except (TypeError, ValueError):
                requested_floor = None
            if requested_floor is not None:
                stmt = stmt.where(
                    or_(
                        and_(JobPost.salary_min.is_(None), JobPost.salary_max.is_(None)),
                        JobPost.salary_min >= requested_floor,
                        JobPost.salary_max >= requested_floor,
                    )
                )

        employment_types = [
            self._normalize_preference_text(value)
            for value in (preferences.get("employment_types") or [])
            if self._normalize_preference_text(value)
        ]
        if employment_types:
            job_type_text = func.lower(func.coalesce(JobPost.job_type, ""))
            stmt = stmt.where(
                or_(
                    job_type_text == "",
                    *[
                        job_type_text.like(f"%{employment_type.replace('%', '')}%")
                        for employment_type in employment_types
                    ],
                )
            )

        if preferences.get("visa_sponsorship_required"):
            haystack = func.lower(
                func.concat(
                    func.coalesce(JobPost.description, ""),
                    " ",
                    func.coalesce(JobPost.company_description, ""),
                    " ",
                    func.coalesce(cast(JobPost.raw_payload, String), ""),
                )
            )
            stmt = stmt.where(
                and_(
                    ~haystack.like("%no visa sponsorship%"),
                    ~haystack.like("%unable to sponsor%"),
                    ~haystack.like("%without sponsorship%"),
                    or_(
                        haystack.like("%visa sponsorship%"),
                        haystack.like("%sponsor%"),
                        haystack.like("%work authorization support%"),
                        haystack.like("%relocation assistance%"),
                    ),
                )
            )

        return stmt

    def activate_matches_by_ids(self, match_ids: List[Any]) -> int:
        if not match_ids:
            return 0
        stmt = select(JobMatch).where(JobMatch.id.in_(match_ids))
        matches = self.db.execute(stmt).scalars().all()
        for match in matches:
            match.status = 'active'
            match.invalidated_reason = None
        return len(matches)

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
        count = self._invalidate_matches(matches, reason)

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
        count = self._invalidate_matches(matches, reason)

        if count > 0:
            logger.info(f"Invalidated {count} matches for resume fingerprint: {reason}")

        return count

    def invalidate_matches_for_resume_except(
        self,
        resume_fingerprint: str,
        active_job_ids: List[Any] | set[Any] | frozenset[Any],
        reason: str = "Resume changed",
    ) -> int:
        stmt = select(JobMatch).where(
            JobMatch.resume_fingerprint == resume_fingerprint,
            JobMatch.status == 'active',
        )
        matches = self.db.execute(stmt).scalars().all()
        keep_ids = {str(job_id) for job_id in active_job_ids}
        matches_to_invalidate = [
            match for match in matches if str(match.job_post_id) not in keep_ids
        ]
        count = self._invalidate_matches(matches_to_invalidate, reason)

        if count > 0:
            logger.info(
                "Invalidated %d stale active matches for resume fingerprint: %s",
                count,
                reason,
            )

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
        count = self._invalidate_matches(matches, reason)

        if count > 0:
            logger.info(f"Batch invalidated {count} matches for {len(job_ids)} jobs: {reason}")

        return count

    def get_match_by_id(self, match_id: Any) -> Optional[JobMatch]:
        """Get a match by its ID."""
        stmt = select(JobMatch).where(JobMatch.id == match_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_match_by_id_for_owner(self, match_id: Any, owner_id: Any) -> Optional[JobMatch]:
        """Get a match by ID only if it belongs to the given owner."""
        stmt = select(JobMatch).where(
            JobMatch.id == match_id,
            JobMatch.owner_id == owner_id,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def update_hidden_status(self, match_id: Any, is_hidden: bool) -> Optional[JobMatch]:
        """Update the hidden status of a match. Returns the updated match or None if not found."""
        match = self.get_match_by_id(match_id)
        if match:
            match.is_hidden = is_hidden
            logger.info(f"Updated match {match_id} hidden status to {is_hidden}")
        return match

    def get_hidden_count(self, resume_fingerprint: str) -> int:
        """Get count of hidden matches for a resume."""
        stmt = select(func.count()).select_from(JobMatch).where(
            JobMatch.resume_fingerprint == resume_fingerprint,
            JobMatch.is_hidden.is_(True)
        )
        return self.db.execute(stmt).scalar() or 0
