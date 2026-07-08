"""DB-backed processing blocker diagnosis."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, true
from sqlalchemy.orm import Session

from database.models import JobMatch, JobPost
from database.repository import JobRepository
from web.backend.models.responses import ProcessingBlockerItem

VALID_BLOCKER_STAGES = {"all", "extraction", "embedding", "matching"}
ERROR_TEXT_LIMIT = 240
STALE_STAGE_MINUTES = 30


def isoformat(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def truncate_error(value: str | None) -> str | None:
    if not value:
        return None
    return value if len(value) <= ERROR_TEXT_LIMIT else f"{value[:ERROR_TEXT_LIMIT - 3]}..."


def aware_datetime(value: Any):
    if value is None:
        return None
    if getattr(value, "tzinfo", None) is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def is_retry_due(next_retry_at: Any, *, now: datetime) -> bool:
    value = aware_datetime(next_retry_at)
    return value is None or value <= now


def is_stale(last_attempt_at: Any, *, stale_cutoff: datetime) -> bool:
    value = aware_datetime(last_attempt_at)
    return value is None or value <= stale_cutoff


def tenant_filter(tenant_id: Any):
    return JobPost.tenant_id.is_(None) if tenant_id is None else JobPost.tenant_id == tenant_id


def blocker_sort_key(item: ProcessingBlockerItem):
    for value in (item.last_attempt_at, item.first_seen_at, item.last_seen_at):
        if value:
            return value
    return ""


class ProcessingBlockerService:
    """Diagnose exact blockers for jobs that are not moving through the pipeline."""

    def __init__(self, *, stale_stage_minutes: int = STALE_STAGE_MINUTES) -> None:
        self.stale_stage_minutes = stale_stage_minutes

    def item(
        self,
        job: JobPost,
        *,
        stage: str,
        blocker_code: str,
        blocker_detail: str,
        status: str,
        attempts: int,
        last_error: str | None,
        retry_eligible: bool,
        last_attempt_at: Any,
        next_retry_at: Any,
        recovery_status: str | None = None,
        recovery_reason: str | None = None,
        recovery_run_id: str | None = None,
    ) -> ProcessingBlockerItem:
        return ProcessingBlockerItem(
            job_id=str(job.id),
            stage=stage,
            blocker_code=blocker_code,
            blocker_detail=blocker_detail,
            status=status,
            attempts=int(attempts or 0),
            last_error=truncate_error(last_error),
            retry_eligible=retry_eligible,
            first_seen_at=isoformat(job.first_seen_at),
            last_seen_at=isoformat(job.last_seen_at),
            last_attempt_at=isoformat(last_attempt_at),
            next_retry_at=isoformat(next_retry_at),
            recovery_status=recovery_status,
            recovery_reason=recovery_reason,
            recovery_run_id=recovery_run_id,
        )

    def extraction_blocker(
        self,
        job: JobPost,
        *,
        now: datetime,
        stale_cutoff: datetime,
    ) -> ProcessingBlockerItem | None:
        if bool(job.is_extracted):
            return None
        status = job.extraction_status or "pending"
        if not (job.description or "").strip():
            recovery_status = getattr(job, "description_recovery_status", None) or "pending"
            recovery_reason = getattr(job, "description_recovery_reason", None)
            retry_eligible = recovery_status in {
                "pending",
                "failed_retryable",
                "source_unmapped",
            } and is_retry_due(
                getattr(job, "description_recovery_next_retry_at", None),
                now=now,
            )
            detail_by_status = {
                "queued": "Job is queued for compliant ATS description recovery.",
                "refreshing": "Job description recovery is checking the ATS source.",
                "posting_not_found": "The authoritative ATS source no longer lists this posting.",
                "source_unsupported": "This source has no compliant description recovery adapter.",
                "source_adapter_missing": "This source needs a supported API adapter before description recovery can run.",
                "source_prohibited": "Hosted description recovery is disabled for this source.",
                "source_unmapped": "This job needs a configured ATS source mapping before recovery can run.",
                "failed_retryable": "Description recovery failed retryably and will retry after backoff.",
                "failed_terminal": "Description recovery failed terminally and needs manual review.",
            }
            return self.item(
                job,
                stage="extraction",
                blocker_code="description_missing",
                blocker_detail=detail_by_status.get(
                    recovery_status,
                    "Job has no description available for extraction.",
                ),
                status=status,
                attempts=getattr(job, "description_recovery_attempts", 0),
                last_error=getattr(job, "description_recovery_last_error", None) or job.extraction_last_error,
                retry_eligible=retry_eligible,
                last_attempt_at=getattr(job, "description_recovery_last_attempt_at", None),
                next_retry_at=getattr(job, "description_recovery_next_retry_at", None),
                recovery_status=recovery_status,
                recovery_reason=recovery_reason,
                recovery_run_id=getattr(job, "description_recovery_run_id", None),
            )
        if status in {"in_progress", "processing"} and is_stale(job.extraction_last_attempt_at, stale_cutoff=stale_cutoff):
            return self.item(
                job,
                stage="extraction",
                blocker_code="stale_extraction",
                blocker_detail="Extraction has been in progress longer than the stale-stage threshold.",
                status=status,
                attempts=job.extraction_attempts,
                last_error=job.extraction_last_error,
                retry_eligible=True,
                last_attempt_at=job.extraction_last_attempt_at,
                next_retry_at=job.extraction_next_retry_at,
            )
        if status == "failed_retryable":
            retry_due = is_retry_due(job.extraction_next_retry_at, now=now)
            return self.item(
                job,
                stage="extraction",
                blocker_code="retryable_extraction" if retry_due else "retry_waiting",
                blocker_detail=(
                    "Extraction failed retryably and is eligible to requeue."
                    if retry_due
                    else "Extraction is waiting for its next retry window."
                ),
                status=status,
                attempts=job.extraction_attempts,
                last_error=job.extraction_last_error,
                retry_eligible=retry_due,
                last_attempt_at=job.extraction_last_attempt_at,
                next_retry_at=job.extraction_next_retry_at,
            )
        if status in {"failed", "failed_terminal"}:
            return self.item(
                job,
                stage="extraction",
                blocker_code="non_retryable_failure",
                blocker_detail="Extraction failed terminally and needs manual repair or a new source payload.",
                status=status,
                attempts=job.extraction_attempts,
                last_error=job.extraction_last_error,
                retry_eligible=False,
                last_attempt_at=job.extraction_last_attempt_at,
                next_retry_at=job.extraction_next_retry_at,
            )
        if status in {"pending", "queued"}:
            queued_too_long = (
                status == "queued"
                and job.extraction_last_attempt_at
                and is_stale(job.extraction_last_attempt_at, stale_cutoff=stale_cutoff)
            )
            blocker_code = "queued_too_long" if queued_too_long else "pending_queue"
            return self.item(
                job,
                stage="extraction",
                blocker_code=blocker_code,
                blocker_detail=(
                    "Job is queued for extraction longer than the stale-stage threshold."
                    if blocker_code == "queued_too_long"
                    else "Job is queued for extraction and waiting for a worker."
                    if status == "queued"
                    else "Job is pending extraction and has not been queued yet."
                ),
                status=status,
                attempts=job.extraction_attempts,
                last_error=job.extraction_last_error,
                retry_eligible=True,
                last_attempt_at=job.extraction_last_attempt_at,
                next_retry_at=job.extraction_next_retry_at,
            )
        return None

    def embedding_blocker(
        self,
        job: JobPost,
        *,
        now: datetime,
        stale_cutoff: datetime,
    ) -> ProcessingBlockerItem | None:
        status = job.embedding_status or "pending"
        if not bool(job.is_extracted):
            return self.item(
                job,
                stage="embedding",
                blocker_code="extraction_not_ready",
                blocker_detail="Embedding is blocked until extraction completes.",
                status=status,
                attempts=job.embedding_attempts,
                last_error=job.embedding_last_error,
                retry_eligible=False,
                last_attempt_at=job.embedding_last_attempt_at,
                next_retry_at=job.embedding_next_retry_at,
            )
        if bool(job.is_embedded):
            return None
        if status in {"in_progress", "processing"} and is_stale(job.embedding_last_attempt_at, stale_cutoff=stale_cutoff):
            return self.item(
                job,
                stage="embedding",
                blocker_code="stale_embedding",
                blocker_detail="Embedding has been in progress longer than the stale-stage threshold.",
                status=status,
                attempts=job.embedding_attempts,
                last_error=job.embedding_last_error,
                retry_eligible=True,
                last_attempt_at=job.embedding_last_attempt_at,
                next_retry_at=job.embedding_next_retry_at,
            )
        if status == "failed_retryable":
            retry_due = is_retry_due(job.embedding_next_retry_at, now=now)
            return self.item(
                job,
                stage="embedding",
                blocker_code="retryable_embedding" if retry_due else "retry_waiting",
                blocker_detail=(
                    "Embedding failed retryably and is eligible to requeue."
                    if retry_due
                    else "Embedding is waiting for its next retry window."
                ),
                status=status,
                attempts=job.embedding_attempts,
                last_error=job.embedding_last_error,
                retry_eligible=retry_due,
                last_attempt_at=job.embedding_last_attempt_at,
                next_retry_at=job.embedding_next_retry_at,
            )
        if status in {"failed", "failed_terminal"}:
            return self.item(
                job,
                stage="embedding",
                blocker_code="non_retryable_failure",
                blocker_detail="Embedding failed terminally and needs manual repair.",
                status=status,
                attempts=job.embedding_attempts,
                last_error=job.embedding_last_error,
                retry_eligible=False,
                last_attempt_at=job.embedding_last_attempt_at,
                next_retry_at=job.embedding_next_retry_at,
            )
        if status in {"pending", "queued"}:
            queued_too_long = status == "queued" and job.embedding_last_attempt_at and is_stale(
                job.embedding_last_attempt_at,
                stale_cutoff=stale_cutoff,
            )
            blocker_code = "queued_too_long" if queued_too_long else "pending_queue"
            return self.item(
                job,
                stage="embedding",
                blocker_code=blocker_code,
                blocker_detail=(
                    "Job is queued for embedding longer than the stale-stage threshold."
                    if blocker_code == "queued_too_long"
                    else "Job is queued for embedding and waiting for a worker."
                    if status == "queued"
                    else "Job is ready for embedding but has not been queued yet."
                ),
                status=status,
                attempts=job.embedding_attempts,
                last_error=job.embedding_last_error,
                retry_eligible=True,
                last_attempt_at=job.embedding_last_attempt_at,
                next_retry_at=job.embedding_next_retry_at,
            )
        return None

    def matching_blocker(
        self,
        job: JobPost,
        *,
        blocker_code: str = "ready_unmatched",
        blocker_detail: str = "Job is extracted and embedded but has no persisted match rows.",
    ) -> ProcessingBlockerItem:
        return self.item(
            job,
            stage="matching",
            blocker_code=blocker_code,
            blocker_detail=blocker_detail,
            status=job.status,
            attempts=0,
            last_error=None,
            retry_eligible=True,
            last_attempt_at=None,
            next_retry_at=None,
        )

    def list_blockers(
        self,
        db: Session,
        *,
        tenant_id: Any,
        stage: str,
        limit: int,
    ) -> list[ProcessingBlockerItem]:
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(minutes=self.stale_stage_minutes)
        blockers: list[ProcessingBlockerItem] = []
        base_filters = [tenant_filter(tenant_id)]

        if stage in {"all", "extraction"}:
            jobs = db.execute(
                select(JobPost)
                .where(
                    *base_filters,
                    JobPost.is_extracted.is_(False),
                    JobPost.extraction_status.in_(
                        (
                            "pending",
                            "queued",
                            "in_progress",
                            "processing",
                            "failed_retryable",
                            "failed",
                            "failed_terminal",
                        )
                    ),
                )
                .order_by(JobPost.extraction_last_attempt_at.asc().nullsfirst(), JobPost.first_seen_at.asc())
                .limit(limit)
            ).scalars().all()
            blockers.extend(
                blocker
                for job in jobs
                if (blocker := self.extraction_blocker(job, now=now, stale_cutoff=stale_cutoff)) is not None
            )

        if stage in {"all", "embedding"}:
            jobs = db.execute(
                select(JobPost)
                .where(
                    *base_filters,
                    JobPost.is_embedded.is_(False),
                    JobPost.embedding_status.in_(
                        (
                            "pending",
                            "queued",
                            "in_progress",
                            "processing",
                            "failed_retryable",
                            "failed",
                            "failed_terminal",
                        )
                    ),
                )
                .order_by(JobPost.embedding_last_attempt_at.asc().nullsfirst(), JobPost.first_seen_at.asc())
                .limit(limit)
            ).scalars().all()
            blockers.extend(
                blocker
                for job in jobs
                if (blocker := self.embedding_blocker(job, now=now, stale_cutoff=stale_cutoff)) is not None
            )

        if stage in {"all", "matching"}:
            latest_resume_fingerprint = JobRepository(db).get_latest_ready_resume_fingerprint()
            has_latest_match = (
                select(JobMatch.id)
                .where(
                    JobMatch.job_post_id == JobPost.id,
                    JobMatch.resume_fingerprint == latest_resume_fingerprint,
                )
                .exists()
                if latest_resume_fingerprint
                else None
            )
            match_filter = ~has_latest_match if has_latest_match is not None else true()
            jobs = db.execute(
                select(JobPost)
                .where(
                    *base_filters,
                    JobPost.is_extracted.is_(True),
                    JobPost.is_embedded.is_(True),
                    match_filter,
                )
                .order_by(JobPost.last_seen_at.asc(), JobPost.id.asc())
                .limit(limit)
            ).scalars().all()
            if not latest_resume_fingerprint:
                blockers.extend(
                    self.matching_blocker(
                        job,
                        blocker_code="missing_resume_context",
                        blocker_detail="Job is ready for matching, but no ready resume context is available.",
                    )
                    for job in jobs
                )
            else:
                for job in jobs:
                    has_any_match = db.execute(
                        select(JobMatch.id)
                        .where(JobMatch.job_post_id == job.id)
                        .limit(1)
                    ).scalar_one_or_none()
                    if has_any_match:
                        blockers.append(
                            self.matching_blocker(
                                job,
                                blocker_code="matching_stale",
                                blocker_detail="Job has match rows for an older resume but not the latest ready resume.",
                            )
                        )
                    else:
                        blockers.append(
                            self.matching_blocker(
                                job,
                                blocker_code="matching_not_queued",
                                blocker_detail="Job is ready for matching but no match row exists for the latest ready resume.",
                            )
                        )

        blockers.sort(key=blocker_sort_key)
        return blockers[:limit]


default_processing_blocker_service = ProcessingBlockerService()
