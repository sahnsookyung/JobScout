#!/usr/bin/env python3
"""Job inventory endpoints - inspect imported jobs and processing state."""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from database.models import JobPost, JobPostSource

from ..dependencies import TenantContext, get_current_user, get_db, get_tenant_context
from ..models.responses import JobInventoryItem, JobsResponse, ProcessingBlockerItem, ProcessingBlockersResponse
from ..services.processing_blocker_service import (
    VALID_BLOCKER_STAGES,
    aware_datetime,
    blocker_sort_key,
    default_processing_blocker_service,
    is_retry_due,
    is_stale,
    tenant_filter,
    truncate_error,
)

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

DbSession = Annotated[Session, Depends(get_db)]

_VALID_JOB_STATUSES = {"active", "inactive", "expired", "unknown", "all"}
_VALID_PROCESSING_STATUSES = {
    "all",
    "ready",
    "extracted",
    "embedded",
    "pending_extraction",
    "pending_embedding",
    "failed",
}
_RETRYABLE_OR_PENDING = {"pending", "queued", "in_progress", "processing", "failed_retryable"}
_FAILED_STATUSES = {"failed_terminal", "failed", "failed_retryable"}


def _request_tenant_id(request: Request):
    """Compatibility wrapper for callers importing the old helper."""
    return get_tenant_context(request).tenant_id


def _isoformat(value) -> str | None:
    return value.isoformat() if value is not None else None


def _truncate_error(value: str | None) -> str | None:
    return truncate_error(value)


def _primary_source(job: JobPost) -> JobPostSource | None:
    sources = list(getattr(job, "sources", None) or [])
    if not sources:
        return None
    active_sources = [source for source in sources if getattr(source, "is_active", False)]
    candidates = active_sources or sources
    return max(
        candidates,
        key=lambda source: (
            getattr(source, "last_seen_at", None) is not None,
            getattr(source, "last_seen_at", None),
        ),
    )


def _job_inventory_item(job: JobPost) -> JobInventoryItem:
    source = _primary_source(job)
    return JobInventoryItem(
        job_id=str(job.id),
        title=job.title,
        company=job.company,
        location=job.location_text,
        is_remote=job.is_remote,
        status=job.status,
        is_extracted=bool(job.is_extracted),
        is_embedded=bool(job.is_embedded),
        extraction_status=job.extraction_status,
        embedding_status=job.embedding_status,
        description_completeness=job.description_completeness or "unknown",
        description_source=job.description_source or "unknown",
        description_warning_code=job.description_warning_code,
        source_site=getattr(source, "site", None),
        source_url=getattr(source, "job_url", None),
        first_seen_at=_isoformat(job.first_seen_at),
        last_seen_at=_isoformat(job.last_seen_at),
        extraction_attempts=int(job.extraction_attempts or 0),
        extraction_last_error=_truncate_error(job.extraction_last_error),
        extraction_next_retry_at=_isoformat(job.extraction_next_retry_at),
        embedding_attempts=int(job.embedding_attempts or 0),
        embedding_last_error=_truncate_error(job.embedding_last_error),
        embedding_next_retry_at=_isoformat(job.embedding_next_retry_at),
    )


def _job_inventory_filters(
    *,
    tenant_id,
    job_status: str,
    processing_status: str,
    search: str | None,
):
    filters = []
    filters.append(JobPost.tenant_id.is_(None) if tenant_id is None else JobPost.tenant_id == tenant_id)

    if job_status != "all":
        filters.append(JobPost.status == job_status)

    if processing_status == "ready":
        filters.append(JobPost.is_extracted.is_(True))
        filters.append(JobPost.is_embedded.is_(True))
    elif processing_status == "extracted":
        filters.append(JobPost.is_extracted.is_(True))
    elif processing_status == "embedded":
        filters.append(JobPost.is_embedded.is_(True))
    elif processing_status == "pending_extraction":
        filters.append(JobPost.extraction_status.in_(_RETRYABLE_OR_PENDING))
        filters.append(JobPost.is_extracted.is_(False))
    elif processing_status == "pending_embedding":
        filters.append(JobPost.embedding_status.in_(_RETRYABLE_OR_PENDING))
        filters.append(JobPost.is_embedded.is_(False))
    elif processing_status == "failed":
        filters.append(
            or_(
                JobPost.extraction_status.in_(_FAILED_STATUSES),
                JobPost.embedding_status.in_(_FAILED_STATUSES),
            )
        )

    query = search.strip() if search else ""
    if query:
        pattern = f"%{query}%"
        filters.append(
            or_(
                JobPost.title.ilike(pattern),
                JobPost.company.ilike(pattern),
                JobPost.location_text.ilike(pattern),
            )
        )

    return filters


def list_job_inventory(
    db: Session,
    *,
    tenant_id,
    job_status: str,
    processing_status: str,
    search: str | None,
    limit: int,
    offset: int,
) -> tuple[list[JobInventoryItem], int]:
    filters = _job_inventory_filters(
        tenant_id=tenant_id,
        job_status=job_status,
        processing_status=processing_status,
        search=search,
    )
    total = int(db.execute(select(func.count(JobPost.id)).where(*filters)).scalar_one() or 0)
    stmt = (
        select(JobPost)
        .options(selectinload(JobPost.sources))
        .where(*filters)
        .order_by(JobPost.last_seen_at.desc(), JobPost.id.desc())
        .offset(offset)
        .limit(limit)
    )
    jobs = db.execute(stmt).scalars().all()
    return [_job_inventory_item(job) for job in jobs], total


def _aware_datetime(value):
    return aware_datetime(value)


def _is_retry_due(next_retry_at, *, now: datetime) -> bool:
    return is_retry_due(next_retry_at, now=now)


def _is_stale(last_attempt_at, *, stale_cutoff: datetime) -> bool:
    return is_stale(last_attempt_at, stale_cutoff=stale_cutoff)


def _tenant_filter(tenant_id):
    return tenant_filter(tenant_id)


def _blocker_sort_key(item: ProcessingBlockerItem):
    return blocker_sort_key(item)


def _processing_blocker_item(
    job: JobPost,
    *,
    stage: str,
    blocker_code: str,
    blocker_detail: str,
    status: str,
    attempts: int,
    last_error: str | None,
    retry_eligible: bool,
    last_attempt_at,
    next_retry_at,
) -> ProcessingBlockerItem:
    return default_processing_blocker_service.item(
        job,
        stage=stage,
        blocker_code=blocker_code,
        blocker_detail=blocker_detail,
        status=status,
        attempts=attempts,
        last_error=last_error,
        retry_eligible=retry_eligible,
        last_attempt_at=last_attempt_at,
        next_retry_at=next_retry_at,
    )


def _extraction_blocker(job: JobPost, *, now: datetime, stale_cutoff: datetime) -> ProcessingBlockerItem | None:
    return default_processing_blocker_service.extraction_blocker(
        job,
        now=now,
        stale_cutoff=stale_cutoff,
    )


def _embedding_blocker(job: JobPost, *, now: datetime, stale_cutoff: datetime) -> ProcessingBlockerItem | None:
    return default_processing_blocker_service.embedding_blocker(
        job,
        now=now,
        stale_cutoff=stale_cutoff,
    )


def _matching_blocker(job: JobPost) -> ProcessingBlockerItem:
    return default_processing_blocker_service.matching_blocker(job)


def list_processing_blockers(
    db: Session,
    *,
    tenant_id,
    stage: str,
    limit: int,
) -> list[ProcessingBlockerItem]:
    return default_processing_blocker_service.list_blockers(
        db,
        tenant_id=tenant_id,
        stage=stage,
        limit=limit,
    )


@router.get(
    "/processing-blockers",
    response_model=ProcessingBlockersResponse,
    responses={
        400: {"description": "Invalid tenant header"},
        422: {"description": "Invalid query parameter"},
    },
)
def get_processing_blockers(
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
    stage: Annotated[str, Query(description="Pipeline stage: all, extraction, embedding, or matching")] = "all",
    limit: Annotated[int, Query(ge=1, le=100, description="Maximum blockers to return")] = 25,
) -> ProcessingBlockersResponse:
    """Return the oldest DB-backed job processing blockers."""
    if stage not in VALID_BLOCKER_STAGES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid stage '{stage}'. Valid values: {', '.join(sorted(VALID_BLOCKER_STAGES))}",
        )
    blockers = list_processing_blockers(
        db,
        tenant_id=tenant_context.tenant_id,
        stage=stage,
        limit=limit,
    )
    return ProcessingBlockersResponse(success=True, count=len(blockers), blockers=blockers)


@router.get(
    "",
    response_model=JobsResponse,
    responses={
        400: {"description": "Invalid tenant header"},
        422: {"description": "Invalid query parameter"},
    },
)
def get_jobs(
    request: Request,
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
    job_status: Annotated[str, Query(description="Job lifecycle status: active, inactive, expired, unknown, or all")] = "all",
    processing_status: Annotated[str, Query(description="Processing filter: all, ready, extracted, embedded, pending_extraction, pending_embedding, or failed")] = "all",
    search: Annotated[str | None, Query(max_length=120, description="Search title, company, or location")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="Maximum jobs to return")] = 50,
    offset: Annotated[int, Query(ge=0, description="Pagination offset")] = 0,
):
    """List imported jobs with extraction and embedding processing state."""
    if job_status not in _VALID_JOB_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid job_status '{job_status}'. Valid values: {', '.join(sorted(_VALID_JOB_STATUSES))}",
        )
    if processing_status not in _VALID_PROCESSING_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid processing_status '{processing_status}'. Valid values: {', '.join(sorted(_VALID_PROCESSING_STATUSES))}",
        )

    jobs, total = list_job_inventory(
        db,
        tenant_id=tenant_context.tenant_id,
        job_status=job_status,
        processing_status=processing_status,
        search=search,
        limit=limit,
        offset=offset,
    )
    return JobsResponse(
        success=True,
        count=len(jobs),
        total=total,
        limit=limit,
        offset=offset,
        jobs=jobs,
    )
