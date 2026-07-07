#!/usr/bin/env python3
"""Job inventory endpoints - inspect imported jobs and processing state."""

import copy
import uuid
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from database.models import JobPost, JobPostSource

from ..dependencies import TenantContext, get_current_user, get_db, get_tenant_context
from ..models.responses import (
    JobAvailabilityMutationResponse,
    JobInventoryItem,
    JobsResponse,
    ProcessingBlockerItem,
    ProcessingBlockersResponse,
)
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
from ..services.cursors import CursorDecodeError, MatchCursorCodec
from ..services.source_availability import source_refresh_kind

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
_VALID_BLOCKER_VIEWS = {"compact", "detail"}
_LIFECYCLE_METADATA_KEY = "jobscout_lifecycle"


def _request_tenant_id(request: Request):
    """Compatibility wrapper for callers importing the old helper."""
    return get_tenant_context(request).tenant_id


def _isoformat(value) -> str | None:
    return value.isoformat() if value is not None else None


def _truncate_error(value: str | None) -> str | None:
    return truncate_error(value)


def _primary_source(job: JobPost) -> JobPostSource | None:
    try:
        sources = list(getattr(job, "sources", None) or [])
    except TypeError:
        sources = []
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


def _lifecycle_metadata(job: JobPost) -> dict:
    payload = getattr(job, "raw_payload", None)
    if not isinstance(payload, dict):
        return {}
    metadata = payload.get(_LIFECYCLE_METADATA_KEY)
    return copy.deepcopy(metadata) if isinstance(metadata, dict) else {}


def _availability_status(job: JobPost, source: JobPostSource | None) -> tuple[str, str]:
    lifecycle = _lifecycle_metadata(job)
    if getattr(job, "status", None) == "expired" and isinstance(lifecycle.get("manual_retirement"), dict):
        return "manually_retired", "manual_retirement"
    if source is not None and getattr(source, "is_active", None) is False:
        return "source_inactive", "source_sync_absent"
    if source is None:
        return "unknown", "source_missing"
    if getattr(job, "status", None) == "inactive":
        return "inactive", "job_inactive"
    return "active", "source_sync_active"


def _availability_actions(job: JobPost, source: JobPostSource | None) -> list[str]:
    actions: list[str] = []
    if source is not None and (getattr(source, "job_url_direct", None) or getattr(source, "job_url", None)):
        actions.append("open_posting")
    refresh_kind = source_refresh_kind(source)
    if refresh_kind == "compliant_ats":
        actions.append("refresh_availability")
    elif refresh_kind == "prohibited":
        actions.append("refresh_unavailable_deployment_disabled")
    elif source is not None:
        actions.append("refresh_unavailable")
    actions.append("restore" if getattr(job, "status", None) == "expired" else "retire")
    return actions


def _job_inventory_item(job: JobPost) -> JobInventoryItem:
    source = _primary_source(job)
    availability_status, availability_reason = _availability_status(job, source)
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
        source_url_direct=getattr(source, "job_url_direct", None),
        source_job_id=getattr(source, "source_job_id", None),
        source_is_active=getattr(source, "is_active", None),
        source_first_seen_at=_isoformat(getattr(source, "first_seen_at", None)),
        source_last_seen_at=_isoformat(getattr(source, "last_seen_at", None)),
        first_seen_at=_isoformat(job.first_seen_at),
        last_seen_at=_isoformat(job.last_seen_at),
        availability_status=availability_status,
        availability_reason=availability_reason,
        availability_actions=_availability_actions(job, source),
        lifecycle_metadata=_lifecycle_metadata(job),
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


def _get_scoped_job(db: Session, job_id: str, *, tenant_id) -> JobPost:
    try:
        lookup_id = uuid.UUID(str(job_id))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid job_id format: {job_id}.") from exc

    stmt = (
        select(JobPost)
        .options(selectinload(JobPost.sources))
        .where(JobPost.id == lookup_id)
    )
    stmt = stmt.where(JobPost.tenant_id.is_(None) if tenant_id is None else JobPost.tenant_id == tenant_id)
    job = db.execute(stmt).scalars().first()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


def _mutation_response(
    job: JobPost,
    message: str,
    *,
    queued: bool = False,
    sync_run_id: str | None = None,
) -> JobAvailabilityMutationResponse:
    source = _primary_source(job)
    availability_status, availability_reason = _availability_status(job, source)
    return JobAvailabilityMutationResponse(
        success=True,
        job_id=str(job.id),
        status=str(job.status),
        availability_status=availability_status,
        availability_reason=availability_reason,
        message=message,
        queued=queued,
        sync_run_id=sync_run_id,
    )


def _set_manual_retirement(job: JobPost, *, user_id: object) -> None:
    payload = copy.deepcopy(job.raw_payload or {})
    lifecycle = payload.get(_LIFECYCLE_METADATA_KEY)
    if not isinstance(lifecycle, dict):
        lifecycle = {}
    lifecycle["manual_retirement"] = {
        "retired_at": datetime.now(timezone.utc).isoformat(),
        "retired_by": str(user_id) if user_id is not None else None,
        "reason": "manual_retire",
    }
    payload[_LIFECYCLE_METADATA_KEY] = lifecycle
    job.raw_payload = payload
    job.status = "expired"


def _clear_manual_retirement(job: JobPost) -> None:
    payload = copy.deepcopy(job.raw_payload or {})
    lifecycle = payload.get(_LIFECYCLE_METADATA_KEY)
    if isinstance(lifecycle, dict):
        lifecycle.pop("manual_retirement", None)
        if lifecycle:
            payload[_LIFECYCLE_METADATA_KEY] = lifecycle
        else:
            payload.pop(_LIFECYCLE_METADATA_KEY, None)
    job.raw_payload = payload
    job.status = "active"


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
    cursor: Annotated[str | None, Query(description="Opaque cursor returned by a prior blockers response")] = None,
    view: Annotated[str, Query(description="Payload view: detail (default) or compact")] = "detail",
) -> ProcessingBlockersResponse:
    """Return the oldest DB-backed job processing blockers."""
    if stage not in VALID_BLOCKER_STAGES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid stage '{stage}'. Valid values: {', '.join(sorted(VALID_BLOCKER_STAGES))}",
        )
    if view not in _VALID_BLOCKER_VIEWS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid view '{view}'. Valid values: {', '.join(sorted(_VALID_BLOCKER_VIEWS))}",
        )
    effective_offset = 0
    if cursor:
        try:
            decoded = MatchCursorCodec.decode(cursor, expected_kind="processing_blockers")
            effective_offset = max(int(decoded.get("offset", 0) or 0), 0)
        except (CursorDecodeError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    fetch_limit = effective_offset + limit + 1
    fetched_blockers = list_processing_blockers(
        db,
        tenant_id=tenant_context.tenant_id,
        stage=stage,
        limit=fetch_limit,
    )
    page_blockers = fetched_blockers[effective_offset:effective_offset + limit]
    has_more = len(fetched_blockers) > effective_offset + limit
    next_cursor = (
        MatchCursorCodec.encode("processing_blockers", offset=effective_offset + len(page_blockers))
        if has_more
        else None
    )
    total = effective_offset + len(page_blockers) + (1 if has_more else 0)
    return ProcessingBlockersResponse(
        success=True,
        count=len(page_blockers),
        total=total,
        limit=limit,
        offset=effective_offset,
        has_more=has_more,
        page_mode="cursor" if cursor else "offset",
        view=view,
        next_cursor=next_cursor,
        blockers=page_blockers,
    )


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


@router.post(
    "/{job_id}/retire",
    response_model=JobAvailabilityMutationResponse,
    responses={
        400: {"description": "Invalid job id or tenant header"},
        404: {"description": "Job not found"},
    },
)
def retire_job(
    job_id: str,
    db: DbSession,
    user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
) -> JobAvailabilityMutationResponse:
    """Mark a job manually retired without changing source activity."""
    job = _get_scoped_job(db, job_id, tenant_id=tenant_context.tenant_id)
    _set_manual_retirement(job, user_id=getattr(user, "id", None))
    db.commit()
    db.refresh(job)
    return _mutation_response(job, "Job retired. Source activity was left unchanged.")


@router.post(
    "/{job_id}/restore",
    response_model=JobAvailabilityMutationResponse,
    responses={
        400: {"description": "Invalid job id or tenant header"},
        404: {"description": "Job not found"},
    },
)
def restore_job(
    job_id: str,
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
) -> JobAvailabilityMutationResponse:
    """Clear manual retirement metadata and return a job to the active pool."""
    job = _get_scoped_job(db, job_id, tenant_id=tenant_context.tenant_id)
    _clear_manual_retirement(job)
    db.commit()
    db.refresh(job)
    return _mutation_response(job, "Job restored to active.")


@router.post(
    "/{job_id}/refresh-availability",
    response_model=JobAvailabilityMutationResponse,
    responses={
        400: {"description": "Invalid job id or tenant header"},
        404: {"description": "Job not found"},
    },
)
def refresh_job_availability(
    job_id: str,
    db: DbSession,
    _user: Annotated[object, Depends(get_current_user)],
    tenant_context: Annotated[TenantContext, Depends(get_tenant_context)],
) -> JobAvailabilityMutationResponse:
    """Report whether this deployment can refresh availability through a compliant source sync."""
    job = _get_scoped_job(db, job_id, tenant_id=tenant_context.tenant_id)
    source = _primary_source(job)
    refresh_kind = source_refresh_kind(source)
    if refresh_kind == "prohibited":
        return _mutation_response(
            job,
            "Availability refresh is disabled for this source in hosted deployments.",
        )
    if refresh_kind == "compliant_ats":
        return _mutation_response(
            job,
            "Refresh must be run through the configured ATS source sync for this workspace.",
            queued=False,
        )
    return _mutation_response(job, "Availability refresh is not available for this source.")
