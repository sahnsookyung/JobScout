#!/usr/bin/env python3
"""
Pipeline endpoints - trigger and monitor matching pipeline.
"""

import json
import os
import asyncio
import hashlib
import hmac
import logging
import re
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Optional
from urllib.parse import quote

from fastapi import APIRouter, UploadFile, File, HTTPException, Form, Request, Depends, Query
from fastapi.responses import StreamingResponse, JSONResponse

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from core.resume_selection import (
    build_resume_fingerprint,
    evaluate_resume_eligibility,
    evaluate_resume_preflight,
    resolve_owner_id,
    serialize_owner_id,
)
from core.ephemeral_quota import (
    EphemeralQuotaExceeded,
    EphemeralQuotaUnavailable,
    consume_ephemeral_quota,
)
from core.scraper.jobspy_client import JobSpyClient
from core.redis_streams import (
    _sanitize_log,
    clear_task_cancellation_requested,
    get_redis_client,
    get_task_state,
    set_task_cancellation_requested,
    set_task_state,
    enqueue_job,
    STREAM_MATCHING,
)
from database.uow import job_uow
from database.repositories.resume import ResumeUploadCreateParams
from web.backend.api_error_codes import (
    COMMON_RATE_LIMIT_EXCEEDED,
    PIPELINE_JOB_PROCESSING_START_FAILED,
    PIPELINE_MATCH_ALREADY_RUNNING,
    PIPELINE_MATCH_START_FAILED,
    PIPELINE_MATCH_STOP_FAILED,
    PIPELINE_MATCH_STOP_NOT_FOUND,
    PIPELINE_RESUME_FILE_EMPTY,
    PIPELINE_RESUME_FILE_REQUIRED,
    PIPELINE_RESUME_FILE_TOO_LARGE,
    PIPELINE_RESUME_FILE_UNSUPPORTED,
    PIPELINE_RESUME_HASH_MISMATCH,
    PIPELINE_RESUME_NOT_READY,
    PIPELINE_RESUME_REUPLOAD_REQUIRED,
    PIPELINE_RESUME_UPLOAD_IN_PROGRESS,
    PIPELINE_RESUME_UPLOAD_NOT_FOUND,
    PIPELINE_RESUME_UPLOAD_NOT_RETRYABLE,
    PIPELINE_STATUS_LOOKUP_FAILED,
    PIPELINE_TASK_INVALID_ID,
    PIPELINE_TASK_NOT_FOUND,
)
from ..dependencies import get_current_user, require_platform_admin
from ..config import get_config
from ..models.responses import (
    ApiError,
    FetchSourceExternalStatusResponse,
    FetchSourceHealthResponse,
    FetchSourceResponse,
    FetchSourcesResponse,
    PipelineTaskResponse,
    PipelineStatusResponse,
    ResumeEligibilityResponse,
    ResumeHashCheckResponse,
    ResumePreflightResponse,
    SourceFetchResponse,
    ResumeUploadResponse,
    ResumeStatusResponse,
)
from ..models.requests import (
    ResumeHashCheckRequest,
    ResumePreflightRequest,
    ResumeRetryRequest,
    ResumeSelectRequest,
    SourceFetchRequest,
)
from etl.external_seed_fetcher import (
    ExternalSeedFetchError,
    SOURCE_POLICY_DISABLED_REASON,
    external_seed_fetcher_catalog_status,
    fetch_and_import_external_seed_source,
    get_external_seed_fetcher_config,
    get_external_seed_fetcher_status,
)
from etl.resume import ResumeParser
from etl.resume.file_safety import ResumeFileSafetyError, validate_resume_content
from web.backend.services.clients import (
    INTERNAL_ORCHESTRATOR_URL_ENV,
    ORCHESTRATOR_URL_ENV,
    resolve_service_url,
)
from web.shared.constants import RESUME_MAX_SIZE
from database.models import (
    RESUME_UPLOAD_FAILED_RETRYABLE,
    RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED,
    RESUME_UPLOAD_IN_PROGRESS,
    RESUME_UPLOAD_PENDING,
    RESUME_UPLOAD_READY,
)

logger = logging.getLogger(__name__)

# Constants
TASK_NOT_FOUND_DETAIL = "Task not found"
TASK_NOT_FOUND_OR_EXPIRED_DETAIL = "Task not found or expired"
ACTIVE_TASK_ID_KEY_PREFIX = "pipeline:active_task_id"
LATEST_MATCHING_TASK_ID_KEY_PREFIX = "pipeline:latest_matching_task"
LATEST_UPLOAD_TASK_ID_KEY_PREFIX = "resume:upload:latest_task_id"
STOP_PIPELINE_ERROR = "Failed to stop pipeline"
STALE_RESUME_UPLOAD_TIMEOUT_SECONDS = 600
RESUME_ETL_WAIT_TIMEOUT_SECONDS = float(
    os.getenv("RESUME_ETL_WAIT_TIMEOUT_SECONDS", "600")
)
RESUME_PROCESSING_FAILED_MESSAGE = "Resume processing failed."
RESUME_PROCESSING_COMPLETED_MESSAGE = "Resume processing completed successfully."
RESUME_PROCESSING_TIMED_OUT_MESSAGE = "Resume processing timed out. Please retry."
MATCHING_PHASES = (
    "initializing",
    "loading_resume",
    "matching_jobs",
    "scoring",
    "saving",
    "notifying",
    "completed",
)
RESUME_PHASES = (
    "loading_resume",
    "extracting_resume",
    "embedding_resume",
    "completed",
)
ACTIVE_MATCHING_STATUSES = {
    "queued",
    "pending",
    "running",
    "cancellation_requested",
    "persisting",
}
USER_SAFE_WARNING_MESSAGES = {
    "matching_enqueue_failed": "Resume processing finished, but matching did not start automatically.",
    "no_jobs_ready": "No prepared jobs were available to score yet.",
    "jobs_preparing": "Some jobs are still being prepared for matching.",
    "notification_disabled": "Notifications are disabled for this run.",
    "notification_skipped": "Notifications were checked but not sent for this run.",
    "notification_no_channel": "No notification channel is configured for this run.",
    "notification_delivery_rejected": "No notification channel accepted delivery for this run.",
    "scorer_degraded": "Some relevance scoring used the deterministic fallback.",
    "stale_resume": "A newer resume upload is active, so this run is no longer current.",
    "matching_backlog_no_progress": "Some prepared jobs remain eligible for matching, but this page made no progress.",
    "matching_backlog_page_queued": "More eligible jobs are waiting; another matching page is already queued.",
    "matching_backlog_page_enqueued": "More eligible jobs are waiting; another matching page was queued.",
    "matching_backlog_enqueue_failed": "More eligible jobs are waiting, but the next matching page could not be queued.",
}
USER_SAFE_FAILURES = {
    "resume_parse_failed": ("Resume parsing failed. Try a simpler PDF/DOCX or upload plain text.", "retry_resume", True),
    "resume_embedding_failed": ("Resume embedding failed after parsing. Retry processing the resume.", "retry_resume", True),
    "resume_processing_failed": ("Resume processing failed. Retry or upload a cleaner resume file.", "retry_resume", True),
    "matching_enqueue_failed": ("Matching could not be started. Try running matching again.", "run_matching", True),
    "matching_failed": ("Matching failed before results could be saved. Try running matching again.", "run_matching", True),
    "scoring_failed": ("Scoring failed before results could be saved. Try running matching again.", "run_matching", True),
    "saving_failed": ("Results could not be saved. Try running matching again.", "run_matching", True),
    "notification_failed": ("Matching finished, but notification delivery failed.", "open_notification_settings", True),
    "cancelled": ("This run was cancelled.", "run_matching", True),
}

# Strong references to fire-and-forget upload tasks — prevents GC on Python 3.12+
# where the event loop only keeps weak refs to asyncio.Task objects.
_upload_tasks: set = set()

limiter = Limiter(key_func=get_remote_address)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# Pre-compiled pattern for task_id validation
# Format used by orchestrator: "match-{8 hex chars}" e.g., "match-a1b2c3d4"
TASK_ID_PATTERN = re.compile(r'^[a-zA-Z0-9-]{1,50}$')
SHA256_HEX_PATTERN = re.compile(r'^[0-9a-fA-F]{64}$')
XXH64_HEX_PATTERN = re.compile(r'^[0-9a-fA-F]{16}$')
JOB_BOARD_TAG = "job board"
JOBSPY_SITE_TYPES = {
    "indeed",
    "glassdoor",
    "linkedin",
    "google",
    "zip_recruiter",
    "tokyodev",
    "japandev",
}
ATS_SITE_TYPES = {"greenhouse", "lever", "ashby", "hubspot", "workday"}
SUPPORTED_ATS_API_SITE_TYPES = {"greenhouse", "lever", "ashby"}
DEPLOYMENT_POLICY_DISABLED_FETCH_MODES = {"seed_website"}
PROVIDER_NAMES = {
    "jobspy_api": "JobSpy",
    "seed_website": "Seed website",
    "custom_source": "Custom source",
    "ats_api": "ATS API",
}

SOURCE_METADATA: dict[str, dict[str, object]] = {
    "tokyodev": {
        "display_name": "TokyoDev",
        "seed_url": "https://www.tokyodev.com/jobs",
        "description": "English-friendly software roles in Japan.",
        "tags": ["japan", "tokyo", "english", "startup", "software"],
    },
    "japandev": {
        "display_name": "Japan Dev",
        "seed_url": "https://japan-dev.com/jobs",
        "description": "Japan-focused developer roles with language and seniority filters.",
        "tags": ["japan", "developer", "english", "visa", "software"],
    },
    "indeed": {
        "display_name": "Indeed",
        "seed_url": "https://www.indeed.com",
        "description": "Broad job-board search through the JobSpy API.",
        "tags": ["general", JOB_BOARD_TAG, "api", "global"],
    },
    "glassdoor": {
        "display_name": "Glassdoor",
        "seed_url": "https://www.glassdoor.com/Job",
        "description": "Company and salary-aware listings through the JobSpy API.",
        "tags": ["company", "salary", JOB_BOARD_TAG, "global"],
    },
    "linkedin": {
        "display_name": "LinkedIn",
        "seed_url": "https://www.linkedin.com/jobs",
        "description": "Professional network job listings with optional descriptions.",
        "tags": ["network", "professional", JOB_BOARD_TAG, "global"],
    },
    "google": {
        "display_name": "Google Jobs",
        "seed_url": "https://www.google.com/search?q=jobs",
        "description": "Aggregated search results when configured in JobSpy.",
        "tags": ["aggregator", "search", "global"],
    },
    "zip_recruiter": {
        "display_name": "ZipRecruiter",
        "seed_url": "https://www.ziprecruiter.com/jobs-search",
        "description": "General job-board listings when configured in JobSpy.",
        "tags": ["general", JOB_BOARD_TAG, "global"],
    },
    "greenhouse": {
        "display_name": "Greenhouse",
        "description": "Tenant ATS API sync through the SaaS integration scheduler.",
        "tags": ["ats", "api", "company careers"],
    },
    "lever": {
        "display_name": "Lever",
        "description": "Tenant ATS API sync through the SaaS integration scheduler.",
        "tags": ["ats", "api", "company careers"],
    },
    "ashby": {
        "display_name": "Ashby",
        "description": "Tenant ATS API sync through the SaaS integration scheduler.",
        "tags": ["ats", "api", "company careers"],
    },
    "hubspot": {
        "display_name": "HubSpot",
        "description": "HubSpot ATS API source when configured by the deployment.",
        "tags": ["ats", "api", "company careers"],
    },
    "workday": {
        "display_name": "Workday",
        "description": "Workday ATS API source when configured by the deployment.",
        "tags": ["ats", "api", "company careers"],
    },
}


class PipelineApiError(HTTPException):
    """Endpoint-local error used to return canonical ApiError bodies."""

    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        detail: Optional[str] = None,
    ) -> None:
        super().__init__(status_code=status_code, detail=message)
        self.status_code = status_code
        self.payload = ApiError(
            code=code,
            message=message,
            detail=detail,
        )


def _pipeline_error_response(exc: PipelineApiError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.payload.model_dump(exclude_none=True),
    )


def _bind_public_operation_lease(request: Request, task_id: Optional[str]) -> None:
    """Let the cloud dependency transfer its request lease to durable work."""
    if not task_id:
        return
    binder = getattr(request.state, "bind_public_operation_lease", None)
    if callable(binder):
        binder(task_id)


def _raise_pipeline_error(
    *,
    status_code: int,
    code: str,
    message: str,
    detail: Optional[str] = None,
) -> None:
    raise PipelineApiError(
        status_code=status_code,
        code=code,
        message=message,
        detail=detail,
    )


def _validate_task_id(task_id: str) -> bool:
    """Validate task_id using allowlist validation.

    Prevents CWE-952: URL manipulation and path injection attacks.

    Args:
        task_id: The task identifier from user input

    Returns:
        True if valid, False otherwise
    """
    if not task_id or not isinstance(task_id, str):
        return False
    if len(task_id) > 50:
        return False
    return bool(TASK_ID_PATTERN.match(task_id))

def _dedupe_strings(values: list[object]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        text = str(value or "").strip()
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped

def _source_metadata(site_type: str) -> dict[str, object]:
    metadata = SOURCE_METADATA.get(site_type, {})
    return {
        "display_name": metadata.get("display_name") or site_type.replace("_", " ").title(),
        "seed_url": metadata.get("seed_url"),
        "description": metadata.get("description"),
        "tags": list(metadata.get("tags") or []),
    }

def _source_option_keywords(options: dict[str, object]) -> list[str]:
    keywords: list[object] = []
    for key, value in options.items():
        keywords.append(key)
        if isinstance(value, list):
            keywords.extend(value)
        elif isinstance(value, dict):
            keywords.extend(value.keys())
            keywords.extend(value.values())
        else:
            keywords.append(value)
    return _dedupe_strings(keywords)

def _source_search_keywords(
    *,
    site_type: str,
    display_name: str,
    seed_url: Optional[str],
    description: Optional[str],
    tags: list[str],
    scraper_cfg,
) -> list[str]:
    return _dedupe_strings([
        site_type,
        display_name,
        seed_url,
        description,
        scraper_cfg.search_term,
        scraper_cfg.location,
        scraper_cfg.country,
        *tags,
        *_source_option_keywords(dict(scraper_cfg.options or {})),
    ])

def _source_fetch_mode(site_type: str, scraper_cfg, seed_url: Optional[str]) -> str:
    explicit_mode = str(
        getattr(scraper_cfg, "fetch_mode", None)
        or dict(scraper_cfg.options or {}).get("fetch_mode")
        or ""
    ).strip().lower()
    if explicit_mode in {"seed_website", "jobspy_api", "ats_api", "custom_source"}:
        return explicit_mode
    if site_type in ATS_SITE_TYPES:
        return "ats_api"
    if site_type in JOBSPY_SITE_TYPES:
        return "jobspy_api"
    if seed_url:
        return "seed_website"
    return "custom_source"

def _production_like_deployment() -> bool:
    env = (
        os.getenv("JOBSCOUT_ENV")
        or os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or "development"
    )
    return env.strip().lower() in {"production", "prod", "staging"}

def _source_provider_name(
    site_type: str,
    fetch_mode: str,
    external_status: dict[str, object] | None = None,
) -> str:
    if fetch_mode == "seed_website" and external_status:
        status = str(external_status.get("status") or "")
        if external_status.get("configured") or status in {"configured", "degraded", "ok", "rate_limited"}:
            return "Worker seed fetcher"
    if fetch_mode == "ats_api":
        display_name = SOURCE_METADATA.get(site_type, {}).get("display_name")
        return f"{display_name or site_type.replace('_', ' ').title()} ATS"
    return PROVIDER_NAMES.get(fetch_mode, fetch_mode.replace("_", " ").title())

def _build_fetch_source_response(
    scraper_cfg,
    *,
    api_health: Optional[FetchSourceHealthResponse] = None,
    external_statuses: Optional[dict[str, dict[str, object]]] = None,
) -> FetchSourceResponse:
    site_types = list(scraper_cfg.site_type or [])
    site_type = str(next(iter(site_types), "unknown"))
    metadata = _source_metadata(site_type)
    display_name = scraper_cfg.display_name or str(metadata["display_name"])
    seed_url = scraper_cfg.seed_url or metadata.get("seed_url")
    description = scraper_cfg.description or metadata.get("description")
    tags = _dedupe_strings([*list(metadata.get("tags") or []), *list(scraper_cfg.tags or [])])
    fetch_mode = _source_fetch_mode(site_type, scraper_cfg, str(seed_url) if seed_url else None)
    enabled = bool(getattr(scraper_cfg, "enabled", True))
    disabled_reason = (
        "source_disabled"
        if not enabled
        else (
            SOURCE_POLICY_DISABLED_REASON
            if _production_like_deployment() and fetch_mode in DEPLOYMENT_POLICY_DISABLED_FETCH_MODES
            else None
        )
    )
    external_status = (
        (external_statuses or {}).get(site_type)
        if fetch_mode == "seed_website"
        else None
    )
    provider_diagnostics: dict[str, object] = {
        "fetch_mode": fetch_mode,
        "deployment_allowed": disabled_reason is None,
    }
    if not enabled:
        availability_status = "disabled"
        availability_reason = "source_disabled"
        api_fetch_available = False
    elif fetch_mode == "ats_api":
        provider_diagnostics["adapter"] = site_type
        if site_type in SUPPORTED_ATS_API_SITE_TYPES:
            availability_status = "available"
            availability_reason = "ats_api_available"
            api_fetch_available = True
        else:
            availability_status = "not_supported"
            availability_reason = "not_supported_api_adapter_missing"
            api_fetch_available = False
    elif fetch_mode == "jobspy_api":
        provider_diagnostics["adapter"] = "jobspy"
        if disabled_reason is not None:
            availability_status = "deployment_disabled"
            availability_reason = disabled_reason
            api_fetch_available = False
        elif bool(api_health and api_health.available):
            availability_status = "available"
            availability_reason = "jobspy_api_available"
            api_fetch_available = True
        else:
            availability_status = "unavailable"
            availability_reason = (
                api_health.status
                if api_health is not None
                else "jobspy_api_status_unknown"
            )
            api_fetch_available = False
    elif fetch_mode == "seed_website" and disabled_reason is not None:
        provider_diagnostics["adapter"] = "external_seed_fetcher"
        availability_status = "deployment_disabled"
        availability_reason = disabled_reason
        api_fetch_available = False
    else:
        availability_status = "available" if disabled_reason is None else "deployment_disabled"
        availability_reason = "custom_source" if disabled_reason is None else disabled_reason
        api_fetch_available = False
    return FetchSourceResponse(
        site_type=site_type,
        display_name=display_name,
        seed_url=str(seed_url) if seed_url else None,
        description=str(description) if description else None,
        tags=tags,
        search_keywords=_source_search_keywords(
            site_type=site_type,
            display_name=display_name,
            seed_url=str(seed_url) if seed_url else None,
            description=str(description) if description else None,
            tags=tags,
            scraper_cfg=scraper_cfg,
        ),
        fetch_mode=fetch_mode,
        enabled=enabled,
        provider_name=_source_provider_name(site_type, fetch_mode, external_status),
        search_term=scraper_cfg.search_term,
        location=scraper_cfg.location,
        country=scraper_cfg.country,
        results_wanted=scraper_cfg.results_wanted,
        hours_old=scraper_cfg.hours_old,
        options=dict(scraper_cfg.options or {}),
        api_health=api_health if fetch_mode == "jobspy_api" else None,
        external_fetch_status=(
            FetchSourceExternalStatusResponse(**external_status)
            if external_status
            else None
        ),
        api_fetch_available=api_fetch_available,
        deployment_allowed=disabled_reason is None,
        disabled_reason=disabled_reason,
        availability_status=availability_status,
        availability_reason=availability_reason,
        provider_diagnostics=provider_diagnostics,
    )

def _source_matches_query(source: FetchSourceResponse, search: Optional[str]) -> bool:
    terms = [
        term
        for term in re.split(r"\s+", (search or "").strip().lower())
        if term
    ]
    if not terms:
        return True

    haystack = " ".join(
        [
            source.site_type,
            source.display_name,
            source.seed_url or "",
            source.description or "",
            source.fetch_mode,
            source.provider_name or "",
            source.search_term or "",
            source.location or "",
            source.country or "",
            *source.tags,
            *source.search_keywords,
        ]
    ).lower()
    return all(term in haystack for term in terms)

def _jobspy_health(config) -> Optional[FetchSourceHealthResponse]:
    if not config.jobspy or not config.jobspy.url:
        return FetchSourceHealthResponse(
            available=False,
            status="not_configured",
            error="JobSpy API URL is not configured",
        )

    with closing(JobSpyClient(
        base_url=config.jobspy.url,
        request_timeout_seconds=config.jobspy.request_timeout_seconds,
    )) as client:
        result = client.check_health(
            timeout_seconds=getattr(config.jobspy, "health_timeout_seconds", 2.0),
        )
        return FetchSourceHealthResponse(**result)

def _active_task_key(owner_id: str) -> str:
    return f"{ACTIVE_TASK_ID_KEY_PREFIX}:{owner_id}"

def _latest_matching_task_key(owner_id: str) -> str:
    return f"{LATEST_MATCHING_TASK_ID_KEY_PREFIX}:{owner_id}"

def _latest_upload_task_key(owner_id: str) -> str:
    return f"{LATEST_UPLOAD_TASK_ID_KEY_PREFIX}:{owner_id}"


def add_rate_limit_handlers(app):
    """Add rate limit exception handlers to the FastAPI app."""
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


def _rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    if request.url.path.startswith("/api/pipeline"):
        return JSONResponse(
            status_code=429,
            content=ApiError(
                code=COMMON_RATE_LIMIT_EXCEEDED,
                message=str(exc),
            ).model_dump(),
        )
    return JSONResponse(
        status_code=429,
        content={"detail": str(exc)}
    )


@router.get("/sources", response_model=FetchSourcesResponse)
def get_fetch_sources(
    request: Request,
    _user: Annotated[None, Depends(require_platform_admin)] = None,
    search: Annotated[
        Optional[str],
        Query(max_length=120, description="Optional whitespace-delimited source search"),
    ] = None,
    include_status: Annotated[
        bool,
        Query(description="Check the configured JobSpy API health endpoint"),
    ] = False,
):
    """Return configured seed websites and API-backed fetch source metadata."""
    config = get_config()
    api_health = _jobspy_health(config) if include_status else None
    external_statuses: dict[str, dict[str, object]] = {}
    external_config = get_external_seed_fetcher_config()
    if include_status:
        tenant_id = getattr(request.state, "tenant_id", None)
        external_statuses = dict(
            get_external_seed_fetcher_status(tenant_id=tenant_id).get("sources") or {}
        )
    else:
        external_statuses = {
            source: external_seed_fetcher_catalog_status(source, config=external_config) or {}
            for source in ("tokyodev", "japandev")
        }
    all_sources = [
        _build_fetch_source_response(
            scraper_cfg,
            api_health=api_health,
            external_statuses=external_statuses,
        )
        for scraper_cfg in config.scrapers
    ]
    sources = [
        source
        for source in all_sources
        if _source_matches_query(source, search)
    ]
    seed_websites = [
        source.seed_url
        for source in sources
        if source.seed_url is not None
    ]
    deployment_api_sources_available = any(
        source.enabled
        and source.deployment_allowed
        and source.fetch_mode in {"ats_api", "jobspy_api"}
        for source in sources
    )
    return FetchSourcesResponse(
        success=True,
        jobspy_url=config.jobspy.url if config.jobspy else None,
        api_based_fetching=deployment_api_sources_available,
        search_query=search,
        total_count=len(all_sources),
        filtered_count=len(sources),
        seed_websites=seed_websites,
        sources=sources,
    )

def _require_source_fetch_admin(request: Request) -> None:
    tenant_role = getattr(request.state, "tenant_role", None)
    if tenant_role is None:
        if os.getenv("JOBSCOUT_ENV", "").strip().lower() in {"production", "prod", "staging"}:
            raise PipelineApiError(
                status_code=403,
                code="pipeline.source_fetch_admin_required",
                message="Tenant admin access is required for hosted source fetching.",
            )
        return
    if tenant_role not in {"owner", "admin"}:
        raise PipelineApiError(
            status_code=403,
            code="pipeline.source_fetch_admin_required",
            message="Tenant admin access is required for hosted source fetching.",
        )

@router.post("/source-fetch", response_model=SourceFetchResponse)
def fetch_seed_source_endpoint(
    request: Request,
    body: SourceFetchRequest,
    _user: Annotated[None, Depends(require_platform_admin)] = None,
):
    """Fetch one configured seed website through the external Worker-backed path."""
    try:
        _require_source_fetch_admin(request)
        summary = fetch_and_import_external_seed_source(
            body.source,
            tenant_id=getattr(request.state, "tenant_id", None),
            limit=body.limit,
        )
        status_code = 200 if summary.success else 429
        return JSONResponse(status_code=status_code, content=summary.as_dict())
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)
    except ExternalSeedFetchError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content=SourceFetchResponse(
                success=False,
                source=body.source,
                status="degraded",
                failure_class=exc.failure_class,
                warnings=[exc.message],
            ).model_dump(),
        )


@router.post(
    "/process-jobs",
    response_model=PipelineTaskResponse,
    responses={
        409: {"model": ApiError, "description": "A matching or processing task is already in progress."},
        500: {"model": ApiError, "description": "Internal server error"},
    },
)
def process_imported_jobs_endpoint(
    user: Annotated[None, Depends(require_platform_admin)] = None,
):
    """Trigger extraction and embedding for already imported jobs."""
    try:
        return _start_imported_job_processing(user)
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)


@router.post(
    "/run-matching",
    response_model=PipelineTaskResponse,
    responses={
        400: {"model": ApiError, "description": "Matching cannot start with the current resume state."},
        409: {"model": ApiError, "description": "A matching task or resume upload is already in progress."},
        500: {"model": ApiError, "description": "Internal server error"},
    }
)
def run_matching_pipeline_endpoint(
    request: Request,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """
    Trigger the full matching pipeline in the background.

    Returns immediately with a task_id that can be used to poll for status via SSE.
    The pipeline will:
    - Extract resume (if changed)
    - Generate embeddings
    - Run vector-based job matching
    - Calculate fit/want scores
    - Save results to database
    - Send notifications (if configured)

    Raises:
        409: Pipeline is already running.
        500: Internal error starting the pipeline.
    """
    try:
        response = _start_matching(user, tenant_id=getattr(request.state, "tenant_id", None))
        _bind_public_operation_lease(request, response.task_id)
        return response
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)


def _guard_resume_not_uploading(redis, owner_id: str) -> None:
    """Raise 409 if a resume upload is currently in progress.

    No-ops silently when Redis is unavailable.
    """
    try:
        if not redis:
            return
        latest_task_id = redis.get(_latest_upload_task_key(owner_id))
        if not latest_task_id:
            return
        decoded_task_id = _decode_redis_value(latest_task_id)
        state = get_task_state(decoded_task_id)
        if (
            state
            and state.get("status") in ("processing", "running", "pending")
            and _latest_resume_upload_uses_task(owner_id, decoded_task_id)
        ):
            # "processing" = web-backend initial write; "running" = orchestrator stage active.
            # Both mean extraction/embedding is in progress; matching against the old
            # fingerprint would produce stale results.
            _raise_pipeline_error(
                status_code=409,
                code=PIPELINE_RESUME_UPLOAD_IN_PROGRESS,
                message="Resume is currently being processed. Please wait and try again.",
            )
    except PipelineApiError:
        raise
    except Exception:
        pass  # Redis unavailable — proceed without guard


def _latest_resume_upload_uses_task(owner_id: str, task_id: str) -> bool:
    """Return True when the owner's latest upload still points at this task."""
    try:
        owner_lookup = uuid.UUID(owner_id)
    except (TypeError, ValueError, AttributeError):
        owner_lookup = owner_id

    try:
        with job_uow() as repo:
            latest_upload = repo.get_latest_resume_upload(owner_lookup)
    except Exception:
        logger.warning("Failed to load latest upload while checking resume guard", exc_info=True)
        return True

    if latest_upload is None:
        return False

    return (
        latest_upload.status in {RESUME_UPLOAD_PENDING, RESUME_UPLOAD_IN_PROGRESS}
        and latest_upload.processing_task_id == task_id
    )


def _clear_latest_upload_task_marker(owner_id: str) -> None:
    """Best-effort cleanup of the latest-upload Redis marker for an owner."""
    try:
        redis = get_redis_client()
        redis.delete(_latest_upload_task_key(owner_id))
    except Exception:
        logger.warning("Failed to clear latest upload task marker for owner %s", owner_id)


def _resume_task_belongs_to_owner(state: dict, owner_id) -> bool:
    """Return True when resume task state belongs to the authenticated user."""
    owner_key = serialize_owner_id(owner_id)
    state_owner = state.get("owner_id")
    if state_owner is not None:
        return serialize_owner_id(state_owner) == owner_key

    upload_id = state.get("upload_id")
    if not upload_id:
        return False

    try:
        with job_uow() as repo:
            return repo.get_resume_upload(upload_id, owner_id) is not None
    except Exception:
        logger.warning("Failed to verify resume task ownership for %s", upload_id, exc_info=True)
        return False


def _task_state_belongs_to_owner(state: dict, owner_id) -> bool:
    """Return True when generic task state belongs to the authenticated user."""
    state_owner = state.get("owner_id")
    if state_owner is None:
        return False
    return serialize_owner_id(state_owner) == serialize_owner_id(owner_id)


def _ensure_task_visible_to_owner(state: dict, owner_id) -> None:
    """Hide task state unless Redis says it belongs to the authenticated owner."""
    if _task_state_belongs_to_owner(state, owner_id):
        return
    task_id = state.get("task_id")
    if task_id and _active_task_id_for_owner(owner_id) == str(task_id):
        return
    _raise_pipeline_error(
        status_code=404,
        code=PIPELINE_TASK_NOT_FOUND,
        message=TASK_NOT_FOUND_OR_EXPIRED_DETAIL,
    )


def _active_task_id_for_owner(owner_id) -> str | None:
    """Return the active matching task id for an owner when Redis is reachable."""
    try:
        redis = get_redis_client()
        task_id_raw = redis.get(_active_task_key(serialize_owner_id(owner_id)))
    except Exception:
        logger.warning("Failed to load active task marker for owner", exc_info=True)
        return None
    if not task_id_raw:
        return None
    return _decode_redis_value(task_id_raw)


def _get_owned_resume_task_state(task_id: str, owner_id) -> dict:
    """Load resume-upload task state and enforce owner visibility."""
    state = get_task_state(task_id)
    if state is None:
        _raise_pipeline_error(
            status_code=404,
            code=PIPELINE_TASK_NOT_FOUND,
            message=TASK_NOT_FOUND_OR_EXPIRED_DETAIL,
        )

    task_type = state.get("task_type")
    if task_type not in (None, "resume_upload"):
        _raise_pipeline_error(
            status_code=404,
            code=PIPELINE_TASK_NOT_FOUND,
            message=TASK_NOT_FOUND_OR_EXPIRED_DETAIL,
        )

    if not _resume_task_belongs_to_owner(state, owner_id):
        _raise_pipeline_error(
            status_code=404,
            code=PIPELINE_TASK_NOT_FOUND,
            message=TASK_NOT_FOUND_OR_EXPIRED_DETAIL,
        )

    return state


def _get_matching_redis_client():
    """Return a Redis client for matching orchestration, or None on failure."""
    try:
        return get_redis_client()
    except Exception:
        logger.warning("Redis unavailable — proceeding without active task check")
        return None


def _decode_redis_value(value) -> str:
    """Normalize Redis values to strings."""
    return value if isinstance(value, str) else value.decode()


def _normalize_matching_step(
    step: Optional[str],
    *,
    default: Optional[str] = None,
) -> Optional[str]:
    """Map raw backend stage names to the canonical matching step vocabulary."""
    if not step:
        return default

    normalized = step.strip().lower().replace(" ", "_").replace("-", "_")
    canonical_steps = {
        "initializing",
        "loading_resume",
        "vector_matching",
        "scoring",
        "saving_results",
        "notifying",
    }
    if normalized in canonical_steps:
        return normalized

    aliases = {
        "start": "initializing",
        "starting": "initializing",
        "queued": "initializing",
        "loading": "loading_resume",
        "resume_loading": "loading_resume",
        "resume_loaded": "loading_resume",
        "extracting": "loading_resume",
        "embedding": "loading_resume",
        "matching": "vector_matching",
        "match": "vector_matching",
        "vector_match": "vector_matching",
        "saving": "saving_results",
        "save": "saving_results",
        "saved": "saving_results",
        "notification": "notifying",
        "notify": "notifying",
    }
    return aliases.get(normalized, default)


def _utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp for task-state metadata."""
    return datetime.now(timezone.utc).isoformat()


def _matching_phase_from_step(status: str, step: Optional[str]) -> str:
    if status in {"completed", "failed", "cancelled"}:
        return status

    normalized = _normalize_matching_step(step, default="initializing")
    if normalized == "vector_matching":
        return "matching_jobs"
    if normalized == "saving_results":
        return "saving"
    if normalized in {"initializing", "loading_resume", "scoring", "notifying"}:
        return normalized
    return "initializing"


def _resume_phase_from_step(status: str, step: Optional[str]) -> str:
    if status == "completed":
        return "completed"
    if status == "failed":
        return "failed"
    if step == "embedding":
        return "embedding_resume"
    if step == "extracting":
        return "extracting_resume"
    return "loading_resume"


def _progress_for_phase(
    phase: str,
    *,
    status: str,
    phases: tuple[str, ...],
    started_at: Optional[str] = None,
    updated_at: Optional[str] = None,
) -> dict[str, Any]:
    total_steps = len(phases)
    if phase in phases:
        current_step = phases.index(phase) + 1
    elif status in {"completed", "failed", "cancelled"}:
        current_step = total_steps
    else:
        current_step = 1

    if status == "completed":
        percent = 100
    else:
        denominator = max(total_steps - 1, 1)
        percent = min(99, int(((current_step - 1) / denominator) * 100))

    return {
        "current_step": current_step,
        "total_steps": total_steps,
        "percent": percent,
        "started_at": started_at,
        "updated_at": updated_at,
    }


def _safe_warning(code: str) -> dict[str, str]:
    return {
        "code": code,
        "message": USER_SAFE_WARNING_MESSAGES.get(
            code,
            "A non-critical issue occurred during this run.",
        ),
    }


def _safe_warnings_from_state(state: dict, stats: dict[str, Any]) -> list[dict[str, str]]:
    seen: set[str] = set()
    warnings: list[dict[str, str]] = []

    for warning in state.get("warnings") or []:
        if isinstance(warning, dict):
            code = str(warning.get("code") or "pipeline_warning")[:80]
        else:
            code = str(warning or "pipeline_warning")[:80]
        if code in seen:
            continue
        seen.add(code)
        warnings.append(_safe_warning(code))

    if state.get("stale_due_to_newer_upload") and "stale_resume" not in seen:
        seen.add("stale_resume")
        warnings.append(_safe_warning("stale_resume"))

    pending_jobs = int(stats.get("jobs_pending_extraction") or 0) + int(
        stats.get("jobs_pending_embedding") or 0
    )
    if pending_jobs > 0 and "jobs_preparing" not in seen:
        seen.add("jobs_preparing")
        warnings.append(_safe_warning("jobs_preparing"))

    if (
        state.get("status") == "completed"
        and state.get("task_type") != "resume_upload"
        and int(stats.get("matches_saved") or 0) == 0
        and int(stats.get("jobs_ready_to_score") or 0) == 0
        and "no_jobs_ready" not in seen
    ):
        warnings.append(_safe_warning("no_jobs_ready"))

    return warnings


def _public_stats_from_state(state: dict) -> dict[str, Any]:
    stats = dict(state.get("stats") or {})
    result_data = state.get("result", {}) or {}
    if "matches_count" in result_data:
        stats.setdefault("candidates_considered", result_data.get("matches_count") or 0)
        stats.setdefault("matches_selected", result_data.get("matches_count") or 0)
    if "saved_count" in result_data:
        stats.setdefault("matches_saved", result_data.get("saved_count") or 0)
    if "notified_count" in result_data:
        stats.setdefault("notifications_sent", result_data.get("notified_count") or 0)
    if "jobs_imported" in result_data:
        stats.setdefault("jobs_imported", result_data.get("jobs_imported") or 0)
    elif "scraped_jobs" in result_data:
        stats.setdefault("jobs_imported", result_data.get("scraped_jobs") or 0)
    if "jobs_processed" in result_data:
        stats.setdefault("jobs_processed", result_data.get("jobs_processed") or 0)
    if "extracted_count" in result_data:
        stats.setdefault("jobs_extracted", result_data.get("extracted_count") or 0)
    if "embedded_count" in result_data:
        stats.setdefault("jobs_embedded", result_data.get("embedded_count") or 0)
    return stats


def _public_failure_from_state(state: dict, *, task_type: str) -> Optional[dict[str, Any]]:
    status = state.get("status")
    if status not in {"failed", "cancelled"}:
        return None

    if status == "cancelled":
        code = "cancelled"
    elif task_type == "resume_upload":
        step = state.get("step")
        if step == "embedding":
            code = "resume_embedding_failed"
        elif step == "extracting":
            code = "resume_parse_failed"
        else:
            code = "resume_processing_failed"
    else:
        step = _normalize_matching_step(state.get("step"), default="initializing")
        if step == "scoring":
            code = "scoring_failed"
        elif step == "saving_results":
            code = "saving_failed"
        elif step == "notifying":
            code = "notification_failed"
        else:
            code = "matching_failed"

    message, next_action, retryable = USER_SAFE_FAILURES[code]
    return {
        "code": code,
        "user_message": message,
        "retryable": retryable,
        "next_action": next_action,
    }


def _build_pipeline_status_response(task_id: str, state: dict) -> PipelineStatusResponse:
    """Convert Redis task state into the shared pipeline status response shape."""
    result_data = state.get("result", {}) or {}
    status = state.get("status", "unknown")
    default_step = "initializing" if status in ("pending", "running") else None
    step = _normalize_matching_step(state.get("step"), default=default_step)
    phase = _matching_phase_from_step(status, step)
    stats = _public_stats_from_state(state)
    failure = _public_failure_from_state(state, task_type="matching")
    return PipelineStatusResponse(
        task_id=task_id,
        status=status,
        phase=phase,
        observer_timeout=bool(state.get("observer_timeout")),
        reconnect_after_seconds=state.get("reconnect_after_seconds"),
        progress=_progress_for_phase(
            phase,
            status=status,
            phases=MATCHING_PHASES,
            started_at=state.get("started_at"),
            updated_at=state.get("updated_at"),
        ),
        stats=stats,
        warnings=_safe_warnings_from_state(state, stats),
        failure=failure,
        upload_id=state.get("upload_id"),
        resume_fingerprint=state.get("resume_fingerprint"),
        step=step,
        matches_count=result_data.get("matches_count"),
        saved_count=result_data.get("saved_count"),
        notified_count=result_data.get("notified_count"),
        execution_time=result_data.get("execution_time"),
        error=failure["user_message"] if failure else None,
        stale_due_to_newer_upload=bool(state.get("stale_due_to_newer_upload")),
        latest_upload_id=state.get("latest_upload_id"),
        latest_resume_fingerprint=state.get("latest_resume_fingerprint"),
        stale_message=state.get("stale_message"),
    )

def _build_resume_eligibility_response(owner_id) -> ResumeEligibilityResponse:
    eligibility = evaluate_resume_eligibility(owner_id)
    return ResumeEligibilityResponse(
        can_run=eligibility.can_run,
        status=eligibility.processing_status,
        message=eligibility.message,
        retryable=eligibility.retryable,
        upload_id=eligibility.upload_id,
        resume_hash=eligibility.resume_hash,
        task_id=eligibility.processing_task_id,
    )


def _build_resume_preflight_response(owner_id, resume_hash: str) -> ResumePreflightResponse:
    preflight = evaluate_resume_preflight(owner_id, resume_hash)
    return ResumePreflightResponse(
        status=preflight.status,
        message=preflight.message,
        retryable=preflight.retryable,
        can_skip_upload=preflight.can_skip_upload,
        resume_hash=preflight.resume_hash,
        upload_id=preflight.upload_id,
        task_id=preflight.processing_task_id,
    )


def _classify_failed_resume_upload(repo, resume_fingerprint: str) -> tuple[str, str, bool]:
    state = repo.get_resume_processing_state(resume_fingerprint)
    structured_resume = repo.get_structured_resume_by_fingerprint(resume_fingerprint)
    message = getattr(state, "user_safe_message", None) or getattr(state, "last_error", None) or "Resume processing failed."

    if structured_resume is not None:
        return RESUME_UPLOAD_FAILED_RETRYABLE, message, True

    return RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED, message, False


def _resume_upload_timed_out(upload) -> bool:
    created_at = getattr(upload, "created_at", None)
    if not isinstance(created_at, datetime):
        return False
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return (
        datetime.now(timezone.utc) - created_at
    ) >= timedelta(seconds=STALE_RESUME_UPLOAD_TIMEOUT_SECONDS)


def _mark_resume_upload_failed_from_stale_task(repo, upload, task_id: str) -> None:
    upload_status, upload_error, retryable = _classify_failed_resume_upload(
        repo, upload.resume_fingerprint
    )
    if upload_error == RESUME_PROCESSING_FAILED_MESSAGE:
        upload_error = RESUME_PROCESSING_TIMED_OUT_MESSAGE

    repo.update_resume_upload(
        upload.id,
        status=upload_status,
        last_error=upload_error,
        processing_task_id=task_id,
        failure_stage="resume_etl",
        failure_class="stale_task",
        retryable=retryable,
        user_safe_message=upload_error,
        failure_debug_context={"reason": "stale_task"},
    )
    now = _utc_now_iso()
    set_task_state(
        task_id,
        {
            "status": "failed",
            "task_type": "resume_upload",
            "step": "embedding" if retryable else "extracting",
            "error": upload_error,
            "upload_status": upload_status,
            "resume_hash": upload.resume_hash,
            "resume_fingerprint": upload.resume_fingerprint,
            "upload_id": str(upload.id),
            "owner_id": serialize_owner_id(upload.owner_id),
            "updated_at": now,
        },
        ttl=3600,
    )


def _reconcile_resume_upload_task(repo, upload):
    task_id = getattr(upload, "processing_task_id", None)
    if not isinstance(task_id, str) or not task_id:
        return upload

    upload_status = getattr(upload, "status", None)

    try:
        state = get_task_state(task_id)
    except Exception:
        logger.warning(
            "Failed to load resume upload task state for %s during reconciliation",
            task_id,
            exc_info=True,
        )
        return upload
    if state is None:
        if _resume_upload_timed_out(upload):
            _mark_resume_upload_failed_from_stale_task(repo, upload, task_id)
            return repo.get_resume_upload(upload.id)
        return upload

    task_status = state.get("status")
    if task_status == "completed" and upload_status != RESUME_UPLOAD_READY:
        repo.update_resume_upload(
            upload.id,
            status=RESUME_UPLOAD_READY,
            last_error=None,
            processing_task_id=task_id,
            retryable=False,
            user_safe_message=RESUME_PROCESSING_COMPLETED_MESSAGE,
        )
        return repo.get_resume_upload(upload.id)

    if task_status == "failed" and upload_status not in {
        RESUME_UPLOAD_FAILED_RETRYABLE,
        RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED,
    }:
        _mark_resume_upload_failed_from_stale_task(repo, upload, task_id)
        return repo.get_resume_upload(upload.id)

    if (
        upload_status in {RESUME_UPLOAD_PENDING, RESUME_UPLOAD_IN_PROGRESS}
        and task_status in {"pending", "processing", "running"}
        and _resume_upload_timed_out(upload)
    ):
        _mark_resume_upload_failed_from_stale_task(repo, upload, task_id)
        return repo.get_resume_upload(upload.id)

    return upload


def _resume_status_from_upload(
    task_id: str,
    upload,
    *,
    matching_task_id: Optional[str] = None,
) -> Optional[ResumeStatusResponse]:
    if upload is None:
        return None

    if upload.status == RESUME_UPLOAD_READY:
        phase = _resume_phase_from_step("completed", None)
        return ResumeStatusResponse(
            task_id=task_id,
            status="completed",
            step=None,
            matching_task_id=matching_task_id,
            phase=phase,
            progress=_progress_for_phase(
                phase,
                status="completed",
                phases=RESUME_PHASES,
            ),
            message=RESUME_PROCESSING_COMPLETED_MESSAGE,
            error=None,
        )

    if upload.status in {
        RESUME_UPLOAD_FAILED_RETRYABLE,
        RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED,
    }:
        state = {"status": "failed", "step": None, "task_type": "resume_upload"}
        failure = _public_failure_from_state(state, task_type="resume_upload")
        phase = _resume_phase_from_step("failed", None)
        return ResumeStatusResponse(
            task_id=task_id,
            status="failed",
            step=None,
            phase=phase,
            progress=_progress_for_phase(
                phase,
                status="failed",
                phases=RESUME_PHASES,
            ),
            failure=failure,
            message=upload.user_safe_message or upload.last_error,
            error=upload.user_safe_message or (failure["user_message"] if failure else RESUME_PROCESSING_FAILED_MESSAGE),
        )

    return None


def _resume_status_message(status: str, step: Optional[str], error: Optional[str]) -> Optional[str]:
    """Return a user-facing message for resume task polling responses."""
    if error:
        return error
    if status == "completed":
        return RESUME_PROCESSING_COMPLETED_MESSAGE
    if status == "failed":
        return RESUME_PROCESSING_FAILED_MESSAGE
    if status == "processing":
        if step == "extracting":
            return "Resume extraction is in progress."
        if step == "embedding":
            return "Resume embedding is in progress."
        return "Resume processing is in progress."
    return None


def _resume_status_from_task_state(task_id: str, state: dict) -> ResumeStatusResponse:
    """Convert owned Redis task state into the shared resume status response shape."""
    status = state.get("status", "unknown")
    public_status = "processing" if status in {"pending", "running"} else status
    step = state.get("step")
    failure = _public_failure_from_state(state, task_type="resume_upload")
    phase = _resume_phase_from_step(public_status, step)
    stats = _public_stats_from_state(state)
    error = failure["user_message"] if failure else None
    return ResumeStatusResponse(
        task_id=task_id,
        status=public_status,
        step=step,
        matching_task_id=state.get("matching_task_id"),
        phase=phase,
        progress=_progress_for_phase(
            phase,
            status=public_status,
            phases=RESUME_PHASES,
            started_at=state.get("started_at"),
            updated_at=state.get("updated_at"),
        ),
        stats=stats,
        warnings=_safe_warnings_from_state(state, stats),
        failure=failure,
        message=_resume_status_message(public_status, step, error),
        error=error,
    )


def _write_resume_ready_state(
    *,
    task_id: str,
    upload_id: str,
    owner_id,
    resume_hash: str,
    resume_fingerprint: str,
    trigger: str,
    tenant_id=None,
) -> Optional[str]:
    """Write final resume-ready state and attach or report the matching enqueue."""
    owner_key = serialize_owner_id(owner_id)
    now = _utc_now_iso()
    try:
        previous_state = get_task_state(task_id) or {}
    except Exception:
        previous_state = {}
    completed_state = {
        "status": "completed",
        "task_type": "resume_upload",
        "upload_status": RESUME_UPLOAD_READY,
        "resume_hash": resume_hash,
        "resume_fingerprint": resume_fingerprint,
        "upload_id": upload_id,
        "owner_id": owner_key,
        "matching_task_id": None,
        "trigger": trigger,
        "started_at": previous_state.get("started_at") or now,
        "updated_at": now,
        "warnings": [],
    }
    try:
        set_task_state(task_id, completed_state, ttl=3600)
    except Exception:
        logger.warning("Failed to write Redis completed state for task %s", task_id)

    matching_task_id = _enqueue_matching_for_ready_resume(
        owner_id=owner_id,
        upload_id=upload_id,
        resume_fingerprint=resume_fingerprint,
        trigger=trigger,
        tenant_id=tenant_id,
    )
    completed_state["matching_task_id"] = matching_task_id
    completed_state["warnings"] = (
        [] if matching_task_id else [{"code": "matching_enqueue_failed"}]
    )
    completed_state["updated_at"] = _utc_now_iso()
    try:
        set_task_state(task_id, completed_state, ttl=3600)
    except Exception:
        logger.warning("Failed to attach matching state for task %s", task_id)
    return matching_task_id


def _get_resume_upload_status(
    repo,
    owner_id,
    task_id: str,
    task_state: Optional[dict] = None,
) -> Optional[ResumeStatusResponse]:
    """Best-effort DB lookup for persisted upload state associated with a task."""
    try:
        upload = repo.get_resume_upload_by_task_id(owner_id, task_id)
        if upload is None:
            return None
        upload = _reconcile_resume_upload_task(repo, upload)
        return _resume_status_from_upload(
            task_id,
            upload,
            matching_task_id=(task_state or {}).get("matching_task_id"),
        )
    except HTTPException:
        raise
    except Exception:
        logger.warning(
            "Failed to load persisted resume upload state for task %s",
            _sanitize_log(task_id),
            exc_info=True,
        )
        return None


def _ensure_no_active_matching_task(redis, owner_id: str) -> None:
    """Raise 409 if a matching task is already pending/running."""
    if not redis:
        return

    try:
        active_id_raw = redis.get(_active_task_key(owner_id))
        if not active_id_raw:
            return

        active_id = _decode_redis_value(active_id_raw)
        state = get_task_state(active_id)
        if state and state.get("status") in ACTIVE_MATCHING_STATUSES:
            _raise_pipeline_error(
                status_code=409,
                code=PIPELINE_MATCH_ALREADY_RUNNING,
                message="Matching pipeline is already running.",
            )
    except PipelineApiError:
        raise
    except Exception:
        logger.warning("Failed to check active task state in Redis")


def _require_resume_eligibility_or_raise(owner_id):
    eligibility = evaluate_resume_eligibility(owner_id)
    if not eligibility.can_run:
        status_code = 409 if eligibility.processing_status in {
            "extracting",
            "extracted",
            "embedding",
        } else 400
        error_code = (
            PIPELINE_RESUME_UPLOAD_IN_PROGRESS
            if status_code == 409
            else PIPELINE_RESUME_NOT_READY
        )
        _raise_pipeline_error(
            status_code=status_code,
            code=error_code,
            message=eligibility.message,
        )
    return eligibility


def _set_initial_matching_task_state(
    task_id: str,
    upload_id: str,
    fingerprint: str,
    owner_id: str,
    *,
    trigger: str = "manual",
) -> None:
    """Write initial pending state for matching tasks."""
    now = _utc_now_iso()
    try:
        set_task_state(
            task_id,
            {
                "status": "pending",
                "step": "initializing",
                "phase": "initializing",
                "task_type": "matching",
                "upload_id": upload_id,
                "owner_id": owner_id,
                "resume_fingerprint": fingerprint,
                "trigger": trigger,
                "started_at": now,
                "updated_at": now,
                "stats": {},
            },
            ttl=3600,
        )
    except Exception:
        logger.warning("Failed to set initial Redis task state for %s", task_id)


def _claim_active_task_id(redis, owner_id: str, task_id: str) -> str:
    """Atomically claim the active matching slot or return the current winner."""
    if not redis:
        return task_id

    try:
        active_key = _active_task_key(owner_id)
        if redis.set(active_key, task_id, ex=3600, nx=True):
            return task_id

        active_task_id = _active_matching_task_id(redis, owner_id)
        if active_task_id:
            return active_task_id

        # Existing marker is stale or points at a terminal task; replace it.
        redis.set(active_key, task_id, ex=3600)
    except Exception:
        logger.warning("Failed to claim active_task_id in Redis for %s", task_id)
    return task_id


def _matching_task_marker_payload(
    *,
    task_id: str,
    upload_id: str,
    resume_fingerprint: str,
    trigger: str,
) -> str:
    """Serialize the latest matching marker written before queueing work."""
    return json.dumps(
        {
            "task_id": task_id,
            "upload_id": upload_id,
            "resume_fingerprint": resume_fingerprint,
            "trigger": trigger,
            "started_at": _utc_now_iso(),
        },
        sort_keys=True,
    )


def _store_latest_matching_task_marker(
    redis,
    owner_id: str,
    *,
    task_id: str,
    upload_id: str,
    resume_fingerprint: str,
    trigger: str,
) -> None:
    """Remember the newest matching task for an upload/fingerprint pair."""
    if not redis:
        return

    try:
        redis.set(
            _latest_matching_task_key(owner_id),
            _matching_task_marker_payload(
                task_id=task_id,
                upload_id=upload_id,
                resume_fingerprint=resume_fingerprint,
                trigger=trigger,
            ),
            ex=3600,
        )
    except Exception:
        logger.warning("Failed to store latest matching task marker for %s", task_id)


def _latest_matching_task_for_upload(
    redis,
    owner_id: str,
    *,
    upload_id: str,
    resume_fingerprint: str,
) -> Optional[str]:
    """Return any recent task already started for this upload/fingerprint."""
    if not redis:
        return None

    try:
        marker_raw = redis.get(_latest_matching_task_key(owner_id))
        if not marker_raw:
            return None
        marker = json.loads(_decode_redis_value(marker_raw))
        if (
            str(marker.get("upload_id")) == str(upload_id)
            and str(marker.get("resume_fingerprint")) == str(resume_fingerprint)
        ):
            task_id = marker.get("task_id")
            return str(task_id) if task_id else None
    except Exception:
        logger.warning("Failed to read latest matching task marker", exc_info=True)
    return None


def _clear_latest_matching_task_marker(redis, owner_id: str, task_id: str) -> None:
    """Clear a failed latest matching marker if it still points at task_id."""
    if not redis:
        return

    try:
        key = _latest_matching_task_key(owner_id)
        marker_raw = redis.get(key)
        if not marker_raw:
            return
        marker = json.loads(_decode_redis_value(marker_raw))
        if str(marker.get("task_id")) == str(task_id):
            redis.delete(key)
    except Exception:
        logger.warning("Failed to clear latest matching task marker for %s", task_id)


def _enqueue_matching_job_or_500(
    task_id: str,
    fingerprint: str,
    upload_id: str,
    owner_id: str,
    redis=None,
    *,
    trigger: str = "manual",
    tenant_id=None,
) -> None:
    """Enqueue matching work or raise 500 if enqueue fails."""
    try:
        enqueue_job(STREAM_MATCHING, {
            "task_id": task_id,
            "resume_fingerprint": fingerprint,
            "resume_upload_id": upload_id,
            "owner_id": owner_id,
            "tenant_id": str(tenant_id) if tenant_id is not None else None,
            "correlation_id": task_id,
            "trigger": trigger,
            "warn_on_no_completion_subscribers": False,
        })
    except Exception:
        logger.exception("Failed to enqueue matching job to stream")
        try:
            clear_task_cancellation_requested(task_id)
        except Exception:
            logger.warning("Failed to clear cancellation marker after enqueue failure")
        try:
            set_task_state(
                task_id,
                    {
                        "status": "failed",
                        "step": "initializing",
                        "task_type": "matching",
                        "owner_id": owner_id,
                        "upload_id": upload_id,
                        "resume_fingerprint": fingerprint,
                        "trigger": trigger,
                        "updated_at": _utc_now_iso(),
                        "warnings": [{"code": "matching_enqueue_failed"}],
                    },
                    ttl=3600,
                )
        except Exception:
            logger.warning("Failed to write failed state for unqueued task %s", task_id)
        if redis:
            try:
                active_key = _active_task_key(owner_id)
                active_value = redis.get(active_key)
                if active_value and _decode_redis_value(active_value) == task_id:
                    redis.delete(active_key)
            except Exception:
                logger.warning("Failed to clear active task marker after enqueue failure")
        _raise_pipeline_error(
            status_code=500,
            code=PIPELINE_MATCH_START_FAILED,
            message="Failed to start matching pipeline.",
        )


def _active_matching_task_id(redis, owner_id: str) -> Optional[str]:
    """Return an active matching task for this owner if one is still running."""
    if not redis:
        return None

    try:
        active_id_raw = redis.get(_active_task_key(owner_id))
        if not active_id_raw:
            return None
        active_id = _decode_redis_value(active_id_raw)
        state = get_task_state(active_id)
        if state and state.get("status") in ACTIVE_MATCHING_STATUSES:
            return active_id
    except Exception:
        logger.warning("Failed to load active matching task for owner", exc_info=True)
    return None


def _enqueue_matching_for_ready_resume(
    *,
    owner_id,
    upload_id: str,
    resume_fingerprint: str,
    trigger: str,
    raise_on_failure: bool = False,
    tenant_id=None,
) -> Optional[str]:
    """Start or reuse a matching task for a ready resume upload."""
    import uuid as _uuid

    owner_key = serialize_owner_id(owner_id)
    redis = _get_matching_redis_client()
    active_task_id = _active_matching_task_id(redis, owner_key)
    if active_task_id:
        return active_task_id

    if trigger != "manual":
        latest_task_id = _latest_matching_task_for_upload(
            redis,
            owner_key,
            upload_id=upload_id,
            resume_fingerprint=resume_fingerprint,
        )
        if latest_task_id:
            return latest_task_id

    try:
        consume_ephemeral_quota(owner_id, "matching_runs", default_limit=2, client=redis)
    except EphemeralQuotaExceeded as exc:
        if raise_on_failure:
            _raise_pipeline_error(
                status_code=429,
                code="public_testing.matching_quota_exceeded",
                message=str(exc),
            )
        logger.info("Automatic matching quota exhausted for owner %s", owner_key)
        return None
    except EphemeralQuotaUnavailable as exc:
        if raise_on_failure:
            _raise_pipeline_error(
                status_code=503,
                code="public_testing.quota_unavailable",
                message=str(exc),
            )
        logger.warning("Automatic matching quota backend unavailable for owner %s", owner_key)
        return None

    task_id = str(_uuid.uuid4())
    _set_initial_matching_task_state(
        task_id,
        upload_id,
        resume_fingerprint,
        owner_key,
        trigger=trigger,
    )
    claimed_task_id = _claim_active_task_id(redis, owner_key, task_id)
    if claimed_task_id != task_id:
        return claimed_task_id
    _store_latest_matching_task_marker(
        redis,
        owner_key,
        task_id=task_id,
        upload_id=upload_id,
        resume_fingerprint=resume_fingerprint,
        trigger=trigger,
    )
    try:
        _enqueue_matching_job_or_500(
            task_id,
            resume_fingerprint,
            upload_id,
            owner_key,
            redis,
            trigger=trigger,
            tenant_id=tenant_id,
        )
    except PipelineApiError:
        _clear_latest_matching_task_marker(redis, owner_key, task_id)
        if raise_on_failure:
            raise
        logger.warning(
            "Auto matching enqueue failed for upload_id=%s trigger=%s",
            _sanitize_log(upload_id),
            trigger,
            exc_info=True,
        )
        return None

    return task_id


def _start_matching(user, *, tenant_id=None) -> PipelineTaskResponse:
    """Enqueue a matching job to the Redis stream for the scorer-matcher consumer."""
    owner_id = resolve_owner_id(user)
    owner_key = serialize_owner_id(owner_id)
    redis = _get_matching_redis_client()
    _guard_resume_not_uploading(redis, owner_key)
    eligibility = _require_resume_eligibility_or_raise(owner_id)

    task_id = _enqueue_matching_for_ready_resume(
        owner_id=owner_id,
        upload_id=eligibility.upload_id,
        resume_fingerprint=eligibility.resume_fingerprint,
        trigger="manual",
        raise_on_failure=True,
        tenant_id=tenant_id,
    )
    if task_id is None:
        _raise_pipeline_error(
            status_code=500,
            code=PIPELINE_MATCH_START_FAILED,
            message="Failed to start matching pipeline.",
        )

    return PipelineTaskResponse(
        success=True,
        task_id=task_id,
        message="Matching pipeline started. Use SSE /api/pipeline/events/{task_id} to track progress.",
    )


def _start_imported_job_processing(user) -> PipelineTaskResponse:
    """Start extract/embed processing for imported jobs through the orchestrator."""
    owner_id = resolve_owner_id(user)
    owner_key = serialize_owner_id(owner_id)
    redis = get_redis_client()
    _ensure_no_active_matching_task(redis, owner_key)

    try:
        from web.backend.services.clients import orchestrator_client

        result = orchestrator_client.start_process_imported_jobs_pipeline()
    except Exception:
        logger.exception("Failed to start imported job processing pipeline")
        _raise_pipeline_error(
            status_code=500,
            code=PIPELINE_JOB_PROCESSING_START_FAILED,
            message="Failed to start imported job processing.",
        )

    task_id = str(result.get("task_id") or "").strip()
    if not task_id:
        _raise_pipeline_error(
            status_code=500,
            code=PIPELINE_JOB_PROCESSING_START_FAILED,
            message="Failed to start imported job processing.",
        )

    now = _utc_now_iso()
    try:
        redis.set(_active_task_key(owner_key), task_id, ex=3600)
        set_task_state(
            task_id,
            {
                "task_id": task_id,
                "status": result.get("status") or "pending",
                "step": result.get("current_stage") or "loading_resume",
                "task_type": "job_processing",
                "owner_id": owner_key,
                "result": result.get("result", {}) or {},
                "started_at": now,
                "updated_at": now,
            },
            ttl=3600,
        )
    except Exception:
        logger.warning("Failed to persist imported job processing task marker", exc_info=True)

    return PipelineTaskResponse(
        success=True,
        task_id=task_id,
        message="Imported job processing started. Extraction and embeddings will run in the background.",
    )


def _stop_matching(user) -> PipelineTaskResponse:
    """Cooperatively cancel the active matching task."""
    try:
        redis = get_redis_client()
        owner_id = serialize_owner_id(resolve_owner_id(user))
        active_id_raw = redis.get(_active_task_key(owner_id))
        if not active_id_raw:
            _raise_pipeline_error(
                status_code=404,
                code=PIPELINE_MATCH_STOP_NOT_FOUND,
                message="No active pipeline to stop.",
            )

        task_id = active_id_raw if isinstance(active_id_raw, str) else active_id_raw.decode()
        state = get_task_state(task_id)
        if not state or state.get("status") not in (
            "pending",
            "running",
            "cancellation_requested",
            "persisting",
        ):
            _raise_pipeline_error(
                status_code=404,
                code=PIPELINE_MATCH_STOP_NOT_FOUND,
                message="No active pipeline to stop.",
            )

        cancelled_state = {"status": "cancellation_requested"}
        normalized_step = _normalize_matching_step(state.get("step"), default="initializing")
        if normalized_step:
            cancelled_state["step"] = normalized_step
        if state.get("result"):
            cancelled_state["result"] = state.get("result")
        set_task_cancellation_requested(task_id, ttl=3600)
        set_task_state(task_id, cancelled_state, ttl=3600)

        return PipelineTaskResponse(
            success=True,
            task_id=task_id,
            message="Pipeline cancellation requested.",
        )
    except PipelineApiError:
        raise
    except Exception:
        logger.exception("Failed to stop pipeline")
        _raise_pipeline_error(
            status_code=500,
            code=PIPELINE_MATCH_STOP_FAILED,
            message=STOP_PIPELINE_ERROR,
        )


@router.get(
    "/status/{task_id}",
    response_model=PipelineStatusResponse,
    responses={
        404: {"model": ApiError, "description": "Task not found"},
        500: {"model": ApiError, "description": "Internal server error"},
    },
)
def get_pipeline_status(
    task_id: str,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """
    Get the status of a pipeline task.

    Status values:
    - pending: Task created but not yet started
    - running: Pipeline is currently executing
    - completed: Pipeline finished successfully
    - failed: Pipeline encountered an error
    """
    owner_id = resolve_owner_id(user)
    # Check Redis first — task state is written by the scorer-matcher consumer
    try:
        state = get_task_state(task_id)
    except Exception:
        state = None
    if state:
        try:
            _ensure_task_visible_to_owner(state, owner_id)
        except PipelineApiError as exc:
            return _pipeline_error_response(exc)
        return _build_pipeline_status_response(task_id, state)

    if _active_task_id_for_owner(owner_id) != task_id:
        return _pipeline_error_response(
            PipelineApiError(
                status_code=404,
                code=PIPELINE_TASK_NOT_FOUND,
                message=TASK_NOT_FOUND_DETAIL,
            )
        )

    # Proxy to orchestrator for tasks not yet reflected in Redis
    try:
        from web.backend.services.clients import orchestrator_client
        result = orchestrator_client.get_task_status(task_id)

        if not result.get("success"):
            _raise_pipeline_error(
                status_code=404,
                code=PIPELINE_TASK_NOT_FOUND,
                message=TASK_NOT_FOUND_DETAIL,
            )

        return _build_pipeline_status_response(
            task_id,
            {
                "status": result.get("status", "unknown"),
                "step": result.get("current_stage"),
                "task_type": "matching",
                "owner_id": serialize_owner_id(owner_id),
                "result": result.get("result", {}) or {},
                "updated_at": _utc_now_iso(),
            },
        )
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)
    except Exception:
        logger.exception("Failed to get pipeline status")
        return _pipeline_error_response(
            PipelineApiError(
                status_code=500,
                code=PIPELINE_STATUS_LOOKUP_FAILED,
                message="Failed to get pipeline status.",
            )
        )


@router.get("/active", response_model=Optional[PipelineStatusResponse])
def get_active_pipeline_task(user: Annotated[None, Depends(get_current_user)] = None):
    """
    Get the currently running pipeline task, if any.

    Useful for frontend recovery on page refresh.
    """
    return _get_active_task(user)


def _get_active_task(user) -> Optional[PipelineStatusResponse]:
    """Return the active matching task from Redis, or None if nothing is running."""
    try:
        owner_key = serialize_owner_id(resolve_owner_id(user))
        redis = get_redis_client()
        task_id_raw = redis.get(_active_task_key(owner_key))
        if not task_id_raw:
            return None
        task_id = task_id_raw if isinstance(task_id_raw, str) else task_id_raw.decode()
        state = get_task_state(task_id)
        if not state or state.get("status") not in ACTIVE_MATCHING_STATUSES:
            return None
        return _build_pipeline_status_response(task_id, state)
    except Exception:
        return None


@router.post(
    "/stop",
    response_model=PipelineTaskResponse,
    responses={
        404: {"model": ApiError, "description": "No active pipeline to stop"},
        500: {"model": ApiError, "description": "Internal server error"},
    }
)
def stop_matching_pipeline(user: Annotated[None, Depends(get_current_user)] = None):
    """
    Stop the currently running pipeline task.

    Raises:
        404: No active pipeline is running.
        500: Internal error stopping the pipeline.
    """
    try:
        return _stop_matching(user)
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)

@router.get("/resume-eligibility", response_model=ResumeEligibilityResponse)
def get_resume_eligibility(user: Annotated[None, Depends(get_current_user)] = None):
    """Return the authoritative eligibility of the latest uploaded resume."""
    return _build_resume_eligibility_response(resolve_owner_id(user))


@router.post("/resume-preflight", response_model=ResumePreflightResponse)
def resume_preflight(
    body: ResumePreflightRequest,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """Read-only check for whether a locally cached resume needs upload bytes."""
    return _build_resume_preflight_response(resolve_owner_id(user), body.resume_hash)


@router.post(
    "/select-resume",
    response_model=ResumeUploadResponse,
    responses={409: {"model": ApiError, "description": "Resume is not ready to select yet."}},
)
def select_ready_resume(
    body: ResumeSelectRequest,
    request: Request,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """Commit a new latest-upload intent for an already-ready resume hash."""
    try:
        owner_id = resolve_owner_id(user)
        owner_key = serialize_owner_id(owner_id)
        resume_fingerprint = build_resume_fingerprint(owner_id, body.resume_hash)
        upload_id: Optional[str] = None

        with job_uow() as repo:
            if not repo.is_resume_ready(resume_fingerprint):
                _raise_pipeline_error(
                    status_code=409,
                    code=PIPELINE_RESUME_NOT_READY,
                    message="Resume is not ready to select yet.",
                )

            upload = repo.create_resume_upload(
                ResumeUploadCreateParams(
                    owner_id=owner_id,
                    resume_hash=body.resume_hash,
                    resume_fingerprint=resume_fingerprint,
                    original_filename=body.original_filename,
                    status=RESUME_UPLOAD_READY,
                    user_safe_message="Resume selected and ready for matching.",
                )
            )
            upload_id = str(upload.id)

        _clear_latest_upload_task_marker(owner_key)
        matching_task_id = _enqueue_matching_for_ready_resume(
            owner_id=owner_id,
            upload_id=upload_id,
            resume_fingerprint=resume_fingerprint,
            trigger="resume_selected",
            tenant_id=getattr(request.state, "tenant_id", None),
        )
        phase = _resume_phase_from_step("completed", None)

        return ResumeUploadResponse(
            success=True,
            resume_hash=body.resume_hash,
            upload_id=upload_id,
            message="Resume selected and ready for matching.",
            matching_task_id=matching_task_id,
            status=RESUME_UPLOAD_READY,
            phase=phase,
            progress=_progress_for_phase(
                phase,
                status="completed",
                phases=RESUME_PHASES,
            ),
            warnings=(
                []
                if matching_task_id
                else [_safe_warning("matching_enqueue_failed")]
            ),
        )
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)


@router.post(
    "/retry-resume",
    response_model=ResumeUploadResponse,
    responses={
        404: {"model": ApiError, "description": "Resume upload not found."},
        409: {"model": ApiError, "description": "Resume upload cannot be retried in its current state."},
    },
)
async def retry_resume(
    body: ResumeRetryRequest,
    request: Request,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """Retry a failed upload attempt by explicit upload_id."""
    import uuid as _uuid

    owner_id = resolve_owner_id(user)
    owner_key = serialize_owner_id(owner_id)
    retry_upload_id: Optional[str] = None
    source_resume_hash: Optional[str] = None
    source_resume_fingerprint: Optional[str] = None
    try:
        with job_uow() as repo:
            source_upload = repo.get_resume_upload(body.upload_id, owner_id)
            if source_upload is None:
                _raise_pipeline_error(
                    status_code=404,
                    code=PIPELINE_RESUME_UPLOAD_NOT_FOUND,
                    message="Resume upload not found.",
                )
            if source_upload.status != RESUME_UPLOAD_FAILED_RETRYABLE:
                _raise_pipeline_error(
                    status_code=409,
                    code=PIPELINE_RESUME_UPLOAD_NOT_RETRYABLE,
                    message="Resume upload is not retryable.",
                )
            if repo.get_structured_resume_by_fingerprint(source_upload.resume_fingerprint) is None:
                _raise_pipeline_error(
                    status_code=409,
                    code=PIPELINE_RESUME_REUPLOAD_REQUIRED,
                    message="Retry requires re-upload because extracted artifacts are missing.",
                )

            source_resume_hash = source_upload.resume_hash
            source_resume_fingerprint = source_upload.resume_fingerprint
            task_id = str(_uuid.uuid4())
            retry_upload = repo.create_resume_upload(
                ResumeUploadCreateParams(
                    owner_id=owner_id,
                    resume_hash=source_resume_hash,
                    resume_fingerprint=source_resume_fingerprint,
                    original_filename=source_upload.original_filename,
                    status=RESUME_UPLOAD_PENDING,
                    processing_task_id=task_id,
                    retry_of_upload_id=source_upload.id,
                    retryable=True,
                )
            )
            retry_upload_id = str(retry_upload.id)
            repo.update_resume_upload(
                retry_upload.id,
                status=RESUME_UPLOAD_IN_PROGRESS,
                processing_task_id=task_id,
                last_error=None,
                failure_stage=None,
                failure_class=None,
                retryable=True,
                user_safe_message=None,
                failure_debug_context=None,
            )
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)

    _bind_public_operation_lease(request, task_id)
    try:
        redis = get_redis_client()
        redis.set(_latest_upload_task_key(owner_key), task_id, ex=3600)
        now = _utc_now_iso()
        set_task_state(
            task_id,
            {
                "status": "pending",
                "step": "embedding",
                "phase": "embedding_resume",
                "task_type": "resume_upload",
                "upload_id": retry_upload_id,
                "owner_id": owner_key,
                "resume_fingerprint": source_resume_fingerprint,
                "started_at": now,
                "updated_at": now,
                "stats": {},
            },
            ttl=3600,
        )
    except Exception:
        logger.warning("Failed to advertise retry upload task %s in Redis", task_id)

    orchestrator_task = asyncio.create_task(
        asyncio.to_thread(
            _retry_resume_background,
            task_id,
            retry_upload_id,
            owner_id,
            source_resume_fingerprint,
            source_resume_hash,
            getattr(request.state, "tenant_id", None),
        )
    )
    _upload_tasks.add(orchestrator_task)
    orchestrator_task.add_done_callback(lambda t: _upload_tasks.discard(t))

    return ResumeUploadResponse(
        success=True,
        resume_hash=source_resume_hash,
        upload_id=retry_upload_id,
        task_id=task_id,
        message="Retry started for the latest resume upload.",
        status=RESUME_UPLOAD_IN_PROGRESS,
        phase="embedding_resume",
        progress=_progress_for_phase(
            "embedding_resume",
            status="processing",
            phases=RESUME_PHASES,
        ),
    )

async def _stream_orchestrator_sse(orchestrator_url: str, task_id: str):
    """Async generator that proxies SSE bytes from the orchestrator."""
    import httpx

    # Validate task_id is a well-formed UUID to prevent path injection (CWE-918)
    try:
        safe_task_id = str(uuid.UUID(task_id))
    except ValueError:
        logger.error("Invalid task_id format: not a valid UUID")
        return
    encoded_task_id = quote(safe_task_id, safe='')

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0)) as client:
            async with client.stream(
                "GET",
                f"{orchestrator_url}/orchestrate/status/{encoded_task_id}"
            ) as response:
                if response.status_code == 404:
                    logger.error("Orchestrator: task not found")
                    yield f"data: {json.dumps({'error': TASK_NOT_FOUND_DETAIL, 'status': 'failed'})}\n\n"
                    return
                if response.is_error:
                    logger.error("Orchestrator returned %s", response.status_code)
                    yield f"data: {json.dumps({'error': 'Failed to get pipeline status'})}\n\n"
                    return
                async for chunk in response.aiter_raw():
                    if chunk:
                        yield chunk
    except Exception:
        logger.exception("Failed to connect to orchestrator")
        yield f"data: {json.dumps({'error': 'Failed to connect to pipeline service'})}\n\n"


async def _preflight_task_check(orchestrator_url: str, _task_id: str) -> None:
    """Quick probe to verify task existence before opening a long-lived SSE stream.

    Raises HTTPException(404) only if the orchestrator definitively reports
    the task does not exist. Failures during the probe are logged and ignored
    so the SSE stream can surface errors itself.

    Note: _task_id is reserved for future use when the orchestrator supports
    task-specific status endpoints.
    """
    import httpx

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as probe:
            probe_resp = await probe.get(f"{orchestrator_url}/orchestrate/active")
            if probe_resp.status_code == 404:
                logger.warning("Task not found")
                raise HTTPException(status_code=404, detail=TASK_NOT_FOUND_DETAIL)
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Pre-flight check failed: %s", type(e).__name__)


async def _stream_local_task_sse(task_id: str):
    """Poll Redis task state and emit SSE events for a running task."""
    timeout = 600.0
    poll_interval = 1.5
    reconnect_after_seconds = 5
    elapsed = 0.0

    while elapsed < timeout:
        try:
            state = await asyncio.to_thread(get_task_state, task_id)
        except Exception:
            state = None

        if state is None:
            yield f"data: {json.dumps({'task_id': task_id, 'status': 'failed', 'error': 'Task not found'})}\n\n"
            return

        # Flatten result sub-dict to match PipelineStatusResponse shape expected by the frontend
        response = _build_pipeline_status_response(task_id, state)
        event_data = response.model_dump(exclude_none=True)

        yield f"data: {json.dumps(event_data)}\n\n"

        if state.get("status") in ("completed", "failed", "cancelled"):
            return

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    observer_timeout_payload = {
        "task_id": task_id,
        "status": "observer_timeout",
        "observer_timeout": True,
        "reconnect_after_seconds": reconnect_after_seconds,
        "error": None,
    }
    yield f"data: {json.dumps(observer_timeout_payload)}\n\n"


@router.get(
    "/events/{task_id}",
    responses={
        404: {"description": TASK_NOT_FOUND_DETAIL},
        400: {"description": "Invalid task_id format"},
    }
)
async def pipeline_events(
    task_id: str,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """
    Server-Sent Events endpoint for real-time pipeline status updates.

    Checks Redis first (task state written by scorer-matcher consumer), then
    falls back to proxying the orchestrator's SSE stream for tasks not yet
    reflected in Redis.

    Raises:
        404: Task does not exist.
        400: Invalid task_id format.
    """
    # Validate task_id BEFORE any use (OWASP: Validate Early)
    if not _validate_task_id(task_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid task_id format. Must be alphanumeric with hyphens, max 50 characters."
        )

    # Check Redis first — task state is written by the scorer-matcher consumer
    try:
        state = await asyncio.to_thread(get_task_state, task_id)
    except Exception:
        state = None

    if state is not None:
        owner_id = resolve_owner_id(user)
        try:
            _ensure_task_visible_to_owner(state, owner_id)
        except PipelineApiError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
        return StreamingResponse(
            _stream_local_task_sse(task_id),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    # Fall back to orchestrator SSE for tasks not managed via Redis
    orchestrator_url = resolve_service_url(
        INTERNAL_ORCHESTRATOR_URL_ENV,
        ORCHESTRATOR_URL_ENV,
    )
    if not orchestrator_url:
        raise HTTPException(status_code=404, detail=TASK_NOT_FOUND_DETAIL)

    owner_id = resolve_owner_id(user)
    if _active_task_id_for_owner(owner_id) != task_id:
        raise HTTPException(status_code=404, detail=TASK_NOT_FOUND_DETAIL)

    await _preflight_task_check(orchestrator_url, task_id)

    return StreamingResponse(
        _stream_orchestrator_sse(orchestrator_url, task_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


@router.get(
    "/resume-status/{task_id}",
    response_model=ResumeStatusResponse,
    responses={
        400: {"model": ApiError, "description": "Invalid task_id format"},
        404: {"model": ApiError, "description": "Task not found"},
    }
)
def get_resume_status(
    task_id: str,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """
    Poll the status of a background resume processing task.

    Status values:
    - processing: Resume ETL is currently running
    - completed: Resume was extracted and embedded successfully
    - failed: Resume processing encountered an error
    """
    try:
        if not _validate_task_id(task_id):
            _raise_pipeline_error(
                status_code=400,
                code=PIPELINE_TASK_INVALID_ID,
                message="Invalid task_id format. Must be alphanumeric with hyphens, max 50 characters.",
            )
        owner_id = resolve_owner_id(user)
        state: Optional[dict] = None
        try:
            state = _get_owned_resume_task_state(task_id, owner_id)
        except PipelineApiError as exc:
            if exc.status_code != 404:
                raise

        with job_uow() as repo:
            upload_status = _get_resume_upload_status(repo, owner_id, task_id, state)
            if upload_status is not None:
                return upload_status
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)
    except Exception:
        logger.warning(
            "Resume status DB lookup unavailable; falling back to task state only",
            exc_info=True,
        )

    if state is None:
        return _pipeline_error_response(
            PipelineApiError(
                status_code=404,
                code=PIPELINE_TASK_NOT_FOUND,
                message=TASK_NOT_FOUND_OR_EXPIRED_DETAIL,
            )
        )

    return _resume_status_from_task_state(task_id, state)


@router.post("/check-resume-hash", response_model=ResumeHashCheckResponse)
@limiter.limit("10/minute")
def check_resume_hash_endpoint(request: Request, body: ResumeHashCheckRequest, user: Annotated[None, Depends(get_current_user)] = None):
    """
    Check if a resume with the given hash already exists in the database.

    Used for deduplication - if the hash exists, the frontend can skip
    uploading the same file again. The frontend stores the file in IndexedDB.
    """
    preflight = evaluate_resume_preflight(resolve_owner_id(user), body.resume_hash)
    exists = preflight.status != "upload_required"

    return ResumeHashCheckResponse(
        exists=exists,
        resume_hash=body.resume_hash
    )


@router.post(
    "/upload-resume",
    response_model=ResumeUploadResponse,
    responses={
        400: {"model": ApiError, "description": "Invalid file, empty file, or hash mismatch"},
        413: {"model": ApiError, "description": "File size exceeds 2MB limit"},
        415: {"model": ApiError, "description": "Unsupported file format"},
        429: {"description": "Rate limit exceeded"},
    }
)
@limiter.limit("5/minute")
async def upload_resume_endpoint(
    request: Request,
    file: Annotated[UploadFile, File(...)],
    resume_hash: Annotated[Optional[str], Form()] = None,
    user: Annotated[None, Depends(get_current_user)] = None,
):
    """
    Upload a resume file.
    Supports: .json, .yaml, .yml, .txt, .docx, .pdf

    If resume_hash is provided, checks if it already exists first.
    If the hash exists, returns success without re-processing.

    The file is written to a temporary file for ETL processing, then cleaned up.
    Returns only the hash for frontend verification and IndexedDB storage.
    """
    from database.uow import job_uow

    try:
        content = await _validate_resume_file(file)

        resume_hash = _compute_and_verify_hash(content, resume_hash)
        owner_id = resolve_owner_id(user)
        tenant_id = getattr(request.state, "tenant_id", None)
        owner_key = serialize_owner_id(owner_id)
        resume_fingerprint = build_resume_fingerprint(owner_id, resume_hash)

        task_id: Optional[str] = None
        upload_id: Optional[str] = None
        with job_uow() as repo:
            latest_same_upload = repo.get_latest_resume_upload_for_hash(owner_id, resume_hash)
            if latest_same_upload is not None:
                latest_same_upload = _reconcile_resume_upload_task(repo, latest_same_upload)
            if repo.is_resume_ready(resume_fingerprint):
                upload = repo.create_resume_upload(
                    ResumeUploadCreateParams(
                        owner_id=owner_id,
                        resume_hash=resume_hash,
                        resume_fingerprint=resume_fingerprint,
                        original_filename=file.filename,
                        processing_task_id=None,
                        status=RESUME_UPLOAD_READY,
                        user_safe_message="Resume already processed and ready for matching.",
                    )
                )
                matching_task_id = _enqueue_matching_for_ready_resume(
                    owner_id=owner_id,
                    upload_id=str(upload.id),
                    resume_fingerprint=resume_fingerprint,
                    trigger="resume_already_ready",
                    tenant_id=tenant_id,
                )
                _bind_public_operation_lease(request, matching_task_id)
                phase = _resume_phase_from_step("completed", None)
                return ResumeUploadResponse(
                    success=True,
                    resume_hash=resume_hash,
                    upload_id=str(upload.id),
                    message="Resume already processed and ready for matching.",
                    task_id=None,
                    matching_task_id=matching_task_id,
                    status=RESUME_UPLOAD_READY,
                    phase=phase,
                    progress=_progress_for_phase(
                        phase,
                        status="completed",
                        phases=RESUME_PHASES,
                    ),
                    warnings=(
                        []
                        if matching_task_id
                        else [_safe_warning("matching_enqueue_failed")]
                    ),
                )

            if latest_same_upload and latest_same_upload.status in {
                RESUME_UPLOAD_PENDING,
                RESUME_UPLOAD_IN_PROGRESS,
            }:
                task_id = getattr(latest_same_upload, "processing_task_id", None)
                if not isinstance(task_id, str):
                    task_id = None
                _bind_public_operation_lease(request, task_id)
                return ResumeUploadResponse(
                    success=True,
                    resume_hash=resume_hash,
                    upload_id=str(latest_same_upload.id),
                    message="Resume is already being processed.",
                    task_id=task_id,
                    status=RESUME_UPLOAD_IN_PROGRESS,
                    phase="extracting_resume",
                    progress=_progress_for_phase(
                        "extracting_resume",
                        status="processing",
                        phases=RESUME_PHASES,
                    ),
                )

            try:
                consume_ephemeral_quota(owner_id, "resume_uploads", default_limit=3)
            except EphemeralQuotaExceeded as exc:
                _raise_pipeline_error(
                    status_code=429,
                    code="public_testing.resume_upload_quota_exceeded",
                    message=str(exc),
                )
            except EphemeralQuotaUnavailable as exc:
                _raise_pipeline_error(
                    status_code=503,
                    code="public_testing.quota_unavailable",
                    message=str(exc),
                )

            import uuid as _uuid
            task_id = str(_uuid.uuid4())
            upload = repo.create_resume_upload(
                ResumeUploadCreateParams(
                    owner_id=owner_id,
                    resume_hash=resume_hash,
                    resume_fingerprint=resume_fingerprint,
                    original_filename=file.filename,
                    status=RESUME_UPLOAD_PENDING,
                    processing_task_id=task_id,
                )
            )
            repo.update_resume_upload(
                upload.id,
                status=RESUME_UPLOAD_IN_PROGRESS,
                processing_task_id=task_id,
                last_error=None,
                failure_stage=None,
                failure_class=None,
                retryable=True,
                user_safe_message=None,
                failure_debug_context=None,
            )
            upload_id = str(upload.id)

        _bind_public_operation_lease(request, task_id)
        try:
            redis = get_redis_client()
            redis.set(_latest_upload_task_key(owner_key), task_id, ex=3600)
            now = _utc_now_iso()
            set_task_state(
                task_id,
                {
                    "status": "pending",
                    "step": "extracting",
                    "phase": "extracting_resume",
                    "task_type": "resume_upload",
                    "upload_id": upload_id,
                    "owner_id": owner_key,
                    "resume_fingerprint": resume_fingerprint,
                    "started_at": now,
                    "updated_at": now,
                    "stats": {},
                },
                ttl=3600,
            )
        except Exception:
            logger.warning("Failed to set resume:upload:latest_task_id in Redis — guard will not work")

        background_task = asyncio.create_task(
            asyncio.to_thread(
                _process_resume_background,
                content,
                file.filename,
                task_id,
                upload_id,
                owner_id,
                resume_hash,
                resume_fingerprint,
                tenant_id,
            )
        )
        _upload_tasks.add(background_task)

        def _upload_done(t: asyncio.Task) -> None:
            _upload_tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.error("Upload background task raised unhandled: %s", t.exception())

        background_task.add_done_callback(_upload_done)

        return ResumeUploadResponse(
            success=True,
            resume_hash=resume_hash,
            upload_id=upload_id,
            message="Resume uploaded. Processing in background...",
            task_id=task_id,
            status=RESUME_UPLOAD_IN_PROGRESS,
            phase="extracting_resume",
            progress=_progress_for_phase(
                "extracting_resume",
                status="processing",
                phases=RESUME_PHASES,
            ),
        )
    except PipelineApiError as exc:
        return _pipeline_error_response(exc)


async def _validate_resume_file(file: UploadFile) -> bytes:
    """Validate resume file and return its content."""
    if not file.filename:
        _raise_pipeline_error(
            status_code=400,
            code=PIPELINE_RESUME_FILE_REQUIRED,
            message="No file provided.",
        )

    # Validate file format — 415 Unsupported Media Type
    parser = ResumeParser()
    if not parser.is_supported(file.filename):
        supported = ', '.join(ResumeParser.get_supported_formats())
        _raise_pipeline_error(
            status_code=415,
            code=PIPELINE_RESUME_FILE_UNSUPPORTED,
            message=f"Unsupported file format. Supported formats: {supported}",
        )

    chunks: list[bytes] = []
    total_bytes = 0
    while total_bytes <= RESUME_MAX_SIZE:
        chunk = await file.read(min(64 * 1024, RESUME_MAX_SIZE + 1 - total_bytes))
        if not chunk:
            break
        chunks.append(chunk)
        total_bytes += len(chunk)
    content = b"".join(chunks)

    if len(content) == 0:
        _raise_pipeline_error(
            status_code=400,
            code=PIPELINE_RESUME_FILE_EMPTY,
            message="Empty file.",
        )

    # File too large — 413 Payload Too Large
    if total_bytes > RESUME_MAX_SIZE:
        limit_mb = RESUME_MAX_SIZE / (1024 * 1024)
        limit_str = f"{limit_mb:.1f}MB" if limit_mb >= 1 else f"{RESUME_MAX_SIZE // 1024}KB"
        _raise_pipeline_error(
            status_code=413,
            code=PIPELINE_RESUME_FILE_TOO_LARGE,
            message=f"File size exceeds {limit_str} limit.",
        )

    try:
        validate_resume_content(file.filename, content)
    except ResumeFileSafetyError as exc:
        _raise_pipeline_error(
            status_code=exc.status_code,
            code=(
                PIPELINE_RESUME_FILE_TOO_LARGE
                if exc.status_code == 413
                else PIPELINE_RESUME_FILE_UNSUPPORTED
            ),
            message=str(exc),
        )

    return content


def _normalized_hash(value: Optional[str]) -> Optional[str]:
    """Return a lowercase client hash, or None when omitted/blank."""
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped.lower()


def _hash_equals(left: str, right: str) -> bool:
    """Compare same-algorithm hashes without timing-shortcut surprises."""
    return len(left) == len(right) and hmac.compare_digest(left, right)


def _compute_and_verify_hash(content: bytes, provided_hash: Optional[str]) -> str:
    """Compute and verify resume hashes accepted by current and legacy clients.

    Hosted browsers now send a CSP-safe SHA-256 hex digest because the former
    xxhash-wasm client path violated the production CSP. Older clients may still
    send the historical xxh64 digest, and server-only callers may omit a hash.
    Store the digest the caller actually proved when present; otherwise preserve
    the legacy server-side fingerprint for backward compatibility.
    """
    from database.models.resume import generate_file_fingerprint

    legacy_hash = generate_file_fingerprint(content).lower()
    sha256_hash = hashlib.sha256(content).hexdigest()
    normalized_provided = _normalized_hash(provided_hash)

    if normalized_provided is None:
        return legacy_hash

    if SHA256_HEX_PATTERN.fullmatch(normalized_provided) and _hash_equals(normalized_provided, sha256_hash):
        return normalized_provided

    if XXH64_HEX_PATTERN.fullmatch(normalized_provided) and _hash_equals(normalized_provided, legacy_hash):
        return normalized_provided

    logger.debug(
        "Hash mismatch - provided: %s, sha256: %s, legacy_xxh64: %s",
        _sanitize_log(normalized_provided),
        _sanitize_log(sha256_hash),
        _sanitize_log(legacy_hash),
    )
    _raise_pipeline_error(
        status_code=400,
        code=PIPELINE_RESUME_HASH_MISMATCH,
        message="File hash mismatch. The provided hash does not match the file content.",
    )
    raise AssertionError("unreachable after resume hash mismatch")


def _process_resume_background(
    file_content: bytes,
    filename: str,
    task_id: str,
    upload_id: str,
    owner_id,
    resume_hash: str,
    resume_fingerprint: str,
    tenant_id=None,
) -> None:
    """Run ETL processing in background thread with status updates.

    Args:
        file_content: Raw file bytes
        filename: Original filename
        task_id: Task identifier
        upload_id: Upload attempt identifier
        owner_id: User-scoped ownership UUID
        resume_hash: Raw file hash from the browser
        resume_fingerprint: Owner-scoped canonical fingerprint
    """
    import time as _time
    from web.backend.services.clients import orchestrator_client

    try:
        now = _utc_now_iso()
        set_task_state(
            task_id,
            {
                "status": "processing",
                "step": "extracting",
                "phase": "extracting_resume",
                "task_type": "resume_upload",
                "upload_id": upload_id,
                "owner_id": serialize_owner_id(owner_id),
                "resume_fingerprint": resume_fingerprint,
                "started_at": now,
                "updated_at": now,
                "stats": {},
            },
            ttl=3600,
        )
    except Exception:
        logger.warning("Failed to write Redis processing state for task %s", task_id)

    tmp_path: Optional[str] = None

    try:
        tmp_path = _write_resume_file_to_shared_volume(file_content, filename, task_id)
        orchestrator_client.process_resume(
            tmp_path,
            task_id,
            upload_id=upload_id,
            owner_id=str(owner_id),
            tenant_id=str(tenant_id) if tenant_id is not None else None,
            resume_fingerprint=resume_fingerprint,
            mode="extract_and_embed",
        )
        final_state = _wait_for_resume_etl_final_state(task_id, _time)
        if final_state.get("status") == "failed":
            raise RuntimeError(final_state.get("error") or "Resume ETL failed")
        with job_uow() as repo:
            repo.update_resume_upload(
                upload_id,
                status=RESUME_UPLOAD_READY,
                last_error=None,
                processing_task_id=task_id,
                retryable=False,
                user_safe_message=RESUME_PROCESSING_COMPLETED_MESSAGE,
            )
        _write_resume_ready_state(
            task_id=task_id,
            upload_id=upload_id,
            owner_id=owner_id,
            resume_hash=resume_hash,
            resume_fingerprint=resume_fingerprint,
            trigger="resume_ready",
            tenant_id=tenant_id,
        )
    except Exception as exc:
        logger.exception("Background resume processing failed")
        _write_resume_failure_state(
            task_id,
            upload_id,
            resume_hash,
            resume_fingerprint,
            serialize_owner_id(owner_id),
            exc,
        )
    finally:
        if tmp_path:
            _remove_temporary_resume_file(tmp_path)


def _retry_resume_background(
    task_id: str,
    upload_id: str,
    owner_id,
    resume_fingerprint: str,
    resume_hash: str,
    tenant_id=None,
) -> None:
    import time as _time
    from web.backend.services.clients import orchestrator_client

    try:
        now = _utc_now_iso()
        set_task_state(
            task_id,
            {
                "status": "processing",
                "step": "embedding",
                "phase": "embedding_resume",
                "task_type": "resume_upload",
                "upload_id": upload_id,
                "owner_id": serialize_owner_id(owner_id),
                "resume_fingerprint": resume_fingerprint,
                "started_at": now,
                "updated_at": now,
                "stats": {},
            },
            ttl=3600,
        )
    except Exception:
        logger.warning("Failed to write retry processing state for task %s", task_id)

    try:
        orchestrator_client.process_resume(
            None,
            task_id,
            upload_id=upload_id,
            owner_id=str(owner_id),
            tenant_id=str(tenant_id) if tenant_id is not None else None,
            resume_fingerprint=resume_fingerprint,
            mode="embed_only",
        )
        final_state = _wait_for_resume_etl_final_state(task_id, _time)
        if final_state.get("status") == "failed":
            raise RuntimeError(final_state.get("error") or "Resume retry failed")
        with job_uow() as repo:
            repo.update_resume_upload(
                upload_id,
                status=RESUME_UPLOAD_READY,
                last_error=None,
                processing_task_id=task_id,
                retryable=False,
                user_safe_message=RESUME_PROCESSING_COMPLETED_MESSAGE,
            )
        _write_resume_ready_state(
            task_id=task_id,
            upload_id=upload_id,
            owner_id=owner_id,
            resume_hash=resume_hash,
            resume_fingerprint=resume_fingerprint,
            trigger="resume_retry_ready",
            tenant_id=tenant_id,
        )
    except Exception as exc:
        logger.exception("Background resume retry failed")
        _write_resume_failure_state(
            task_id,
            upload_id,
            resume_hash,
            resume_fingerprint,
            serialize_owner_id(owner_id),
            exc,
        )


def _write_resume_file_to_shared_volume(file_content: bytes, filename: str, task_id: str) -> str:
    """Write uploaded resume to shared volume for orchestrator processing."""
    shared_dir = Path("/data/resume_uploads")
    shared_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = shared_dir / f"{task_id}{Path(filename).suffix}"
    tmp_path.write_bytes(file_content)
    return str(tmp_path)


def _wait_for_resume_etl_final_state(task_id: str, time_module) -> dict:
    """Poll Redis for resume ETL completion and return the final state."""
    poll_timeout_seconds = RESUME_ETL_WAIT_TIMEOUT_SECONDS
    deadline = time_module.time() + poll_timeout_seconds

    while time_module.time() < deadline:
        state = get_task_state(task_id)
        if state and state.get("status") in ("completed", "failed"):
            return state
        time_module.sleep(1)

    raise RuntimeError(
        f"Resume ETL timed out after {poll_timeout_seconds}s waiting for orchestrator"
    )


def _write_resume_failure_state(
    task_id: str,
    upload_id: str,
    resume_hash: str,
    resume_fingerprint: str,
    owner_id: str,
    error: Exception,
) -> None:
    """Persist failed state for resume ETL tasks and upload attempts."""
    retryable = False
    with job_uow() as repo:
        upload_status, upload_error, retryable = _classify_failed_resume_upload(repo, resume_fingerprint)
        repo.update_resume_upload(
            upload_id,
            status=upload_status,
            last_error=upload_error,
            processing_task_id=task_id,
            failure_stage="resume_etl",
            failure_class="processing_failed",
            retryable=retryable,
            user_safe_message=upload_error,
            failure_debug_context={"exception_type": type(error).__name__},
        )
    try:
        now = _utc_now_iso()
        set_task_state(
            task_id,
            {
                "status": "failed",
                "task_type": "resume_upload",
                "step": "embedding" if retryable else "extracting",
                "error": upload_error,
                "upload_status": upload_status,
                "resume_hash": resume_hash,
                "resume_fingerprint": resume_fingerprint,
                "upload_id": upload_id,
                "owner_id": owner_id,
                "updated_at": now,
            },
            ttl=3600,
        )
    except Exception:
        logger.warning("Failed to write Redis failed state for task %s", task_id)


def _remove_temporary_resume_file(tmp_path: str) -> None:
    """Best-effort cleanup of temporary resume file."""
    try:
        os.unlink(tmp_path)
    except Exception:
        pass  # best-effort cleanup; ignore missing file or permission errors
