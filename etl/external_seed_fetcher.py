from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from database.uow import job_uow
from etl.import_models import NormalizedJobRecord
from etl.orchestrator import JobETLService

logger = logging.getLogger(__name__)

PROVIDER_NAME = "cloudflare_worker_seed"
INGEST_MODE = "external_seed_fetch"
SCHEMA_VERSION = 1
REQUEST_BODY_MAX_BYTES = 2 * 1024
UPSTREAM_BODY_MAX_BYTES = 2 * 1024 * 1024
RESPONSE_BODY_MAX_BYTES = 256 * 1024
JOB_DESCRIPTION_MAX_CHARS = 4_000
MAX_JOBS_PER_SOURCE = 25
MAX_GLOBAL_CALLS_PER_DAY = 250
DEFAULT_GLOBAL_CALLS_PER_DAY = 100
DEFAULT_SOURCE_CALLS_PER_DAY = 50
DEFAULT_MIN_INTERVAL_MINUTES = 240
DEFAULT_MAX_JOB_AGE_DAYS = 45
DEFAULT_TIMEOUT_SECONDS = 15.0
RUNTIME_MAX_CALLS_PER_DAY = 100
RUNTIME_MAX_CALLS_PER_SOURCE_PER_DAY = 50
RUNTIME_MIN_INTERVAL_MINUTES = 240
RUNTIME_MAX_TIMEOUT_SECONDS = 15.0
DAILY_WINDOW_SECONDS = 24 * 60 * 60
STATUS_TTL_SECONDS = 14 * DAILY_WINDOW_SECONDS
ALLOWED_SOURCES = frozenset({"tokyodev", "japandev"})
SOURCE_URLS = {
    "tokyodev": "https://www.tokyodev.com/jobs",
    "japandev": "https://japan-dev.com/jobs",
}


class ExternalSeedFetchError(RuntimeError):
    """Raised when an external seed fetch cannot proceed safely."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        failure_class: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.failure_class = failure_class or code


@dataclass(frozen=True)
class ExternalSeedFetcherConfig:
    enabled: bool
    worker_url: str | None
    secret: str | None
    previous_secret: str | None
    sources: tuple[str, ...]
    max_jobs_per_source: int
    timeout_seconds: float
    min_interval_minutes: int
    max_job_age_days: int
    max_calls_per_day: int
    max_calls_per_source_per_day: int
    oci_direct_fallback_enabled: bool

    @property
    def configured(self) -> bool:
        return bool(self.worker_url and self.secret and self.sources)


@dataclass(frozen=True)
class ExternalSeedJob:
    source_job_id: str
    title: str
    company_name: str
    job_url: str
    location: str | None = None
    description: str | None = None
    job_url_direct: str | None = None
    date_posted: str | None = None
    employment_type: str | None = None
    is_remote: bool | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    freshness_status: str = "unknown"

    def as_payload(self) -> dict[str, Any]:
        return {
            "source_job_id": self.source_job_id,
            "title": self.title,
            "company_name": self.company_name,
            "job_url": self.job_url,
            "location": self.location,
            "description": self.description,
            "job_url_direct": self.job_url_direct or self.job_url,
            "date_posted": self.date_posted,
            "employment_type": self.employment_type,
            "is_remote": self.is_remote,
            "metadata": {
                **dict(self.metadata),
                "freshness_status": self.freshness_status,
            },
        }


@dataclass(frozen=True)
class ExternalSeedFetchResult:
    source: str
    fetched_at: str
    upstream_url: str
    jobs: tuple[ExternalSeedJob, ...]
    warnings: tuple[str, ...] = ()
    request_id: str | None = None


@dataclass(frozen=True)
class ExternalSeedFetchSummary:
    success: bool
    source: str
    status: str
    fetched_count: int = 0
    imported_count: int = 0
    skipped_count: int = 0
    warnings: tuple[str, ...] = ()
    next_eligible_at: str | None = None
    failure_class: str | None = None
    budget_remaining: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "source": self.source,
            "status": self.status,
            "fetched_count": self.fetched_count,
            "imported_count": self.imported_count,
            "skipped_count": self.skipped_count,
            "warnings": list(self.warnings),
            "next_eligible_at": self.next_eligible_at,
            "failure_class": self.failure_class,
            "budget_remaining": self.budget_remaining,
        }


def _split_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


def _production_like() -> bool:
    env = (
        os.getenv("JOBSCOUT_ENV")
        or os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or "development"
    )
    return env.strip().lower() in {"production", "prod", "staging"}


def get_external_seed_fetcher_config() -> ExternalSeedFetcherConfig:
    sources = tuple(
        source for source in _split_csv(os.getenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_SOURCES"))
        if source in ALLOWED_SOURCES
    )
    max_jobs = min(
        max(_env_int("JOBSCOUT_EXTERNAL_SEED_FETCHER_MAX_JOBS_PER_SOURCE", MAX_JOBS_PER_SOURCE), 1),
        MAX_JOBS_PER_SOURCE,
    )
    max_calls = min(
        max(_env_int("JOBSCOUT_EXTERNAL_SEED_FETCHER_MAX_CALLS_PER_DAY", DEFAULT_GLOBAL_CALLS_PER_DAY), 0),
        RUNTIME_MAX_CALLS_PER_DAY,
    )
    max_source_calls = min(
        max(
            _env_int(
                "JOBSCOUT_EXTERNAL_SEED_FETCHER_MAX_CALLS_PER_SOURCE_PER_DAY",
                DEFAULT_SOURCE_CALLS_PER_DAY,
            ),
            0,
        ),
        RUNTIME_MAX_CALLS_PER_SOURCE_PER_DAY,
    )
    worker_url = (os.getenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_URL") or "").strip() or None
    if worker_url and _production_like() and not worker_url.lower().startswith("https://"):
        worker_url = None
    return ExternalSeedFetcherConfig(
        enabled=_env_flag("JOBSCOUT_EXTERNAL_SEED_FETCHER_ENABLED", False),
        worker_url=worker_url,
        secret=(os.getenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_SECRET") or "").strip() or None,
        previous_secret=(
            os.getenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_PREVIOUS_SECRET") or ""
        ).strip()
        or None,
        sources=sources,
        max_jobs_per_source=max_jobs,
        timeout_seconds=min(
            max(
                _env_float(
                    "JOBSCOUT_EXTERNAL_SEED_FETCHER_TIMEOUT_SECONDS",
                    DEFAULT_TIMEOUT_SECONDS,
                ),
                1.0,
            ),
            RUNTIME_MAX_TIMEOUT_SECONDS,
        ),
        min_interval_minutes=max(
            _env_int(
                "JOBSCOUT_EXTERNAL_SEED_FETCHER_MIN_INTERVAL_MINUTES",
                DEFAULT_MIN_INTERVAL_MINUTES,
            ),
            RUNTIME_MIN_INTERVAL_MINUTES,
        ),
        max_job_age_days=max(
            _env_int(
                "JOBSCOUT_EXTERNAL_SEED_FETCHER_MAX_JOB_AGE_DAYS",
                DEFAULT_MAX_JOB_AGE_DAYS,
            ),
            1,
        ),
        max_calls_per_day=max_calls,
        max_calls_per_source_per_day=max_source_calls,
        oci_direct_fallback_enabled=False,
    )


def external_seed_fetcher_catalog_status(
    source: str,
    *,
    config: ExternalSeedFetcherConfig | None = None,
) -> dict[str, Any] | None:
    cfg = config or get_external_seed_fetcher_config()
    if source not in ALLOWED_SOURCES:
        return None
    if source not in cfg.sources:
        return {
            "enabled": cfg.enabled,
            "configured": False,
            "status": "unconfigured",
            "provider": PROVIDER_NAME,
        }
    status = "configured" if cfg.enabled and cfg.configured else "unconfigured"
    if not cfg.enabled:
        status = "disabled"
    return {
        "enabled": cfg.enabled,
        "configured": cfg.configured,
        "status": status,
        "provider": PROVIDER_NAME,
    }


def _canonical_body(payload: dict[str, Any]) -> bytes:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    body = text.encode("utf-8")
    if len(body) > REQUEST_BODY_MAX_BYTES:
        raise ExternalSeedFetchError(
            "external_seed_request_too_large",
            "External seed fetch request body exceeds the safety cap.",
            status_code=400,
        )
    return body


def _signature_headers(
    *,
    secret: str,
    body: bytes,
    request_id: str,
    timestamp: int,
) -> dict[str, str]:
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = f"v1\n{timestamp}\n{request_id}\nPOST\n/fetch\n{body_hash}"
    signature = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256)
    return {
        "Content-Type": "application/json",
        "User-Agent": "JobScout external seed fetcher (+https://jobscout.sookyungahn.com)",
        "X-JobScout-Timestamp": str(timestamp),
        "X-JobScout-Request-Id": request_id,
        "X-JobScout-Body-SHA256": body_hash,
        "X-JobScout-Signature": f"v1={signature.hexdigest()}",
        "X-JobScout-Client": "jobscout-cloud",
    }


def _read_capped_response(response: requests.Response, cap_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_content(chunk_size=16 * 1024):
        if not chunk:
            continue
        total += len(chunk)
        if total > cap_bytes:
            raise ExternalSeedFetchError(
                "external_seed_response_too_large",
                "External seed fetch response exceeded the safety cap.",
                status_code=502,
                failure_class="response_too_large",
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _normalize_url(
    value: Any,
    *,
    base_url: str,
    allowed_netloc: str | None = None,
) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(urljoin(base_url, raw))
    if parsed.scheme not in {"https", "http"} or not parsed.netloc:
        return None
    if allowed_netloc and parsed.netloc.lower() != allowed_netloc.lower():
        return None
    normalized = parsed._replace(fragment="")
    return urlunparse(normalized)


def _parse_posted_at(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "remote"}:
        return True
    if text in {"0", "false", "no", "onsite"}:
        return False
    return None


def _validate_job(
    raw_job: Any,
    *,
    source: str,
    upstream_url: str,
    max_age_days: int,
    seen: set[tuple[str, str]],
) -> tuple[ExternalSeedJob | None, str | None]:
    if not isinstance(raw_job, dict):
        return None, "invalid_job_shape"

    source_job_id = str(raw_job.get("source_job_id") or "").strip()
    title = str(raw_job.get("title") or "").strip()
    company_name = str(raw_job.get("company_name") or "").strip()
    source_netloc = urlparse(upstream_url).netloc
    job_url = _normalize_url(
        raw_job.get("job_url"),
        base_url=upstream_url,
        allowed_netloc=source_netloc,
    )
    if not source_job_id or not title or not company_name or not job_url:
        return None, "missing_required_field"

    key = (job_url.lower(), source_job_id.lower())
    if key in seen:
        return None, "duplicate_job"
    seen.add(key)

    posted_at = _parse_posted_at(raw_job.get("date_posted"))
    freshness_status = "unknown"
    if posted_at is not None:
        max_age = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        if posted_at < max_age:
            return None, "stale_job"
        freshness_status = "fresh"

    description = raw_job.get("description")
    if isinstance(description, str):
        description = description.strip()[:JOB_DESCRIPTION_MAX_CHARS]
    else:
        description = None

    metadata = raw_job.get("metadata") if isinstance(raw_job.get("metadata"), dict) else {}
    return (
        ExternalSeedJob(
            source_job_id=source_job_id,
            title=title[:240],
            company_name=company_name[:240],
            job_url=job_url,
            location=str(raw_job.get("location") or "").strip()[:240] or None,
            description=description,
            job_url_direct=_normalize_url(
                raw_job.get("job_url_direct"),
                base_url=job_url,
                allowed_netloc=source_netloc,
            ),
            date_posted=posted_at.isoformat() if posted_at else None,
            employment_type=str(raw_job.get("employment_type") or "").strip()[:120] or None,
            is_remote=_coerce_bool(raw_job.get("is_remote")),
            metadata={
                key: value
                for key, value in metadata.items()
                if isinstance(key, str) and len(key) <= 80
            },
            freshness_status=freshness_status,
        ),
        None,
    )


def _validate_fetch_payload(
    payload: Any,
    *,
    expected_source: str,
    config: ExternalSeedFetcherConfig,
) -> ExternalSeedFetchResult:
    if not isinstance(payload, dict):
        raise ExternalSeedFetchError(
            "external_seed_invalid_payload",
            "External seed fetch response is not a JSON object.",
            status_code=502,
            failure_class="invalid_payload",
        )
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ExternalSeedFetchError(
            "external_seed_invalid_schema",
            "External seed fetch response schema version is unsupported.",
            status_code=502,
            failure_class="invalid_schema",
        )
    source = str(payload.get("source") or "").strip().lower()
    if source != expected_source:
        raise ExternalSeedFetchError(
            "external_seed_source_mismatch",
            "External seed fetch response source does not match the request.",
            status_code=502,
            failure_class="source_mismatch",
        )
    upstream_url = _normalize_url(payload.get("upstream_url"), base_url=SOURCE_URLS[source])
    if upstream_url != SOURCE_URLS[source]:
        raise ExternalSeedFetchError(
            "external_seed_upstream_mismatch",
            "External seed fetch response used an unexpected upstream URL.",
            status_code=502,
            failure_class="upstream_mismatch",
        )

    raw_jobs = payload.get("jobs")
    if not isinstance(raw_jobs, list):
        raise ExternalSeedFetchError(
            "external_seed_invalid_jobs",
            "External seed fetch response jobs field is invalid.",
            status_code=502,
            failure_class="invalid_jobs",
        )

    warnings = [str(item)[:160] for item in payload.get("warnings") or [] if item]
    seen: set[tuple[str, str]] = set()
    jobs: list[ExternalSeedJob] = []
    for raw_job in raw_jobs[: config.max_jobs_per_source]:
        job, warning = _validate_job(
            raw_job,
            source=source,
            upstream_url=upstream_url,
            max_age_days=config.max_job_age_days,
            seen=seen,
        )
        if job is None:
            if warning:
                warnings.append(warning)
            continue
        jobs.append(job)

    return ExternalSeedFetchResult(
        source=source,
        fetched_at=str(payload.get("fetched_at") or datetime.now(timezone.utc).isoformat()),
        upstream_url=upstream_url,
        jobs=tuple(jobs),
        warnings=tuple(warnings),
        request_id=str(payload.get("request_id") or "") or None,
    )


class ExternalSeedFetcherClient:
    """Small signed client for the Cloudflare Worker seed fetcher."""

    def __init__(self, config: ExternalSeedFetcherConfig | None = None) -> None:
        self.config = config or get_external_seed_fetcher_config()

    def fetch_source(self, source: str, *, limit: int | None = None) -> ExternalSeedFetchResult:
        config = self.config
        normalized_source = source.strip().lower()
        if not config.enabled:
            raise ExternalSeedFetchError(
                "external_seed_disabled",
                "External seed fetching is disabled.",
                status_code=403,
            )
        if normalized_source not in config.sources:
            raise ExternalSeedFetchError(
                "external_seed_source_not_allowed",
                "External seed fetching is not enabled for this source.",
                status_code=403,
            )
        if not config.configured:
            raise ExternalSeedFetchError(
                "external_seed_unconfigured",
                "External seed fetcher URL or secret is not configured.",
                status_code=503,
            )

        request_id = str(uuid.uuid4())
        payload = {
            "source": normalized_source,
            "limit": max(
                min(limit or config.max_jobs_per_source, config.max_jobs_per_source),
                1,
            ),
            "request_id": request_id,
        }
        body = _canonical_body(payload)
        headers = _signature_headers(
            secret=str(config.secret),
            body=body,
            request_id=request_id,
            timestamp=int(time.time()),
        )

        try:
            response = requests.post(
                str(config.worker_url),
                data=body,
                headers=headers,
                timeout=config.timeout_seconds,
                stream=True,
            )
        except requests.RequestException as exc:
            raise ExternalSeedFetchError(
                "external_seed_request_failed",
                "External seed fetcher request failed.",
                status_code=502,
                failure_class=exc.__class__.__name__,
            ) from exc

        if response.status_code >= 400:
            raise ExternalSeedFetchError(
                "external_seed_worker_error",
                "External seed fetcher returned an error.",
                status_code=502,
                failure_class=f"worker_http_{response.status_code}",
            )

        body_bytes = _read_capped_response(response, RESPONSE_BODY_MAX_BYTES)
        try:
            payload = json.loads(body_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExternalSeedFetchError(
                "external_seed_invalid_json",
                "External seed fetcher returned invalid JSON.",
                status_code=502,
                failure_class="invalid_json",
            ) from exc

        return _validate_fetch_payload(
            payload,
            expected_source=normalized_source,
            config=config,
        )


def _tenant_key(tenant_id: Any | None) -> str:
    raw = str(tenant_id or "local")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _quota_key(name: str, *, source: str | None = None) -> str:
    window = int(time.time()) // DAILY_WINDOW_SECONDS
    if source:
        return f"external_seed_fetch:quota:{window}:source:{source}"
    return f"external_seed_fetch:quota:{window}:{name}"


def _incr_with_ttl(redis_client: Any, key: str, ttl_seconds: int) -> int:
    count = int(redis_client.incr(key))
    if count == 1:
        redis_client.expire(key, ttl_seconds)
    return count


def _redis_or_error() -> Any:
    try:
        from core.redis_streams import get_redis_client

        client = get_redis_client()
        client.ping()
        return client
    except Exception as exc:
        if _production_like():
            raise ExternalSeedFetchError(
                "external_seed_quota_unavailable",
                "External seed quota enforcement is unavailable.",
                status_code=503,
                failure_class=exc.__class__.__name__,
            ) from exc
        logger.warning("External seed Redis controls unavailable outside production.")
        return None


def _iso_from_epoch(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def _status_key(tenant_id: Any | None, source: str) -> str:
    return f"external_seed_fetch:status:{_tenant_key(tenant_id)}:{source}"


def _record_status(
    redis_client: Any,
    *,
    tenant_id: Any | None,
    source: str,
    status: str,
    fetched_count: int = 0,
    imported_count: int = 0,
    skipped_count: int = 0,
    warnings: tuple[str, ...] = (),
    failure_class: str | None = None,
    next_eligible_at: str | None = None,
    budget_remaining: int | None = None,
) -> None:
    if redis_client is None:
        return
    payload = {
        "source": source,
        "status": status,
        "last_attempt_at": datetime.now(timezone.utc).isoformat(),
        "last_success_at": datetime.now(timezone.utc).isoformat() if status == "ok" else None,
        "fetched_count": fetched_count,
        "imported_count": imported_count,
        "skipped_count": skipped_count,
        "warnings": list(warnings)[:10],
        "failure_class": failure_class,
        "next_eligible_at": next_eligible_at,
        "budget_remaining": budget_remaining,
    }
    redis_client.setex(
        _status_key(tenant_id, source),
        STATUS_TTL_SECONDS,
        json.dumps(payload, sort_keys=True),
    )


def _record_metric(source: str, outcome: str) -> None:
    try:
        from saas.metrics import record_external_seed_fetch_outcome

        record_external_seed_fetch_outcome(source, outcome)
    except Exception:
        return


def _load_status(redis_client: Any, tenant_id: Any | None, source: str) -> dict[str, Any]:
    if redis_client is None:
        return {}
    raw = redis_client.get(_status_key(tenant_id, source))
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"status": "invalid_status"}


def _reserve_budget(
    redis_client: Any,
    *,
    tenant_id: Any | None,
    source: str,
    config: ExternalSeedFetcherConfig,
) -> tuple[int | None, str | None]:
    if redis_client is None:
        return None, None

    now = time.time()
    interval_key = f"external_seed_fetch:interval:{_tenant_key(tenant_id)}:{source}"
    existing_next = redis_client.get(interval_key)
    if existing_next:
        try:
            next_epoch = float(existing_next)
        except ValueError:
            next_epoch = 0.0
        if next_epoch > now:
            return None, _iso_from_epoch(next_epoch)

    global_count = _incr_with_ttl(
        redis_client,
        _quota_key("global"),
        DAILY_WINDOW_SECONDS + 60,
    )
    source_count = _incr_with_ttl(
        redis_client,
        _quota_key("source", source=source),
        DAILY_WINDOW_SECONDS + 60,
    )
    remaining = min(
        max(config.max_calls_per_day - global_count, 0),
        max(config.max_calls_per_source_per_day - source_count, 0),
    )
    if (
        global_count > config.max_calls_per_day
        or source_count > config.max_calls_per_source_per_day
    ):
        raise ExternalSeedFetchError(
            "external_seed_budget_exhausted",
            "External seed fetch budget is exhausted for the current window.",
            status_code=429,
            failure_class="budget_exhausted",
        )

    next_epoch = now + (config.min_interval_minutes * 60)
    redis_client.setex(
        interval_key,
        max(config.min_interval_minutes * 60, 1),
        str(next_epoch),
    )
    return remaining, _iso_from_epoch(next_epoch)


def _import_jobs(
    result: ExternalSeedFetchResult,
    *,
    tenant_id: Any | None,
) -> int:
    service = JobETLService(ai_service=None)  # import_record does not use the LLM provider
    imported = 0
    with job_uow() as repo:
        for job in result.jobs:
            record = NormalizedJobRecord.from_external_seed_payload(
                job.as_payload(),
                result.source,
                tenant_id=tenant_id,
                provider=PROVIDER_NAME,
                fetched_at=result.fetched_at,
                request_id=result.request_id,
            )
            service.import_record(repo, record)
            imported += 1
    return imported


def fetch_and_import_external_seed_source(
    source: str,
    *,
    tenant_id: Any | None = None,
    limit: int | None = None,
    config: ExternalSeedFetcherConfig | None = None,
    client: ExternalSeedFetcherClient | None = None,
) -> ExternalSeedFetchSummary:
    cfg = config or get_external_seed_fetcher_config()
    normalized_source = source.strip().lower()
    if normalized_source not in ALLOWED_SOURCES:
        raise ExternalSeedFetchError(
            "external_seed_source_unknown",
            "The requested source is not supported by the external seed fetcher.",
            status_code=404,
        )
    if not cfg.enabled:
        raise ExternalSeedFetchError(
            "external_seed_disabled",
            "External seed fetching is disabled.",
            status_code=403,
        )
    if normalized_source not in cfg.sources:
        raise ExternalSeedFetchError(
            "external_seed_source_not_allowed",
            "External seed fetching is not enabled for this source.",
            status_code=403,
        )
    if not cfg.configured:
        raise ExternalSeedFetchError(
            "external_seed_unconfigured",
            "External seed fetcher URL or secret is not configured.",
            status_code=503,
        )

    redis_client = _redis_or_error()
    budget_remaining: int | None = None
    next_eligible_at: str | None = None
    try:
        budget_remaining, next_eligible_at = _reserve_budget(
            redis_client,
            tenant_id=tenant_id,
            source=normalized_source,
            config=cfg,
        )
        if budget_remaining is None and next_eligible_at is not None:
            _record_metric(normalized_source, "rate_limited")
            summary = ExternalSeedFetchSummary(
                success=False,
                source=normalized_source,
                status="rate_limited",
                next_eligible_at=next_eligible_at,
                failure_class="min_interval",
                budget_remaining=budget_remaining,
            )
            _record_status(
                redis_client,
                tenant_id=tenant_id,
                source=normalized_source,
                status=summary.status,
                failure_class=summary.failure_class,
                next_eligible_at=next_eligible_at,
                budget_remaining=budget_remaining,
            )
            return summary

        fetch_client = client or ExternalSeedFetcherClient(cfg)
        _record_metric(normalized_source, "attempted")
        result = fetch_client.fetch_source(normalized_source, limit=limit)
        imported = _import_jobs(result, tenant_id=tenant_id)
        _record_metric(normalized_source, "imported")
        skipped = max(0, int(limit or cfg.max_jobs_per_source) - len(result.jobs))
        summary = ExternalSeedFetchSummary(
            success=True,
            source=normalized_source,
            status="ok",
            fetched_count=len(result.jobs),
            imported_count=imported,
            skipped_count=skipped,
            warnings=result.warnings,
            next_eligible_at=next_eligible_at,
            budget_remaining=budget_remaining,
        )
        _record_status(
            redis_client,
            tenant_id=tenant_id,
            source=normalized_source,
            status="ok",
            fetched_count=len(result.jobs),
            imported_count=imported,
            skipped_count=skipped,
            warnings=result.warnings,
            next_eligible_at=next_eligible_at,
            budget_remaining=budget_remaining,
        )
        return summary
    except ExternalSeedFetchError as exc:
        metric_outcome = (
            "rate_limited"
            if exc.status_code == 429 or exc.failure_class in {"budget_exhausted", "min_interval"}
            else "failed"
        )
        _record_metric(normalized_source, metric_outcome)
        _record_status(
            redis_client,
            tenant_id=tenant_id,
            source=normalized_source,
            status="degraded",
            failure_class=exc.failure_class,
            next_eligible_at=next_eligible_at,
            budget_remaining=budget_remaining,
        )
        raise


def get_external_seed_fetcher_status(tenant_id: Any | None = None) -> dict[str, Any]:
    cfg = get_external_seed_fetcher_config()
    try:
        redis_client = _redis_or_error()
        redis_warning = None
    except ExternalSeedFetchError as exc:
        redis_client = None
        redis_warning = exc.failure_class
    sources: dict[str, Any] = {}
    for source in sorted(ALLOWED_SOURCES):
        catalog = external_seed_fetcher_catalog_status(source, config=cfg) or {}
        sources[source] = {
            **catalog,
            **_load_status(redis_client, tenant_id, source),
        }
    return {
        "enabled": cfg.enabled,
        "configured": cfg.configured,
        "provider": PROVIDER_NAME,
        "allowed_sources": list(cfg.sources),
        "limits": {
            "max_jobs_per_source": cfg.max_jobs_per_source,
            "min_interval_minutes": cfg.min_interval_minutes,
            "max_job_age_days": cfg.max_job_age_days,
            "max_calls_per_day": cfg.max_calls_per_day,
            "max_calls_per_source_per_day": cfg.max_calls_per_source_per_day,
            "oci_direct_fallback_enabled": cfg.oci_direct_fallback_enabled,
        },
        "warning": redis_warning,
        "sources": sources,
    }
