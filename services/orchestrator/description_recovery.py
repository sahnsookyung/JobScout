"""Recover missing job descriptions through compliant ATS APIs only."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse, urlunparse

import requests

from core.metrics import (
    observe_description_recovery_provider_latency_seconds,
    record_description_recovery_job,
)
from database.models import JobPost, JobPostSource
from database.repositories.job_post import DESCRIPTION_RECOVERY_RETRY_DELAYS_SECONDS
from database.repository import JobRepository

logger = logging.getLogger(__name__)

SUPPORTED_ATS_PROVIDERS = frozenset({"greenhouse", "lever", "ashby"})
PROHIBITED_SOURCE_NAMES = frozenset({"tokyodev", "japandev", "jobspy"})
ADAPTER_MISSING_SOURCE_NAMES = frozenset({"workday"})
RECOVERY_USER_AGENT = "JobScout description recovery (+https://jobscout.sookyungahn.com)"
REQUEST_TIMEOUT_SECONDS = 30
MAX_DESCRIPTION_CHARS = 60_000


@dataclass(frozen=True)
class AtsBinding:
    provider: str
    source_key: str
    source_job_id: str | None
    job_url: str | None


@dataclass(frozen=True)
class ProviderJob:
    source_job_id: str | None
    title: str | None
    company_name: str | None
    location: str | None
    description: str | None
    job_url: str | None
    job_url_direct: str | None
    raw_provider: str


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _raw_payload(job: JobPost) -> dict[str, Any]:
    payload = getattr(job, "raw_payload", None)
    return payload if isinstance(payload, dict) else {}


def _source_provider(job: JobPost, source: JobPostSource | None) -> str | None:
    payload = _raw_payload(job)
    for candidate in (
        payload.get("source_provider"),
        payload.get("provider"),
        getattr(source, "site", None),
    ):
        value = _clean(candidate)
        if not value:
            continue
        lowered = value.lower()
        if lowered in SUPPORTED_ATS_PROVIDERS or lowered in PROHIBITED_SOURCE_NAMES:
            return lowered
        if lowered in ADAPTER_MISSING_SOURCE_NAMES:
            return lowered
        for token in lowered.replace("/", ":").split(":"):
            if token in SUPPORTED_ATS_PROVIDERS or token in PROHIBITED_SOURCE_NAMES:
                return token
            if token in ADAPTER_MISSING_SOURCE_NAMES:
                return token
    return _provider_from_url(
        _clean(getattr(source, "job_url_direct", None))
        or _clean(getattr(source, "job_url", None))
        or _clean(payload.get("job_url_direct"))
        or _clean(payload.get("job_url"))
    )


def _provider_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if "greenhouse.io" in host:
        return "greenhouse"
    if host == "jobs.lever.co":
        return "lever"
    if host == "jobs.ashbyhq.com":
        return "ashby"
    if "workdayjobs.com" in host or "myworkdayjobs.com" in host:
        return "workday"
    if parsed.scheme == "external":
        provider = parsed.netloc.lower()
        return provider if provider in SUPPORTED_ATS_PROVIDERS else None
    return None


def _normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return url.strip().rstrip("/").lower() or None
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        path=parsed.path.rstrip("/"),
        params="",
        query="",
        fragment="",
    )
    return urlunparse(normalized).lower()


def _source_key_from_external_url(provider: str, url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme != "external" or parsed.netloc.lower() != provider:
        return None
    parts = [part for part in parsed.path.split("/") if part]
    return parts[0] if parts else None


def _source_key_from_public_url(provider: str, url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    parts = [part for part in parsed.path.split("/") if part]
    if provider == "greenhouse" and "greenhouse.io" in host:
        query = parse_qs(parsed.query)
        if query.get("for"):
            return query["for"][0]
        return parts[0] if parts else None
    if provider == "lever" and host == "jobs.lever.co":
        return parts[0] if parts else None
    if provider == "ashby" and host == "jobs.ashbyhq.com":
        return parts[0] if parts else None
    return None


def _source_job_id_from_url(provider: str, url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if provider == "greenhouse":
        query = parse_qs(parsed.query)
        if query.get("token"):
            return query["token"][0]
        return parts[-1] if parts and parts[-2:-1] == ["jobs"] else None
    if provider in {"lever", "ashby"}:
        return parts[1] if len(parts) > 1 else None
    if parsed.scheme == "external":
        return parts[1] if len(parts) > 1 else None
    return None


def _primary_source(job: JobPost) -> JobPostSource | None:
    sources = list(getattr(job, "sources", []) or [])
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


def resolve_ats_binding(job: JobPost, source: JobPostSource | None = None) -> AtsBinding | str:
    source = source or _primary_source(job)
    provider = _source_provider(job, source)
    if provider in PROHIBITED_SOURCE_NAMES:
        return "source_prohibited"
    if provider in ADAPTER_MISSING_SOURCE_NAMES:
        return "source_adapter_missing"
    if provider not in SUPPORTED_ATS_PROVIDERS:
        return "source_unsupported" if provider else "source_unmapped"

    payload = _raw_payload(job)
    source_url = (
        _clean(getattr(source, "job_url_direct", None))
        or _clean(getattr(source, "job_url", None))
        or _clean(payload.get("job_url_direct"))
        or _clean(payload.get("job_url"))
    )
    source_key = (
        _clean(payload.get("source_key"))
        or _source_key_from_external_url(provider, source_url)
        or _source_key_from_public_url(provider, source_url)
    )
    source_job_id = (
        _clean(getattr(source, "source_job_id", None))
        or _clean(payload.get("source_job_id"))
        or _source_job_id_from_url(provider, source_url)
    )
    if not source_key:
        return "source_unmapped"
    return AtsBinding(
        provider=provider,
        source_key=source_key,
        source_job_id=source_job_id,
        job_url=source_url,
    )


def _request_json(url: str) -> Any:
    response = requests.get(
        url,
        headers={"User-Agent": RECOVERY_USER_AGENT, "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def _trim_description(value: Any) -> str | None:
    text = _clean(value)
    if not text:
        return None
    return text[:MAX_DESCRIPTION_CHARS]


def _greenhouse_jobs(source_key: str) -> list[ProviderJob]:
    payload = _request_json(
        f"https://boards-api.greenhouse.io/v1/boards/{source_key}/jobs?content=true"
    )
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    rows = jobs if isinstance(jobs, list) else []
    parsed: list[ProviderJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        location = row.get("location")
        location_text = location.get("name") if isinstance(location, dict) else None
        parsed.append(
            ProviderJob(
                source_job_id=_clean(row.get("id")),
                title=_clean(row.get("title")),
                company_name=None,
                location=_clean(location_text),
                description=_trim_description(row.get("content")),
                job_url=_clean(row.get("absolute_url")),
                job_url_direct=_clean(row.get("absolute_url")),
                raw_provider="greenhouse",
            )
        )
    return parsed


def _lever_jobs(source_key: str) -> list[ProviderJob]:
    payload = _request_json(f"https://api.lever.co/v0/postings/{source_key}?mode=json")
    rows = payload if isinstance(payload, list) else []
    parsed: list[ProviderJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        categories = row.get("categories") if isinstance(row.get("categories"), dict) else {}
        parsed.append(
            ProviderJob(
                source_job_id=_clean(row.get("id")),
                title=_clean(row.get("text")),
                company_name=None,
                location=_clean(categories.get("location")),
                description=_trim_description(
                    row.get("descriptionPlain") or row.get("description")
                ),
                job_url=_clean(row.get("hostedUrl")),
                job_url_direct=_clean(row.get("applyUrl") or row.get("hostedUrl")),
                raw_provider="lever",
            )
        )
    return parsed


def _ashby_jobs(source_key: str) -> list[ProviderJob]:
    payload = _request_json(f"https://api.ashbyhq.com/posting-api/job-board/{source_key}")
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    rows = jobs if isinstance(jobs, list) else []
    parsed: list[ProviderJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        parsed.append(
            ProviderJob(
                source_job_id=_clean(row.get("id")),
                title=_clean(row.get("title")),
                company_name=None,
                location=_clean(row.get("locationName")),
                description=_trim_description(
                    row.get("descriptionPlain") or row.get("descriptionHtml")
                ),
                job_url=_clean(row.get("jobUrl")),
                job_url_direct=_clean(row.get("applyUrl") or row.get("jobUrl")),
                raw_provider="ashby",
            )
        )
    return parsed


def fetch_provider_jobs(binding: AtsBinding) -> list[ProviderJob]:
    started = time.perf_counter()
    try:
        if binding.provider == "greenhouse":
            return _greenhouse_jobs(binding.source_key)
        if binding.provider == "lever":
            return _lever_jobs(binding.source_key)
        if binding.provider == "ashby":
            return _ashby_jobs(binding.source_key)
        return []
    finally:
        observe_description_recovery_provider_latency_seconds(
            binding.provider,
            time.perf_counter() - started,
        )


def _match_provider_job(binding: AtsBinding, provider_jobs: list[ProviderJob]) -> ProviderJob | None:
    target_ids = {binding.source_job_id} if binding.source_job_id else set()
    target_urls = {_normalize_url(binding.job_url)} if binding.job_url else set()
    target_urls.discard(None)
    for job in provider_jobs:
        if job.source_job_id and job.source_job_id in target_ids:
            return job
        if _normalize_url(job.job_url) in target_urls or _normalize_url(job.job_url_direct) in target_urls:
            return job
    return None


def _job_data_from_provider(job: JobPost, binding: AtsBinding, provider_job: ProviderJob) -> dict[str, Any]:
    return {
        "title": provider_job.title or job.title,
        "company_name": provider_job.company_name or job.company,
        "location": provider_job.location or job.location_text,
        "description": provider_job.description,
        "is_remote": job.is_remote,
        "skills": [],
        "job_url": provider_job.job_url or binding.job_url,
        "job_url_direct": provider_job.job_url_direct or provider_job.job_url or binding.job_url,
        "source_job_id": provider_job.source_job_id or binding.source_job_id,
        "source_provider": "ats_description_recovery",
        "source_key": binding.source_key,
        "source_metadata": {
            "ingest_mode": "description_recovery",
            "description_source": "ats_description_recovery",
            "description_provider": binding.provider,
            "description_completeness": "full",
            "trusted_description_metadata": True,
        },
    }


def _retryable_allowed(job: JobPost) -> bool:
    return int(getattr(job, "description_recovery_attempts", 0) or 0) < len(
        DESCRIPTION_RECOVERY_RETRY_DELAYS_SECONDS
    )


def _provider_fetch_failed(
    repo: JobRepository,
    job: JobPost,
    binding: AtsBinding,
    *,
    run_id: str,
    exc: Exception,
) -> dict[str, Any]:
    retryable = _retryable_allowed(job)
    outcome = "failed_retryable" if retryable else "failed_terminal"
    repo.mark_description_recovery_status(
        job,
        status=outcome,
        reason="provider_fetch_failed",
        run_id=run_id,
        error=exc.__class__.__name__,
        retryable=retryable,
    )
    record_description_recovery_job(binding.provider, outcome)
    logger.warning(
        "Description recovery provider fetch failed for %s (%s)",
        binding.provider,
        exc.__class__.__name__,
    )
    try:
        from core.oci_critical_logging import emit_oci_critical_event

        emit_oci_critical_event(
            "description_recovery",
            severity="warning",
            provider=binding.provider,
            outcome="provider_fetch_failed",
            error_class=exc.__class__.__name__,
        )
    except Exception:
        pass
    return {
        "job_id": str(job.id),
        "provider": binding.provider,
        "outcome": outcome,
    }


def recover_missing_description_job(
    repo: JobRepository,
    job: JobPost,
    *,
    run_id: str,
    source: JobPostSource | None = None,
    binding: AtsBinding | str | None = None,
    provider_jobs: list[ProviderJob] | None = None,
    mark_refreshing: bool = True,
) -> dict[str, Any]:
    """Resolve one missing-description job into a durable recovery outcome."""
    source = source or _primary_source(job)
    binding = binding if binding is not None else resolve_ats_binding(job, source)
    if isinstance(binding, str):
        repo.mark_description_recovery_status(
            job,
            status=binding,
            reason=binding,
            run_id=run_id,
        )
        metric_provider = {
            "source_prohibited": "prohibited",
            "source_unsupported": "unsupported",
            "source_unmapped": "unmapped",
            "source_adapter_missing": "adapter_missing",
        }.get(binding, "unmapped")
        record_description_recovery_job(
            metric_provider,
            binding,
        )
        return {"job_id": str(job.id), "provider": None, "outcome": binding}

    if mark_refreshing:
        repo.mark_description_recovery_refreshing(job, run_id=run_id)
    if provider_jobs is None:
        try:
            provider_jobs = fetch_provider_jobs(binding)
        except (requests.RequestException, ValueError) as exc:
            return _provider_fetch_failed(repo, job, binding, run_id=run_id, exc=exc)

    provider_job = _match_provider_job(binding, provider_jobs)
    if provider_job is None:
        repo.mark_description_recovery_posting_not_found(
            job,
            source=source,
            run_id=run_id,
        )
        record_description_recovery_job(binding.provider, "posting_not_found")
        try:
            from core.oci_critical_logging import emit_oci_critical_event

            emit_oci_critical_event(
                "description_recovery",
                severity="info",
                provider=binding.provider,
                outcome="posting_not_found",
            )
        except Exception:
            pass
        return {
            "job_id": str(job.id),
            "provider": binding.provider,
            "outcome": "posting_not_found",
        }

    if not provider_job.description:
        retryable = _retryable_allowed(job)
        repo.mark_description_recovery_status(
            job,
            status="failed_retryable" if retryable else "failed_terminal",
            reason="provider_payload_missing_description",
            run_id=run_id,
            retryable=retryable,
        )
        record_description_recovery_job(
            binding.provider,
            "failed_retryable" if retryable else "failed_terminal",
        )
        return {
            "job_id": str(job.id),
            "provider": binding.provider,
            "outcome": "failed_retryable" if retryable else "failed_terminal",
        }

    repo.save_job_content(job.id, _job_data_from_provider(job, binding, provider_job))
    repo.mark_description_recovered(job, run_id=run_id)
    record_description_recovery_job(binding.provider, "description_found")
    return {
        "job_id": str(job.id),
        "provider": binding.provider,
        "outcome": "description_found",
        "extraction_job_id": str(job.id),
    }


def recover_missing_description_jobs(
    repo: JobRepository,
    jobs: list[JobPost],
    *,
    run_id: str,
) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "claimed": len(jobs),
        "processed": 0,
        "description_found": 0,
        "posting_not_found": 0,
        "source_unsupported": 0,
        "source_prohibited": 0,
        "source_unmapped": 0,
        "source_adapter_missing": 0,
        "failed_retryable": 0,
        "failed_terminal": 0,
        "description_found_job_ids": [],
        "provider_breakdown": {},
    }

    def record_result(result: dict[str, Any]) -> None:
        outcome = str(result.get("outcome") or "failed_terminal")
        provider = str(result.get("provider") or "unknown")
        stats["processed"] += 1
        if outcome in stats:
            stats[outcome] += 1
        if outcome == "description_found" and result.get("extraction_job_id"):
            stats["description_found_job_ids"].append(str(result["extraction_job_id"]))
        breakdown = stats["provider_breakdown"].setdefault(provider, {})
        breakdown[outcome] = int(breakdown.get(outcome, 0)) + 1

    grouped: dict[tuple[str, str], list[tuple[JobPost, JobPostSource | None, AtsBinding]]] = {}
    for job in jobs:
        source = _primary_source(job)
        binding = resolve_ats_binding(job, source)
        if isinstance(binding, str):
            record_result(
                recover_missing_description_job(
                    repo,
                    job,
                    run_id=run_id,
                    source=source,
                    binding=binding,
                )
            )
            continue
        grouped.setdefault((binding.provider, binding.source_key), []).append(
            (job, source, binding)
        )

    for entries in grouped.values():
        first_binding = entries[0][2]
        for job, _source, _binding in entries:
            repo.mark_description_recovery_refreshing(job, run_id=run_id)
        try:
            provider_jobs = fetch_provider_jobs(first_binding)
        except (requests.RequestException, ValueError) as exc:
            for job, _source, binding in entries:
                record_result(
                    _provider_fetch_failed(repo, job, binding, run_id=run_id, exc=exc)
                )
            continue

        for job, source, binding in entries:
            record_result(
                recover_missing_description_job(
                    repo,
                    job,
                    run_id=run_id,
                    source=source,
                    binding=binding,
                    provider_jobs=provider_jobs,
                    mark_refreshing=False,
                )
            )
    return stats
