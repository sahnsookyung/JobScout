from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

import etl.external_seed_fetcher as external_seed_fetcher
from etl.external_seed_fetcher import (
    ExternalSeedFetchError,
    ExternalSeedFetchResult,
    ExternalSeedFetcherClient,
    ExternalSeedFetcherConfig,
    ExternalSeedJob,
    fetch_and_import_external_seed_source,
)
from etl.import_models import NormalizedJobRecord


def _config(**overrides: Any) -> ExternalSeedFetcherConfig:
    values = {
        "enabled": True,
        "worker_url": "https://seed-worker.example/fetch",
        "secret": "test-secret",
        "previous_secret": None,
        "sources": ("tokyodev", "japandev"),
        "max_jobs_per_source": 25,
        "timeout_seconds": 15.0,
        "min_interval_minutes": 240,
        "max_job_age_days": 45,
        "max_calls_per_day": 100,
        "max_calls_per_source_per_day": 50,
        "oci_direct_fallback_enabled": False,
    }
    values.update(overrides)
    return ExternalSeedFetcherConfig(**values)


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.expirations: dict[str, int] = {}

    def ping(self) -> bool:
        return True

    def incr(self, key: str) -> int:
        count = int(self.values.get(key, "0")) + 1
        self.values[key] = str(count)
        return count

    def expire(self, key: str, ttl_seconds: int) -> None:
        self.expirations[key] = ttl_seconds

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        self.values[key] = value
        self.expirations[key] = ttl_seconds

    def get(self, key: str) -> str | None:
        return self.values.get(key)


class _FakeResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self.status_code = status_code
        self._body = json.dumps(payload).encode("utf-8")

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [self._body]


def _worker_payload(source: str = "tokyodev") -> dict[str, Any]:
    upstream_url = external_seed_fetcher.SOURCE_URLS[source]
    return {
        "schema_version": external_seed_fetcher.SCHEMA_VERSION,
        "source": source,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "upstream_url": upstream_url,
        "request_id": "request-1",
        "jobs": [
            {
                "source_job_id": "job-1",
                "title": "Platform Engineer",
                "company_name": "Acme",
                "job_url": f"{upstream_url.rstrip('/')}/job-1",
                "job_url_direct": f"{upstream_url.rstrip('/')}/job-1",
                "location": "Tokyo, Japan",
                "description": "Build developer tools.",
            }
        ],
        "warnings": [],
    }


def test_external_seed_config_filters_sources_and_caps_limits(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_URL", "https://worker.example/fetch")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_SECRET", "secret")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_SOURCES", "tokyodev,evil,japandev")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_MAX_JOBS_PER_SOURCE", "999")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_MAX_CALLS_PER_DAY", "999")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_MAX_CALLS_PER_SOURCE_PER_DAY", "999")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_MIN_INTERVAL_MINUTES", "1")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_TIMEOUT_SECONDS", "30")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_OCI_DIRECT_FALLBACK_ENABLED", "true")

    config = external_seed_fetcher.get_external_seed_fetcher_config()

    assert config.enabled is True
    assert config.configured is True
    assert config.sources == ("tokyodev", "japandev")
    assert config.max_jobs_per_source == 25
    assert config.max_calls_per_day == 100
    assert config.max_calls_per_source_per_day == 50
    assert config.min_interval_minutes == 240
    assert config.timeout_seconds == 15.0
    assert config.oci_direct_fallback_enabled is False

def test_external_seed_config_rejects_insecure_worker_url_in_production(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_ENV", "production")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_URL", "http://worker.example/fetch")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_SECRET", "secret")
    monkeypatch.setenv("JOBSCOUT_EXTERNAL_SEED_FETCHER_SOURCES", "tokyodev")

    config = external_seed_fetcher.get_external_seed_fetcher_config()

    assert config.worker_url is None
    assert config.configured is False


def test_external_seed_client_signs_worker_request(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        captured["url"] = url
        captured.update(kwargs)
        return _FakeResponse(_worker_payload("tokyodev"))

    monkeypatch.setattr(external_seed_fetcher.requests, "post", fake_post)

    result = ExternalSeedFetcherClient(_config()).fetch_source("tokyodev", limit=5)

    assert result.source == "tokyodev"
    assert len(result.jobs) == 1
    assert captured["url"] == "https://seed-worker.example/fetch"
    body = captured["data"]
    headers = captured["headers"]
    assert json.loads(body)["limit"] == 5
    assert headers["X-JobScout-Body-SHA256"] == hashlib.sha256(body).hexdigest()
    assert headers["X-JobScout-Signature"].startswith("v1=")
    assert captured["stream"] is True
    assert captured["timeout"] == 15.0


def test_validate_fetch_payload_rejects_stale_duplicates_and_bad_jobs() -> None:
    fresh_date = datetime.now(timezone.utc).isoformat()
    stale_date = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    payload = _worker_payload("japandev")
    valid_job = payload["jobs"][0]
    payload["jobs"] = [
        {**valid_job, "source_job_id": "job-1", "date_posted": fresh_date},
        {**valid_job, "source_job_id": "job-1", "date_posted": fresh_date},
        {**valid_job, "source_job_id": "job-2", "job_url": "https://japan-dev.com/jobs/job-2", "date_posted": stale_date},
        {"title": "Missing company"},
    ]

    result = external_seed_fetcher._validate_fetch_payload(
        payload,
        expected_source="japandev",
        config=_config(max_job_age_days=45),
    )

    assert len(result.jobs) == 1
    assert result.jobs[0].freshness_status == "fresh"
    assert {"duplicate_job", "stale_job", "missing_required_field"} <= set(result.warnings)

def test_validate_fetch_payload_rejects_job_urls_outside_source_host() -> None:
    payload = _worker_payload("tokyodev")
    payload["jobs"][0]["job_url"] = "https://attacker.example/jobs/job-1"

    result = external_seed_fetcher._validate_fetch_payload(
        payload,
        expected_source="tokyodev",
        config=_config(),
    )

    assert result.jobs == ()
    assert "missing_required_field" in result.warnings

def test_validate_fetch_payload_strips_direct_urls_outside_source_host() -> None:
    payload = _worker_payload("tokyodev")
    payload["jobs"][0]["job_url_direct"] = "https://attacker.example/jobs/job-1"

    result = external_seed_fetcher._validate_fetch_payload(
        payload,
        expected_source="tokyodev",
        config=_config(),
    )
    job_payload = result.jobs[0].as_payload()

    assert len(result.jobs) == 1
    assert job_payload["job_url_direct"] == job_payload["job_url"]
    assert "attacker.example" not in json.dumps(job_payload)


def test_fetch_and_import_uses_budget_safe_worker_path(monkeypatch) -> None:
    fetched = ExternalSeedFetchResult(
        source="tokyodev",
        fetched_at=datetime.now(timezone.utc).isoformat(),
        upstream_url=external_seed_fetcher.SOURCE_URLS["tokyodev"],
        jobs=(
            ExternalSeedJob(
                source_job_id="job-1",
                title="Platform Engineer",
                company_name="Acme",
                job_url="https://www.tokyodev.com/companies/acme/jobs/job-1",
            ),
        ),
        warnings=("parser_warning",),
        request_id="request-1",
    )
    metrics: list[tuple[str, str]] = []

    class FakeClient:
        def fetch_source(self, source: str, *, limit: int | None = None) -> ExternalSeedFetchResult:
            assert source == "tokyodev"
            assert limit == 3
            return fetched

    monkeypatch.setattr(external_seed_fetcher, "_redis_or_error", lambda: None)
    monkeypatch.setattr(external_seed_fetcher, "_import_jobs", lambda result, tenant_id: len(result.jobs))
    monkeypatch.setattr(external_seed_fetcher, "_record_metric", lambda source, outcome: metrics.append((source, outcome)))

    summary = fetch_and_import_external_seed_source(
        "tokyodev",
        tenant_id="tenant-1",
        limit=3,
        config=_config(max_jobs_per_source=3),
        client=FakeClient(),
    )

    assert summary.success is True
    assert summary.fetched_count == 1
    assert summary.imported_count == 1
    assert summary.skipped_count == 2
    assert summary.warnings == ("parser_warning",)
    assert metrics == [("tokyodev", "attempted"), ("tokyodev", "imported")]


def test_external_seed_budget_enforces_interval_and_daily_caps() -> None:
    redis = _FakeRedis()
    config = _config(max_calls_per_day=2, max_calls_per_source_per_day=2)

    remaining, next_eligible_at = external_seed_fetcher._reserve_budget(
        redis,
        tenant_id="tenant-1",
        source="tokyodev",
        config=config,
    )
    blocked_remaining, blocked_next = external_seed_fetcher._reserve_budget(
        redis,
        tenant_id="tenant-1",
        source="tokyodev",
        config=config,
    )

    assert remaining == 1
    assert next_eligible_at
    assert blocked_remaining is None
    assert blocked_next == next_eligible_at
    assert any(key.startswith("external_seed_fetch:interval:") for key in redis.values)

    with pytest.raises(ExternalSeedFetchError, match="budget is exhausted"):
        external_seed_fetcher._reserve_budget(
            _FakeRedis(),
            tenant_id="tenant-1",
            source="tokyodev",
            config=_config(max_calls_per_day=0),
        )


def test_external_seed_payload_preserves_import_identity() -> None:
    payload = _worker_payload("tokyodev")["jobs"][0]

    record = NormalizedJobRecord.from_external_seed_payload(
        payload,
        "tokyodev",
        provider=external_seed_fetcher.PROVIDER_NAME,
        tenant_id="tenant-1",
        fetched_at="2026-05-23T00:00:00+00:00",
        request_id="request-1",
    )
    job_data = record.as_job_data()

    assert record.tenant_id == "tenant-1"
    assert job_data["source_provider"] == "cloudflare_worker_seed"
    assert job_data["source_key"] == "tokyodev"
    assert job_data["source_metadata"]["ingest_mode"] == "external_seed_fetch"
    assert job_data["source_metadata"]["request_id"] == "request-1"
