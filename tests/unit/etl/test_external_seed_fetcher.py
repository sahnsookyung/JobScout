from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import requests

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


def test_catalog_status_and_small_helpers_handle_edge_cases(monkeypatch) -> None:
    config = _config(enabled=False, sources=("tokyodev",))

    assert external_seed_fetcher.external_seed_fetcher_catalog_status("unknown", config=config) is None
    assert external_seed_fetcher.external_seed_fetcher_catalog_status("japandev", config=config)["status"] == "unconfigured"
    assert external_seed_fetcher.external_seed_fetcher_catalog_status("tokyodev", config=config)["status"] == "disabled"
    assert external_seed_fetcher._normalize_url("", base_url="https://example.com") is None
    assert external_seed_fetcher._normalize_url("mailto:dev@example.com", base_url="https://example.com") is None
    assert external_seed_fetcher._normalize_url("https://evil.example", base_url="https://example.com", allowed_netloc="example.com") is None
    assert external_seed_fetcher._parse_posted_at(None) is None
    assert external_seed_fetcher._parse_posted_at("not-a-date") is None
    assert external_seed_fetcher._parse_posted_at("2026-05-23").tzinfo == timezone.utc
    assert external_seed_fetcher._coerce_bool(True) is True
    assert external_seed_fetcher._coerce_bool(None) is None
    assert external_seed_fetcher._coerce_bool("remote") is True
    assert external_seed_fetcher._coerce_bool("onsite") is False
    assert external_seed_fetcher._coerce_bool("maybe") is None

    with pytest.raises(ExternalSeedFetchError, match="body exceeds"):
        external_seed_fetcher._canonical_body({"source": "x" * 3000})


def test_capped_response_skips_empty_chunks_and_rejects_large_body() -> None:
    class Response:
        def __init__(self, chunks):
            self.chunks = chunks

        def iter_content(self, chunk_size: int):
            return self.chunks

    assert external_seed_fetcher._read_capped_response(Response([b"", b"ok"]), 4) == b"ok"
    with pytest.raises(ExternalSeedFetchError, match="safety cap"):
        external_seed_fetcher._read_capped_response(Response([b"abc", b"def"]), 4)


@pytest.mark.parametrize(
    "payload,expected_code",
    [
        ([], "external_seed_invalid_payload"),
        ({"schema_version": 999}, "external_seed_invalid_schema"),
        ({**_worker_payload("tokyodev"), "source": "japandev"}, "external_seed_source_mismatch"),
        ({**_worker_payload("tokyodev"), "upstream_url": "https://www.tokyodev.com/other"}, "external_seed_upstream_mismatch"),
        ({**_worker_payload("tokyodev"), "jobs": {}}, "external_seed_invalid_jobs"),
    ],
)
def test_validate_fetch_payload_rejects_invalid_worker_payloads(payload, expected_code) -> None:
    with pytest.raises(ExternalSeedFetchError) as exc_info:
        external_seed_fetcher._validate_fetch_payload(
            payload,
            expected_source="tokyodev",
            config=_config(),
        )

    assert exc_info.value.code == expected_code


def test_validate_job_handles_invalid_shapes_and_non_string_description() -> None:
    seen: set[tuple[str, str]] = set()
    job, warning = external_seed_fetcher._validate_job(
        123,
        source="tokyodev",
        upstream_url=external_seed_fetcher.SOURCE_URLS["tokyodev"],
        max_age_days=45,
        seen=seen,
    )
    assert job is None
    assert warning == "invalid_job_shape"

    payload = _worker_payload("tokyodev")["jobs"][0]
    payload["description"] = {"unsafe": "shape"}
    payload["metadata"] = {"ok": "kept", 123: "dropped", "x" * 90: "dropped"}
    job, warning = external_seed_fetcher._validate_job(
        payload,
        source="tokyodev",
        upstream_url=external_seed_fetcher.SOURCE_URLS["tokyodev"],
        max_age_days=45,
        seen=seen,
    )

    assert warning is None
    assert job.description is None
    assert job.metadata == {"ok": "kept"}


def test_external_seed_client_rejects_disabled_unallowed_and_unconfigured_sources() -> None:
    for config, source, expected_code in [
        (_config(enabled=False), "tokyodev", "external_seed_disabled"),
        (_config(sources=("japandev",)), "tokyodev", "external_seed_source_not_allowed"),
        (_config(worker_url=None), "tokyodev", "external_seed_unconfigured"),
    ]:
        with pytest.raises(ExternalSeedFetchError) as exc_info:
            ExternalSeedFetcherClient(config).fetch_source(source)
        assert exc_info.value.code == expected_code


def test_external_seed_client_maps_worker_failures(monkeypatch) -> None:
    client = ExternalSeedFetcherClient(_config())

    monkeypatch.setattr(
        external_seed_fetcher.requests,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(requests.Timeout("slow")),
    )
    with pytest.raises(ExternalSeedFetchError) as exc_info:
        client.fetch_source("tokyodev")
    assert exc_info.value.code == "external_seed_request_failed"
    assert exc_info.value.failure_class == "Timeout"

    monkeypatch.setattr(external_seed_fetcher.requests, "post", lambda *args, **kwargs: _FakeResponse({}, status_code=429))
    with pytest.raises(ExternalSeedFetchError) as exc_info:
        client.fetch_source("tokyodev")
    assert exc_info.value.failure_class == "worker_http_429"

    class InvalidJsonResponse:
        status_code = 200

        def iter_content(self, chunk_size: int):
            return [b"not-json"]

    monkeypatch.setattr(external_seed_fetcher.requests, "post", lambda *args, **kwargs: InvalidJsonResponse())
    with pytest.raises(ExternalSeedFetchError) as exc_info:
        client.fetch_source("tokyodev")
    assert exc_info.value.failure_class == "invalid_json"


def test_redis_controls_fail_closed_only_in_production(monkeypatch) -> None:
    monkeypatch.setattr("core.redis_streams.get_redis_client", lambda: (_ for _ in ()).throw(RuntimeError("down")))
    monkeypatch.delenv("JOBSCOUT_ENV", raising=False)

    assert external_seed_fetcher._redis_or_error() is None

    monkeypatch.setenv("JOBSCOUT_ENV", "production")
    with pytest.raises(ExternalSeedFetchError) as exc_info:
        external_seed_fetcher._redis_or_error()
    assert exc_info.value.code == "external_seed_quota_unavailable"


def test_status_recording_and_loading_handles_absent_or_invalid_state() -> None:
    redis = _FakeRedis()

    assert external_seed_fetcher._load_status(None, "tenant-1", "tokyodev") == {}
    assert external_seed_fetcher._load_status(redis, "tenant-1", "tokyodev") == {}

    external_seed_fetcher._record_status(
        redis,
        tenant_id="tenant-1",
        source="tokyodev",
        status="ok",
        fetched_count=2,
        imported_count=1,
        skipped_count=1,
        warnings=tuple(f"warning-{index}" for index in range(12)),
        budget_remaining=7,
    )
    status = external_seed_fetcher._load_status(redis, "tenant-1", "tokyodev")
    assert status["status"] == "ok"
    assert status["last_success_at"]
    assert len(status["warnings"]) == 10

    redis.setex(external_seed_fetcher._status_key("tenant-1", "tokyodev"), 60, "{bad-json")
    assert external_seed_fetcher._load_status(redis, "tenant-1", "tokyodev") == {"status": "invalid_status"}


def test_reserve_budget_ignores_invalid_interval_marker(monkeypatch) -> None:
    redis = _FakeRedis()
    redis.setex(
        f"external_seed_fetch:interval:{external_seed_fetcher._tenant_key('tenant-1')}:tokyodev",
        60,
        "not-a-float",
    )

    remaining, next_eligible_at = external_seed_fetcher._reserve_budget(
        redis,
        tenant_id="tenant-1",
        source="tokyodev",
        config=_config(),
    )

    assert remaining == 49
    assert next_eligible_at is not None


def test_import_jobs_streams_records_through_etl_service(monkeypatch) -> None:
    imported_records: list[NormalizedJobRecord] = []

    class FakeService:
        def __init__(self, ai_service):
            assert ai_service is None

        def import_record(self, repo, record):
            imported_records.append(record)

    class FakeUow:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    result = ExternalSeedFetchResult(
        source="tokyodev",
        fetched_at="2026-05-23T00:00:00+00:00",
        upstream_url=external_seed_fetcher.SOURCE_URLS["tokyodev"],
        jobs=(
            ExternalSeedJob(
                source_job_id="job-1",
                title="Platform Engineer",
                company_name="Acme",
                job_url="https://www.tokyodev.com/jobs/job-1",
            ),
        ),
        request_id="request-1",
    )
    monkeypatch.setattr(external_seed_fetcher, "JobETLService", FakeService)
    monkeypatch.setattr(external_seed_fetcher, "job_uow", lambda: FakeUow())

    assert external_seed_fetcher._import_jobs(result, tenant_id="tenant-1") == 1
    assert imported_records[0].source.provider == external_seed_fetcher.PROVIDER_NAME


def test_fetch_and_import_external_seed_source_validates_and_records_failures(monkeypatch) -> None:
    redis = _FakeRedis()
    statuses: list[dict[str, Any]] = []
    metrics: list[tuple[str, str]] = []
    monkeypatch.setattr(external_seed_fetcher, "_redis_or_error", lambda: redis)
    monkeypatch.setattr(external_seed_fetcher, "_record_metric", lambda source, outcome: metrics.append((source, outcome)))
    monkeypatch.setattr(
        external_seed_fetcher,
        "_record_status",
        lambda redis_client, **kwargs: statuses.append(kwargs),
    )

    with pytest.raises(ExternalSeedFetchError) as exc_info:
        fetch_and_import_external_seed_source("unknown", config=_config())
    assert exc_info.value.status_code == 404

    for config, expected_code in [
        (_config(enabled=False), "external_seed_disabled"),
        (_config(sources=("japandev",)), "external_seed_source_not_allowed"),
        (_config(secret=None), "external_seed_unconfigured"),
    ]:
        with pytest.raises(ExternalSeedFetchError) as exc_info:
            fetch_and_import_external_seed_source("tokyodev", config=config)
        assert exc_info.value.code == expected_code

    class FailingClient:
        def fetch_source(self, source: str, *, limit: int | None = None):
            raise ExternalSeedFetchError("external_seed_worker_error", "down", status_code=502, failure_class="worker_http_502")

    with pytest.raises(ExternalSeedFetchError):
        fetch_and_import_external_seed_source("tokyodev", config=_config(), client=FailingClient())

    assert metrics[-1] == ("tokyodev", "failed")
    assert statuses[-1]["status"] == "degraded"


def test_fetch_and_import_external_seed_source_returns_rate_limited_summary(monkeypatch) -> None:
    redis = _FakeRedis()
    metrics: list[tuple[str, str]] = []
    statuses: list[dict[str, Any]] = []
    monkeypatch.setattr(external_seed_fetcher, "_redis_or_error", lambda: redis)
    monkeypatch.setattr(external_seed_fetcher, "_reserve_budget", lambda *args, **kwargs: (None, "2026-05-24T00:00:00+00:00"))
    monkeypatch.setattr(external_seed_fetcher, "_record_metric", lambda source, outcome: metrics.append((source, outcome)))
    monkeypatch.setattr(external_seed_fetcher, "_record_status", lambda redis_client, **kwargs: statuses.append(kwargs))

    summary = fetch_and_import_external_seed_source("tokyodev", config=_config())

    assert summary.success is False
    assert summary.status == "rate_limited"
    assert summary.failure_class == "min_interval"
    assert metrics == [("tokyodev", "rate_limited")]
    assert statuses[0]["status"] == "rate_limited"


def test_external_seed_fetcher_status_merges_catalog_and_runtime_status(monkeypatch) -> None:
    redis = _FakeRedis()
    external_seed_fetcher._record_status(
        redis,
        tenant_id="tenant-1",
        source="tokyodev",
        status="ok",
        fetched_count=1,
    )
    monkeypatch.setattr(external_seed_fetcher, "get_external_seed_fetcher_config", lambda: _config(sources=("tokyodev",)))
    monkeypatch.setattr(external_seed_fetcher, "_redis_or_error", lambda: redis)

    status = external_seed_fetcher.get_external_seed_fetcher_status("tenant-1")

    assert status["enabled"] is True
    assert status["configured"] is True
    assert status["sources"]["tokyodev"]["status"] == "ok"
    assert status["sources"]["japandev"]["configured"] is False
