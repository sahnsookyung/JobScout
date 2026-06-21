from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch
import uuid

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.backend.dependencies import get_current_user, get_db
from web.backend.routers.jobs import _embedding_blocker, _extraction_blocker, _job_inventory_item, router


class TestJobsRouter:
    def _client(self):
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_current_user] = lambda: Mock(id="user-123")
        app.dependency_overrides[get_db] = lambda: Mock()
        return TestClient(app, raise_server_exceptions=True)

    def test_get_jobs_returns_paginated_inventory(self):
        client = self._client()
        item = {
            "job_id": str(uuid.uuid4()),
            "title": "Software Engineer",
            "company": "Ramp",
            "location": "New York, NY",
            "is_remote": True,
            "status": "active",
            "is_extracted": True,
            "is_embedded": True,
            "extraction_status": "succeeded",
            "embedding_status": "succeeded",
            "description_completeness": "full",
            "description_source": "ats.greenhouse",
        }

        with patch("web.backend.routers.jobs.list_job_inventory", return_value=([item], 12)) as list_jobs:
            response = client.get(
                "/api/jobs",
                params={
                    "job_status": "active",
                    "processing_status": "ready",
                    "search": "ramp",
                    "limit": 25,
                    "offset": 50,
                },
                headers={"X-Tenant-Id": "00000000-0000-4000-8000-000000000201"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["count"] == 1
        assert data["total"] == 12
        assert data["jobs"][0]["title"] == "Software Engineer"
        kwargs = list_jobs.call_args.kwargs
        assert str(kwargs["tenant_id"]) == "00000000-0000-4000-8000-000000000201"
        assert kwargs["job_status"] == "active"
        assert kwargs["processing_status"] == "ready"
        assert kwargs["search"] == "ramp"
        assert kwargs["limit"] == 25
        assert kwargs["offset"] == 50

    def test_get_jobs_rejects_invalid_tenant_header(self):
        client = self._client()

        response = client.get("/api/jobs", headers={"X-Tenant-Id": "not-a-uuid"})

        assert response.status_code == 400
        assert response.json()["detail"] == "X-Tenant-Id must be a UUID."

    def test_get_jobs_rejects_invalid_processing_status(self):
        client = self._client()

        response = client.get("/api/jobs", params={"processing_status": "queued_somewhere"})

        assert response.status_code == 422
        assert "Invalid processing_status" in response.json()["detail"]


def test_job_inventory_item_serializes_source_and_limited_errors():
    job_id = uuid.uuid4()
    source = SimpleNamespace(
        site="greenhouse",
        job_url="https://boards.greenhouse.io/example/jobs/1",
        is_active=True,
        last_seen_at=datetime(2026, 6, 18, tzinfo=timezone.utc),
    )
    job = SimpleNamespace(
        id=job_id,
        title="Backend Engineer",
        company="Example",
        location_text="Remote",
        is_remote=True,
        status="active",
        is_extracted=False,
        is_embedded=True,
        extraction_status="failed_retryable",
        embedding_status="succeeded",
        description_completeness="partial",
        description_source="seed",
        description_warning_code="truncated_by_ingest_cap",
        sources=[source],
        first_seen_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 6, 18, tzinfo=timezone.utc),
        extraction_attempts=3,
        extraction_last_error="x" * 400,
        extraction_next_retry_at=datetime(2026, 6, 19, tzinfo=timezone.utc),
        embedding_attempts=1,
        embedding_last_error=None,
        embedding_next_retry_at=None,
    )

    item = _job_inventory_item(job)

    assert item.job_id == str(job_id)
    assert item.source_site == "greenhouse"
    assert item.source_url == "https://boards.greenhouse.io/example/jobs/1"
    assert item.description_warning_code == "truncated_by_ingest_cap"
    assert item.extraction_last_error is not None
    assert len(item.extraction_last_error) == 240


def test_processing_blockers_include_queued_extraction_and_embedding_jobs():
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(minutes=30)
    job = SimpleNamespace(
        id=uuid.uuid4(),
        description="Full job description",
        is_extracted=False,
        is_embedded=False,
        extraction_status="queued",
        embedding_status="queued",
        extraction_attempts=0,
        embedding_attempts=0,
        extraction_last_error=None,
        embedding_last_error=None,
        extraction_last_attempt_at=None,
        embedding_last_attempt_at=None,
        extraction_next_retry_at=None,
        embedding_next_retry_at=None,
        first_seen_at=now,
        last_seen_at=now,
    )

    extraction = _extraction_blocker(job, now=now, stale_cutoff=stale_cutoff)
    job.is_extracted = True
    embedding = _embedding_blocker(job, now=now, stale_cutoff=stale_cutoff)

    assert extraction is not None
    assert extraction.blocker_code == "pending_queue"
    assert extraction.status == "queued"
    assert "queued for extraction" in extraction.blocker_detail
    assert embedding is not None
    assert embedding.blocker_code == "pending_queue"
    assert embedding.status == "queued"
    assert "queued for embedding" in embedding.blocker_detail

def test_processing_blockers_classify_stale_and_retryable_states():
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(minutes=30)
    job = SimpleNamespace(
        id=uuid.uuid4(),
        description="Full job description",
        is_extracted=False,
        is_embedded=False,
        extraction_status="in_progress",
        embedding_status="failed_retryable",
        extraction_attempts=1,
        embedding_attempts=2,
        extraction_last_error="worker disappeared",
        embedding_last_error="provider timeout",
        extraction_last_attempt_at=now - timedelta(hours=2),
        embedding_last_attempt_at=now - timedelta(minutes=5),
        extraction_next_retry_at=None,
        embedding_next_retry_at=now - timedelta(minutes=1),
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now,
    )

    extraction = _extraction_blocker(job, now=now, stale_cutoff=stale_cutoff)
    job.is_extracted = True
    embedding = _embedding_blocker(job, now=now, stale_cutoff=stale_cutoff)

    assert extraction is not None
    assert extraction.blocker_code == "stale_extraction"
    assert extraction.retry_eligible is True
    assert embedding is not None
    assert embedding.blocker_code == "retryable_embedding"
    assert embedding.retry_eligible is True

def test_processing_blockers_classify_queued_too_long_and_terminal_failures():
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(minutes=30)
    job = SimpleNamespace(
        id=uuid.uuid4(),
        description="Full job description",
        is_extracted=False,
        is_embedded=False,
        extraction_status="queued",
        embedding_status="failed_terminal",
        extraction_attempts=0,
        embedding_attempts=3,
        extraction_last_error=None,
        embedding_last_error="invalid payload",
        extraction_last_attempt_at=now - timedelta(hours=1),
        embedding_last_attempt_at=now - timedelta(hours=2),
        extraction_next_retry_at=None,
        embedding_next_retry_at=None,
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now,
    )

    extraction = _extraction_blocker(job, now=now, stale_cutoff=stale_cutoff)
    job.is_extracted = True
    embedding = _embedding_blocker(job, now=now, stale_cutoff=stale_cutoff)

    assert extraction is not None
    assert extraction.blocker_code == "queued_too_long"
    assert embedding is not None
    assert embedding.blocker_code == "non_retryable_failure"
    assert embedding.retry_eligible is False
