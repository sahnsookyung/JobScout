from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch
import uuid

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.backend.dependencies import get_current_user, get_db
from web.backend.routers.jobs import _job_inventory_item, router


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
