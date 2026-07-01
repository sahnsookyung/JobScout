from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch
import uuid

from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.backend.dependencies import get_current_user, get_db
from web.backend.routers.jobs import (
    _blocker_sort_key,
    _embedding_blocker,
    _extraction_blocker,
    _is_retry_due,
    _is_stale,
    _job_inventory_filters,
    _job_inventory_item,
    _matching_blocker,
    _primary_source,
    _processing_blocker_item,
    _request_tenant_id,
    _tenant_filter,
    list_job_inventory,
    router,
)
from web.backend.services.cursors import MatchCursorCodec


class TestJobsRouter:
    @staticmethod
    def _current_user():
        return Mock(id="user-123")

    @staticmethod
    def _db_session():
        return Mock()

    def _client(self):
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_current_user] = self._current_user
        app.dependency_overrides[get_db] = self._db_session
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

    def test_get_jobs_rejects_invalid_job_status(self):
        client = self._client()

        response = client.get("/api/jobs", params={"job_status": "archived"})

        assert response.status_code == 422
        assert "Invalid job_status" in response.json()["detail"]

    def test_processing_blockers_returns_cursor_metadata(self):
        client = self._client()
        blockers = [
            {
                "job_id": str(uuid.uuid4()),
                "stage": "extraction",
                "blocker_code": "queued_too_long",
                "blocker_detail": "Queued too long.",
                "status": "queued",
                "attempts": 0,
                "retry_eligible": True,
            },
            {
                "job_id": str(uuid.uuid4()),
                "stage": "embedding",
                "blocker_code": "retryable_embedding",
                "blocker_detail": "Retryable.",
                "status": "failed_retryable",
                "attempts": 1,
                "retry_eligible": True,
            },
            {
                "job_id": str(uuid.uuid4()),
                "stage": "matching",
                "blocker_code": "matching_not_queued",
                "blocker_detail": "Not queued.",
                "status": "active",
                "attempts": 0,
                "retry_eligible": True,
            },
        ]

        with patch("web.backend.routers.jobs.list_processing_blockers", return_value=blockers) as list_blockers:
            response = client.get("/api/jobs/processing-blockers", params={"limit": 2})

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert data["has_more"] is True
        assert data["next_cursor"]
        assert list_blockers.call_args.kwargs["limit"] == 3

    def test_processing_blockers_accepts_compact_view(self):
        client = self._client()

        with patch("web.backend.routers.jobs.list_processing_blockers", return_value=[]) as list_blockers:
            response = client.get(
                "/api/jobs/processing-blockers",
                params={"view": "compact", "limit": 5},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["view"] == "compact"
        assert data["blockers"] == []
        assert list_blockers.call_args.kwargs["limit"] == 6

    def test_processing_blockers_rejects_invalid_view(self):
        client = self._client()

        response = client.get("/api/jobs/processing-blockers", params={"view": "tiny"})

        assert response.status_code == 422
        assert "Invalid view" in response.json()["detail"]

    def test_processing_blockers_rejects_invalid_stage_and_cursor(self):
        client = self._client()

        bad_stage = client.get("/api/jobs/processing-blockers", params={"stage": "parse"})
        bad_cursor = client.get(
            "/api/jobs/processing-blockers",
            params={"cursor": MatchCursorCodec.encode("jobs", offset=10)},
        )

        assert bad_stage.status_code == 422
        assert "Invalid stage" in bad_stage.json()["detail"]
        assert bad_cursor.status_code == 422
        assert "Cursor does not apply" in bad_cursor.json()["detail"]

    def test_processing_blockers_uses_cursor_offset(self):
        client = self._client()
        blockers = [
            {
                "job_id": str(uuid.uuid4()),
                "stage": "extraction",
                "blocker_code": f"blocker-{idx}",
                "blocker_detail": "Queued.",
                "status": "queued",
                "attempts": 0,
                "retry_eligible": True,
            }
            for idx in range(4)
        ]
        cursor = MatchCursorCodec.encode("processing_blockers", offset=1)

        with patch("web.backend.routers.jobs.list_processing_blockers", return_value=blockers) as list_blockers:
            response = client.get(
                "/api/jobs/processing-blockers",
                params={"limit": 2, "cursor": cursor},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["page_mode"] == "cursor"
        assert data["offset"] == 1
        assert data["count"] == 2
        assert data["has_more"] is True
        assert list_blockers.call_args.kwargs["limit"] == 4


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


def test_primary_source_prefers_active_then_most_recent_source():
    older = SimpleNamespace(site="older", job_url="old", is_active=True, last_seen_at=datetime(2026, 1, 1))
    newer = SimpleNamespace(site="newer", job_url="new", is_active=True, last_seen_at=datetime(2026, 1, 2))
    inactive_newest = SimpleNamespace(site="inactive", job_url="inactive", is_active=False, last_seen_at=datetime(2026, 1, 3))

    assert _primary_source(SimpleNamespace(sources=[])) is None
    assert _primary_source(SimpleNamespace(sources=[inactive_newest, older, newer])) is newer
    assert _primary_source(SimpleNamespace(sources=[inactive_newest])).site == "inactive"


def test_job_inventory_filters_cover_processing_states_and_search():
    assert len(_job_inventory_filters(tenant_id=None, job_status="active", processing_status="ready", search="  ai ")) == 5
    assert len(_job_inventory_filters(tenant_id=uuid.uuid4(), job_status="all", processing_status="extracted", search=None)) == 2
    assert len(_job_inventory_filters(tenant_id=None, job_status="all", processing_status="embedded", search=None)) == 2
    assert len(_job_inventory_filters(tenant_id=None, job_status="all", processing_status="pending_extraction", search=None)) == 3
    assert len(_job_inventory_filters(tenant_id=None, job_status="all", processing_status="pending_embedding", search=None)) == 3
    assert len(_job_inventory_filters(tenant_id=None, job_status="all", processing_status="failed", search=None)) == 2


def test_list_job_inventory_executes_count_and_page_queries():
    db = Mock()
    count_result = Mock()
    count_result.scalar_one.return_value = 2
    rows_result = Mock()
    rows_result.scalars.return_value.all.return_value = [
        SimpleNamespace(
            id=uuid.uuid4(),
            title="Backend Engineer",
            company="Example",
            location_text="Remote",
            is_remote=True,
            status="active",
            is_extracted=True,
            is_embedded=True,
            extraction_status="succeeded",
            embedding_status="succeeded",
            description_completeness=None,
            description_source=None,
            description_warning_code=None,
            sources=[],
            first_seen_at=None,
            last_seen_at=None,
            extraction_attempts=None,
            extraction_last_error=None,
            extraction_next_retry_at=None,
            embedding_attempts=None,
            embedding_last_error=None,
            embedding_next_retry_at=None,
        )
    ]
    db.execute.side_effect = [count_result, rows_result]

    jobs, total = list_job_inventory(
        db,
        tenant_id=None,
        job_status="all",
        processing_status="all",
        search=None,
        limit=10,
        offset=0,
    )

    assert total == 2
    assert jobs[0].description_completeness == "unknown"
    assert db.execute.call_count == 2


def test_compatibility_wrappers_delegate_to_processing_blocker_service():
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(minutes=30)
    job = SimpleNamespace(id=uuid.uuid4(), first_seen_at=now, last_seen_at=now)
    blocker = _processing_blocker_item(
        job,
        stage="matching",
        blocker_code="missing_embedding",
        blocker_detail="Missing embedding.",
        status="active",
        attempts=1,
        last_error="err",
        retry_eligible=False,
        last_attempt_at=now,
        next_retry_at=None,
    )

    assert _request_tenant_id(SimpleNamespace(state=SimpleNamespace(tenant_id=None), headers={})) is None
    assert _is_retry_due(now - timedelta(minutes=1), now=now) is True
    assert _is_stale(now - timedelta(hours=1), stale_cutoff=stale_cutoff) is True
    assert _tenant_filter(None) is not None
    assert _blocker_sort_key(blocker)
    assert blocker.blocker_code == "missing_embedding"
    assert _matching_blocker(
        SimpleNamespace(
                id=uuid.uuid4(),
                is_embedded=False,
                status="active",
                first_seen_at=now,
                last_seen_at=now,
            )
    ).stage == "matching"


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
