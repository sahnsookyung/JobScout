from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

from core.resume_variants.quota import (
    ResumeVariantConcurrencyError,
    ResumeVariantQuotaExceeded,
    ResumeVariantQuotaUnavailable,
)
from core.resume_variants.service import (
    ResumeVariantConflict,
    ResumeVariantNotFound,
    ResumeVariantValidationError,
)
from web.backend.app import create_app
from web.backend.dependencies import get_current_user, get_db
from web.backend.routers import resume_variants


def _client(monkeypatch, fake_service) -> TestClient:
    from web.backend.routers import resume_variants

    app = create_app()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=uuid4())
    app.dependency_overrides[get_db] = lambda: iter([SimpleNamespace()])
    monkeypatch.setattr(resume_variants, "_service", lambda db: fake_service)
    return TestClient(app, raise_server_exceptions=False)


class _FakeService:
    def __init__(self, variant=None) -> None:
        self.variant = variant or _variant()
        self.create_calls = 0
        self.create_kwargs = None
        self.get_kwargs = None
        self.list_kwargs = None

    def create_for_match(self, **kwargs):
        self.create_calls += 1
        self.create_kwargs = kwargs
        return SimpleNamespace(variant=self.variant, reused=False, quota_status={"daily_remaining": 9, "hourly_remaining": 2})

    def get_variant(self, **kwargs):
        self.get_kwargs = kwargs
        return self.variant

    def list_for_match(self, **kwargs):
        self.list_kwargs = kwargs
        return [self.variant]


def _variant():
    return SimpleNamespace(
        id=uuid4(),
        match_id=uuid4(),
        job_post_id=uuid4(),
        template_key="compact",
        generation_mode="deterministic",
        created_at=datetime.now(timezone.utc),
        content_json={
            "job": {"title": '<img src=x onerror="alert(1)">'},
            "summary": [
                {
                    "text": "<script>alert(1)</script> Python engineer",
                    "sources": [{"kind": "structured_resume", "path": "profile.summary.text"}],
                }
            ],
            "skills": [],
            "targeted_evidence": [],
            "experience": [],
        },
        evidence_map={"claim_count": 1},
        warnings=[],
    )


@pytest.mark.security
def test_create_rejects_client_protected_fields(monkeypatch) -> None:
    client = _client(monkeypatch, _FakeService())

    response = client.post(
        f"/api/matches/{uuid4()}/resume-variants",
        json={"template_key": "compact", "owner_id": str(uuid4())},
    )

    assert response.status_code == 422


def test_create_accepts_valid_forwarded_tenant_header(monkeypatch) -> None:
    fake_service = _FakeService()
    client = _client(monkeypatch, fake_service)
    tenant_id = uuid4()

    response = client.post(
        f"/api/matches/{uuid4()}/resume-variants",
        json={"template_key": "compact"},
        headers={"X-Tenant-Id": str(tenant_id)},
    )

    assert response.status_code == 200
    assert fake_service.create_kwargs["tenant_id"] == tenant_id


@pytest.mark.security
def test_invalid_tenant_header_is_rejected(monkeypatch) -> None:
    fake_service = _FakeService()
    client = _client(monkeypatch, fake_service)

    response = client.post(
        f"/api/matches/{uuid4()}/resume-variants",
        json={"template_key": "compact"},
        headers={"X-Tenant-Id": "not-a-uuid"},
    )

    assert response.status_code == 400
    assert response.json()["error"] == "X-Tenant-Id must be a UUID."
    assert fake_service.create_calls == 0


@pytest.mark.security
def test_download_rechecks_auth_and_streams_safe_html(monkeypatch) -> None:
    variant = _variant()
    client = _client(monkeypatch, _FakeService(variant))

    response = client.get(f"/api/resume-variants/{variant.id}/download?format=html")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["content-disposition"].startswith("attachment; filename=\"resume-variant-")
    assert "<script>" not in response.text
    assert "<img" not in response.text
    assert "&lt;script&gt;" in response.text


def test_list_requires_bounded_limit(monkeypatch) -> None:
    client = _client(monkeypatch, _FakeService())

    response = client.get(f"/api/resume-variants?match_id={uuid4()}&limit=5000")

    assert response.status_code == 422

def test_get_and_list_resume_variants_pass_tenant_context(monkeypatch) -> None:
    variant = _variant()
    fake_service = _FakeService(variant)
    client = _client(monkeypatch, fake_service)
    tenant_id = uuid4()

    get_response = client.get(
        f"/api/resume-variants/{variant.id}",
        headers={"X-Tenant-Id": str(tenant_id)},
    )
    list_response = client.get(
        f"/api/resume-variants?match_id={variant.match_id}&limit=3",
        headers={"X-Tenant-Id": str(tenant_id)},
    )

    assert get_response.status_code == 200
    assert list_response.status_code == 200
    assert fake_service.get_kwargs == {
        "variant_id": variant.id,
        "owner_id": fake_service.get_kwargs["owner_id"],
        "tenant_id": tenant_id,
    }
    assert fake_service.list_kwargs == {
        "match_id": variant.match_id,
        "owner_id": fake_service.list_kwargs["owner_id"],
        "tenant_id": tenant_id,
        "limit": 3,
    }
    assert list_response.json()["count"] == 1

@pytest.mark.security
def test_get_and_list_reject_invalid_uuid_inputs(monkeypatch) -> None:
    fake_service = _FakeService()
    client = _client(monkeypatch, fake_service)

    get_response = client.get("/api/resume-variants/not-a-uuid")
    list_response = client.get("/api/resume-variants?match_id=not-a-uuid")

    assert get_response.status_code == 400
    assert list_response.status_code == 400
    assert fake_service.get_kwargs is None
    assert fake_service.list_kwargs is None

@pytest.mark.security
def test_request_tenant_id_prefers_trusted_state_and_validates_it() -> None:
    tenant_id = uuid4()
    request = Request(
        {
            "type": "http",
            "headers": [(b"x-tenant-id", str(uuid4()).encode("ascii"))],
            "method": "GET",
            "path": "/",
        }
    )
    request.state.tenant_id = str(tenant_id)

    assert resume_variants._request_tenant_id(request) == tenant_id

    request.state.tenant_id = "not-a-uuid"
    with pytest.raises(HTTPException) as exc_info:
        resume_variants._request_tenant_id(request)
    assert exc_info.value.status_code == 400

def test_request_tenant_id_returns_none_without_header() -> None:
    request = Request({"type": "http", "headers": [], "method": "GET", "path": "/"})

    assert resume_variants._request_tenant_id(request) is None

def test_download_streams_markdown_and_docx(monkeypatch) -> None:
    variant = _variant()
    client = _client(monkeypatch, _FakeService(variant))

    markdown_response = client.get(f"/api/resume-variants/{variant.id}/download?format=markdown")
    docx_response = client.get(f"/api/resume-variants/{variant.id}/download?format=docx")

    assert markdown_response.status_code == 200
    assert markdown_response.headers["content-type"].startswith("text/markdown")
    assert markdown_response.headers["content-disposition"].endswith('.md"')
    assert b"Python engineer" in markdown_response.content
    assert docx_response.status_code == 200
    assert docx_response.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    assert docx_response.headers["content-disposition"].endswith('.docx"')
    assert docx_response.content.startswith(b"PK")

def test_download_maps_renderer_size_errors(monkeypatch) -> None:
    variant = _variant()
    client = _client(monkeypatch, _FakeService(variant))

    def _raise_too_large(self, content):
        del self, content
        raise ValueError("rendered output too large")

    monkeypatch.setattr(resume_variants.ResumeVariantRenderer, "render_markdown", _raise_too_large)

    response = client.get(f"/api/resume-variants/{variant.id}/download?format=markdown")

    assert response.status_code == 413
    assert response.json()["error"] == "rendered output too large"

@pytest.mark.parametrize(
    ("exception", "status_code"),
    [
        (ResumeVariantNotFound("missing"), 404),
        (ResumeVariantConflict("conflict"), 409),
        (ResumeVariantValidationError("too large"), 413),
        (ResumeVariantConcurrencyError("busy"), 409),
        (ResumeVariantQuotaUnavailable("redis down"), 503),
    ],
)
def test_run_service_call_maps_domain_errors(exception: Exception, status_code: int) -> None:
    with pytest.raises(HTTPException) as exc_info:
        resume_variants._run_service_call(lambda: (_ for _ in ()).throw(exception))

    assert exc_info.value.status_code == status_code
    assert exc_info.value.detail == str(exception)

def test_run_service_call_maps_quota_retry_header() -> None:
    with pytest.raises(HTTPException) as exc_info:
        resume_variants._run_service_call(
            lambda: (_ for _ in ()).throw(ResumeVariantQuotaExceeded("hourly", retry_after=17))
        )

    assert exc_info.value.status_code == 429
    assert exc_info.value.headers == {"Retry-After": "17"}

def test_run_service_call_maps_quota_without_retry_header() -> None:
    with pytest.raises(HTTPException) as exc_info:
        resume_variants._run_service_call(lambda: (_ for _ in ()).throw(ResumeVariantQuotaExceeded("daily")))

    assert exc_info.value.status_code == 429
    assert exc_info.value.headers == {}
