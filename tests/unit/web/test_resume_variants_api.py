from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from web.backend.app import create_app
from web.backend.dependencies import get_current_user, get_db


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
