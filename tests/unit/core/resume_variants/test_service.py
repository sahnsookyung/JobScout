from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from core.resume_variants.service import ResumeVariantNotFound, ResumeVariantRequest, ResumeVariantService


class _QuotaShouldNotRun:
    def lease(self, owner_id: str):
        del owner_id
        raise AssertionError("fresh reuse must not acquire generation quota")


class _RepoWithExisting:
    def __init__(self, existing):
        self.existing = existing

    def find_current(self, identity):
        del identity
        return self.existing


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _FakeDb:
    def __init__(self, *, job=None, resume=None):
        self.job = job
        self.resume = resume

    def get(self, model, key):
        del model, key
        return self.job

    def execute(self, statement):
        del statement
        return _Result(self.resume)


class _RepoWithList:
    def __init__(self):
        self.calls = []

    def list_for_match(self, **kwargs):
        self.calls.append(kwargs)
        return ["variant"]


@pytest.mark.concurrency
def test_fresh_variant_reuse_does_not_consume_quota_or_lock() -> None:
    now = datetime.now(timezone.utc)
    existing = SimpleNamespace(id=uuid4())
    service = ResumeVariantService(SimpleNamespace(), quota=_QuotaShouldNotRun())
    service.repo = _RepoWithExisting(existing)
    match = SimpleNamespace(
        id=uuid4(),
        job_post_id=uuid4(),
        resume_fingerprint="resume-fp",
        status="active",
        updated_at=now,
        calculated_at=now,
        job_content_hash="job-hash",
    )
    job = SimpleNamespace(content_hash="job-hash")
    resume = SimpleNamespace(extracted_data={"profile": {}}, updated_at=now, created_at=now)
    service._load_sources = lambda **kwargs: (match, job, resume)

    result = service.create_for_match(
        match_id=match.id,
        owner_id=uuid4(),
        tenant_id=None,
        request=ResumeVariantRequest(force=False),
    )

    assert result.variant is existing
    assert result.reused is True


@pytest.mark.security
def test_load_sources_rejects_cross_tenant_job(monkeypatch) -> None:
    tenant_id = uuid4()
    other_tenant_id = uuid4()
    match = SimpleNamespace(
        id=uuid4(),
        job_post_id=uuid4(),
        resume_fingerprint="resume-fp",
        status="active",
    )
    job = SimpleNamespace(tenant_id=other_tenant_id)

    class _MatchRepo:
        def __init__(self, db):
            del db

        def get_match_by_id_for_owner(self, match_id, owner_id):
            del match_id, owner_id
            return match

    monkeypatch.setattr("core.resume_variants.service.MatchRepository", _MatchRepo)
    service = ResumeVariantService(_FakeDb(job=job))

    with pytest.raises(ResumeVariantNotFound):
        service._load_sources(match_id=match.id, owner_id=uuid4(), tenant_id=tenant_id)


def test_list_for_match_allows_stale_match_history(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    owner_id = uuid4()
    match_id = uuid4()
    tenant_id = uuid4()
    match = SimpleNamespace(
        id=match_id,
        job_post_id=uuid4(),
        resume_fingerprint="resume-fp",
        status="stale",
    )
    job = SimpleNamespace(tenant_id=tenant_id)
    resume = SimpleNamespace(extracted_data={"profile": {}}, updated_at=now, created_at=now)

    class _MatchRepo:
        def __init__(self, db):
            del db

        def get_match_by_id_for_owner(self, requested_match_id, requested_owner_id):
            assert requested_match_id == match_id
            assert requested_owner_id == owner_id
            return match

    monkeypatch.setattr("core.resume_variants.service.MatchRepository", _MatchRepo)
    service = ResumeVariantService(_FakeDb(job=job, resume=resume))
    repo = _RepoWithList()
    service.repo = repo

    assert service.list_for_match(
        match_id=match_id,
        owner_id=owner_id,
        tenant_id=tenant_id,
    ) == ["variant"]
    assert repo.calls[0]["match_id"] == match_id
