from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from core.resume_variants.service import (
    ResumeVariantConflict,
    ResumeVariantNotFound,
    ResumeVariantRequest,
    ResumeVariantService,
    ResumeVariantValidationError,
    content_size,
    variant_to_response,
)


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


class _Lease:
    status = SimpleNamespace(daily_remaining=3, hourly_remaining=1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Quota:
    def __init__(self):
        self.owner_keys = []

    def lease(self, owner_id: str):
        self.owner_keys.append(owner_id)
        return _Lease()


class _RepoForCreate:
    def __init__(self, variant):
        self.variant = variant
        self.created_values = None
        self.pruned = None

    def find_current(self, identity):
        return None

    def create(self, values):
        self.created_values = values
        return self.variant

    def prune_scope(self, **kwargs):
        self.pruned = kwargs
        return 0


class _RepoWithIntegrityConflict:
    def __init__(self, existing):
        self.existing = existing

    def find_current(self, identity):
        return self.existing

    def create(self, values):
        raise IntegrityError("insert", {}, Exception("duplicate"))

    def prune_scope(self, **kwargs):
        raise AssertionError("prune is skipped after duplicate insert")


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


def test_create_for_match_generates_persists_and_reports_quota(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    owner_id = uuid4()
    tenant_id = uuid4()
    variant = SimpleNamespace(
        id=uuid4(),
        match_id=uuid4(),
        job_post_id=uuid4(),
        template_key="compact",
        generation_mode="deterministic",
        created_at=now,
        content_json={"summary": []},
        evidence_map={},
        warnings=[],
    )
    match = SimpleNamespace(
        id=variant.match_id,
        job_post_id=variant.job_post_id,
        resume_fingerprint="resume-fp",
        status="active",
        updated_at=now,
        calculated_at=now,
        job_content_hash="job-hash",
    )
    job = SimpleNamespace(content_hash="job-hash")
    resume = SimpleNamespace(extracted_data={"profile": {"name": "Soo"}}, updated_at=now, created_at=now)
    service = ResumeVariantService(SimpleNamespace(commit=lambda: None), quota=_Quota())
    repo = _RepoForCreate(variant)
    service.repo = repo
    service._load_sources = lambda **kwargs: (match, job, resume)
    service._requirement_matches = lambda match_id: ["requirement"]
    monkeypatch.setattr(
        "core.resume_variants.service.generate_resume_variant_content",
        lambda **kwargs: (variant.content_json, {"summary": []}, ["warning"]),
    )

    result = service.create_for_match(
        match_id=match.id,
        owner_id=owner_id,
        tenant_id=tenant_id,
        request=ResumeVariantRequest(tone="direct"),
    )

    assert result.variant is variant
    assert result.reused is False
    assert result.quota_status == {"daily_remaining": 3, "hourly_remaining": 1}
    assert repo.created_values["owner_id"] == owner_id
    assert repo.created_values["warnings"] == ["warning"]
    assert repo.pruned == {"owner_id": owner_id, "tenant_id": tenant_id, "keep_id": variant.id}


def test_create_for_match_passes_resume_evidence_units_to_generator(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    owner_id = uuid4()
    variant = SimpleNamespace(
        id=uuid4(),
        match_id=uuid4(),
        job_post_id=uuid4(),
        template_key="compact",
        generation_mode="deterministic",
        created_at=now,
        content_json={"summary": []},
        evidence_map={},
        warnings=[],
    )
    match = SimpleNamespace(
        id=variant.match_id,
        job_post_id=variant.job_post_id,
        resume_fingerprint="resume-fp",
        status="active",
        updated_at=now,
        calculated_at=now,
        job_content_hash="job-hash",
    )
    job = SimpleNamespace(content_hash="job-hash")
    resume = SimpleNamespace(extracted_data={"profile": {}}, updated_at=now, created_at=now)
    evidence_units = [SimpleNamespace(source_text="Built TypeScript UI.")]
    captured = {}
    service = ResumeVariantService(SimpleNamespace(commit=lambda: None), quota=_Quota())
    repo = _RepoForCreate(variant)
    service.repo = repo
    service._load_sources = lambda **kwargs: (match, job, resume)
    service._requirement_matches = lambda match_id: []
    service._resume_evidence_units = lambda requested_owner_id, resume_fingerprint: evidence_units

    def _capture_generator(**kwargs):
        captured.update(kwargs)
        return variant.content_json, {"summary": []}, []

    monkeypatch.setattr("core.resume_variants.service.generate_resume_variant_content", _capture_generator)

    service.create_for_match(
        match_id=match.id,
        owner_id=owner_id,
        tenant_id=None,
        request=ResumeVariantRequest(force=True),
    )

    assert captured["resume_evidence_units"] is evidence_units
    assert captured["resume_data"] == resume.extracted_data


def test_create_for_match_reuses_current_variant_after_unique_race(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    existing = SimpleNamespace(id=uuid4())
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
    db = SimpleNamespace(commit=lambda: None, rollback=lambda: None)
    service = ResumeVariantService(db, quota=_Quota())
    service.repo = _RepoWithIntegrityConflict(existing)
    service._load_sources = lambda **kwargs: (match, job, resume)
    service._requirement_matches = lambda match_id: []
    monkeypatch.setattr(
        "core.resume_variants.service.generate_resume_variant_content",
        lambda **kwargs: ({"summary": []}, {}, []),
    )

    result = service.create_for_match(
        match_id=match.id,
        owner_id=uuid4(),
        tenant_id=None,
        request=ResumeVariantRequest(force=True),
    )

    assert result.variant is existing
    assert result.reused is True


def test_get_variant_raises_not_found_for_hidden_variant() -> None:
    service = ResumeVariantService(SimpleNamespace())
    service.repo = SimpleNamespace(get_for_owner=lambda *args, **kwargs: None)

    with pytest.raises(ResumeVariantNotFound):
        service.get_variant(variant_id=uuid4(), owner_id=uuid4(), tenant_id=None)


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
        def __init__(self, _db):
            pass

        def get_match_by_id_for_owner(self, match_id, owner_id):
            return match

    monkeypatch.setattr("core.resume_variants.service.MatchRepository", _MatchRepo)
    service = ResumeVariantService(_FakeDb(job=job))

    with pytest.raises(ResumeVariantNotFound):
        service._load_sources(match_id=match.id, owner_id=uuid4(), tenant_id=tenant_id)


def test_load_sources_rejects_missing_inactive_or_unowned_sources(monkeypatch) -> None:
    owner_id = uuid4()
    match_id = uuid4()

    class _MissingMatchRepo:
        def __init__(self, _db):
            pass

        def get_match_by_id_for_owner(self, requested_match_id, requested_owner_id):
            return None

    monkeypatch.setattr("core.resume_variants.service.MatchRepository", _MissingMatchRepo)
    service = ResumeVariantService(_FakeDb())
    with pytest.raises(ResumeVariantNotFound):
        service._load_sources(match_id=match_id, owner_id=owner_id, tenant_id=None)

    inactive_match = SimpleNamespace(
        id=match_id,
        job_post_id=uuid4(),
        resume_fingerprint="resume-fp",
        status="hidden",
    )

    class _InactiveMatchRepo:
        def __init__(self, _db):
            pass

        def get_match_by_id_for_owner(self, requested_match_id, requested_owner_id):
            return inactive_match

    monkeypatch.setattr("core.resume_variants.service.MatchRepository", _InactiveMatchRepo)
    with pytest.raises(ResumeVariantConflict):
        ResumeVariantService(_FakeDb())._load_sources(match_id=match_id, owner_id=owner_id, tenant_id=None)

    active_match = SimpleNamespace(
        id=match_id,
        job_post_id=uuid4(),
        resume_fingerprint="resume-fp",
        status="active",
    )

    class _ActiveMatchRepo:
        def __init__(self, _db):
            pass

        def get_match_by_id_for_owner(self, requested_match_id, requested_owner_id):
            return active_match

    monkeypatch.setattr("core.resume_variants.service.MatchRepository", _ActiveMatchRepo)
    with pytest.raises(ResumeVariantNotFound):
        ResumeVariantService(_FakeDb(job=None))._load_sources(match_id=match_id, owner_id=owner_id, tenant_id=None)
    with pytest.raises(ResumeVariantNotFound):
        ResumeVariantService(_FakeDb(job=SimpleNamespace(tenant_id=uuid4())))._load_sources(
            match_id=match_id,
            owner_id=owner_id,
            tenant_id=None,
        )
    with pytest.raises(ResumeVariantConflict):
        ResumeVariantService(_FakeDb(job=SimpleNamespace(tenant_id=None), resume=None))._load_sources(
            match_id=match_id,
            owner_id=owner_id,
            tenant_id=None,
        )


def test_identity_requires_freshness_timestamps() -> None:
    service = ResumeVariantService(SimpleNamespace())
    now = datetime.now(timezone.utc)

    with pytest.raises(ResumeVariantConflict):
        service._identity(
            owner_id=uuid4(),
            tenant_id=None,
            match=SimpleNamespace(
                id=uuid4(),
                updated_at=None,
                calculated_at=None,
                job_content_hash=None,
            ),
            job=SimpleNamespace(content_hash="job-hash"),
            resume=SimpleNamespace(extracted_data={}, updated_at=now, created_at=now),
            request=ResumeVariantRequest(),
        )


def test_variant_response_and_size_helpers_include_optional_fields() -> None:
    now = datetime.now(timezone.utc)
    variant = SimpleNamespace(
        id=uuid4(),
        match_id=uuid4(),
        job_post_id=uuid4(),
        template_key="compact",
        generation_mode="deterministic",
        created_at=now,
        content_json={"summary": []},
        evidence_map={"summary": []},
        warnings=["low evidence"],
    )

    response = variant_to_response(
        variant,
        reused=False,
        quota_status={"daily_remaining": 1, "hourly_remaining": 0},
    )

    assert response["reused"] is False
    assert response["quota_status"]["daily_remaining"] == 1
    assert response["download_formats"] == ["markdown", "html", "docx"]
    assert content_size({"created_at": now}) > 0


def test_validate_size_rejects_oversized_content(monkeypatch) -> None:
    service = ResumeVariantService(SimpleNamespace())
    monkeypatch.setattr("core.resume_variants.service.canonical_json_bytes", lambda content: b"x" * 100_000)
    monkeypatch.setattr("core.resume_variants.service.MAX_CONTENT_JSON_BYTES", 10)

    with pytest.raises(ResumeVariantValidationError):
        service._validate_size({"summary": []})


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
        def __init__(self, _db):
            pass

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
