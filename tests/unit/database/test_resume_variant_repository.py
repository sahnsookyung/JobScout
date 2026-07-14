from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from database.repositories.resume_variant import ResumeVariantRepository


class _Result:
    def __init__(self, *, scalar=None, rows=None):
        self.scalar = scalar
        self.rows = list(rows or [])

    def scalar_one_or_none(self):
        return self.scalar

    def scalars(self):
        return iter(self.rows)


class _Db:
    def __init__(self, results=None):
        self.results = list(results or [])
        self.executed = []
        self.added = []
        self.flushed = 0

    def execute(self, stmt):
        self.executed.append(stmt)
        if self.results:
            return self.results.pop(0)
        return _Result()

    def add(self, value):
        self.added.append(value)

    def flush(self):
        self.flushed += 1


def test_resume_variant_repository_reads_visible_variants() -> None:
    variant = SimpleNamespace(id=uuid4())
    db = _Db(results=[_Result(scalar=variant), _Result(rows=[variant]), _Result(scalar=variant)])
    repo = ResumeVariantRepository(db)
    identity = {
        "owner_id": uuid4(),
        "tenant_id": uuid4(),
        "match_id": uuid4(),
        "template_key": "compact",
    }

    assert repo.get_for_owner(variant.id, owner_id=identity["owner_id"], tenant_id=identity["tenant_id"]) is variant
    assert repo.list_for_match(
        owner_id=identity["owner_id"],
        tenant_id=None,
        match_id=identity["match_id"],
        limit=10,
    ) == [variant]
    assert repo.find_current(identity) is variant
    assert len(db.executed) == 3


def test_resume_variant_repository_create_adds_and_flushes() -> None:
    db = _Db()
    repo = ResumeVariantRepository(db)

    variant = repo.create(
        {
            "owner_id": uuid4(),
            "match_id": uuid4(),
            "job_post_id": uuid4(),
            "resume_fingerprint": "resume-fp",
            "template_key": "compact",
            "generation_mode": "deterministic",
            "template_version": "test",
            "generator_version": "test",
            "renderer_version": "test",
            "evidence_policy_version": "test",
            "source_match_updated_at": "now",
            "source_match_calculated_at": "now",
            "source_job_content_hash": "job-hash",
            "source_resume_updated_at": "now",
            "source_resume_content_hash": "resume-hash",
            "content_json": {},
            "evidence_map": {},
            "warnings": [],
        }
    )

    assert db.added == [variant]
    assert db.flushed == 1


def test_resume_variant_repository_replaces_current_generated_payload() -> None:
    variant = SimpleNamespace(
        job_post_id=uuid4(),
        resume_fingerprint="old-resume-fp",
        content_json={"summary": "old"},
        evidence_map={"claim_count": 1},
        warnings=["old"],
    )
    db = _Db(results=[_Result(scalar=variant)])
    repo = ResumeVariantRepository(db)
    new_job_post_id = uuid4()

    replaced = repo.replace_current(
        {"owner_id": uuid4(), "tenant_id": None, "match_id": uuid4()},
        {
            "job_post_id": new_job_post_id,
            "resume_fingerprint": "new-resume-fp",
            "content_json": {"summary": "new"},
            "evidence_map": {"claim_count": 2},
            "warnings": [],
        },
    )

    assert replaced is variant
    assert variant.job_post_id == new_job_post_id
    assert variant.resume_fingerprint == "new-resume-fp"
    assert variant.content_json == {"summary": "new"}
    assert variant.evidence_map == {"claim_count": 2}
    assert variant.warnings == []
    assert db.flushed == 1


def test_resume_variant_repository_prunes_old_variants() -> None:
    keep_id = uuid4()
    stale_id = uuid4()
    db = _Db(results=[_Result(rows=[stale_id, keep_id, uuid4()])])
    repo = ResumeVariantRepository(db)

    assert repo.prune_scope(owner_id=uuid4(), tenant_id=uuid4(), keep_id=keep_id, max_variants=1) == 2
    assert len(db.executed) == 2
    assert db.flushed == 1


def test_resume_variant_repository_prune_noops_when_under_limit_or_only_keep_remains() -> None:
    keep_id = uuid4()
    repo_under_limit = ResumeVariantRepository(_Db(results=[_Result(rows=[keep_id])]))
    repo_only_keep = ResumeVariantRepository(_Db(results=[_Result(rows=[keep_id])]))

    assert repo_under_limit.prune_scope(owner_id=uuid4(), tenant_id=None, keep_id=keep_id, max_variants=2) == 0
    assert repo_only_keep.prune_scope(owner_id=uuid4(), tenant_id=None, keep_id=keep_id, max_variants=0) == 0
