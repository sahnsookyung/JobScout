"""Exercise generation isolation and backfill checkpoints on real pgvector."""

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from database.models import JobPost, JobRequirementUnit, JobRequirementUnitEmbedding, JobMatch, StructuredResume
from database.models.resume import ResumeSectionEmbedding, ResumeEvidenceUnitEmbedding, SYSTEM_OWNER_ID
from database.repositories.job_post import JobPostRepository
from database.repositories.match import MatchRepository
from database.repositories.resume import ResumeRepository
from scripts.migrate_embedding_space import BackfillItem, collect_items, persist_items, backfill_resumes, migration_status
from core.llm_evaluation import MatchLlmEvaluationService
from core.resume_variants.service import ResumeVariantService

pytestmark = [pytest.mark.integration, pytest.mark.db]
VECTOR = [1.0] + [0.0]*1023
QUERY = [0.0, 1.0] + [0.0]*1022


@pytest.fixture
def space_db(test_database, monkeypatch):
    for module in ("database.repositories.job_post", "database.repositories.match", "database.repositories.resume",
                   "database.repositories.embedding_contract", "scripts.migrate_embedding_space"):
        monkeypatch.setattr(module + ".current_embedding_space", lambda: "test-new-space")
    engine = create_engine(test_database)
    yield engine
    engine.dispose()


def test_partial_catalog_backfill_never_mixes_spaces_and_skips_changed_text(space_db):
    marker = str(uuid.uuid4())
    with Session(space_db) as session, session.begin():
        job = JobPost(title="Synthetic role", company="Fixture", canonical_fingerprint=marker,
                      is_extracted=True, is_embedded=True, embedding_status="succeeded",
                      summary_embedding=VECTOR, embedding_space="legacy", content_hash=marker)
        session.add(job)
        session.flush()
        req = JobRequirementUnit(job_post_id=job.id, req_type="required", text="Python")
        session.add(req)
        session.flush()
        session.add(JobRequirementUnitEmbedding(job_requirement_unit_id=req.id, embedding=VECTOR))
        job_id, req_id = job.id, req.id
    try:
        items = [item for item in collect_items(space_db, "requirements", 10000) if item.id == req_id]
        with Session(space_db) as session, session.begin():
            session.get(JobRequirementUnit, req_id).text = "Python and SQL"
        assert persist_items(space_db, "requirements", items, [QUERY]) == {"persisted": 0, "changed_or_deleted": 1}
        with Session(space_db) as session, session.begin():
            job = session.get(JobPost, job_id)
            JobPostRepository(session).save_job_embedding(job, VECTOR)
            assert job_id not in {row.id for row, score in JobPostRepository(session).get_top_jobs_by_summary_embedding(QUERY)}
        updated = [BackfillItem(req_id, "Python and SQL")]
        assert persist_items(space_db, "requirements", updated, [QUERY])["persisted"] == 1
        assert persist_items(space_db, "requirements", updated, [QUERY])["persisted"] == 1
        with Session(space_db) as session, session.begin():
            assert job_id in {row.id for row, score in JobPostRepository(session).get_top_jobs_by_summary_embedding(QUERY)}
            session.add(JobMatch(owner_id=SYSTEM_OWNER_ID, job_post_id=job_id,
                                 resume_fingerprint=marker, job_content_hash=marker, embedding_space="legacy", notified=True))
            session.flush()
            assert MatchRepository(session).count_reusable_matches_for_resume(marker) == 0
            match = session.scalar(select(JobMatch).where(JobMatch.resume_fingerprint == marker))
            match.embedding_space = "test-new-space"
            session.flush()
            assert MatchRepository(session).count_reusable_matches_for_resume(marker) == 1
        assert req_id not in {item.id for item in collect_items(space_db, "requirements", 10000)}
        assert job_id not in {item.id for item in collect_items(space_db, "jobs", 10000)}
    finally:
        with Session(space_db) as session, session.begin():
            session.delete(session.get(JobPost, job_id))


def test_resume_backfill_keeps_legacy_vectors_and_reads_query_vector(space_db, monkeypatch):
    monkeypatch.setattr("core.llm_evaluation.current_embedding_space", lambda: "test-new-space")
    monkeypatch.setattr("core.resume_variants.service.current_embedding_space", lambda: "test-new-space")
    marker = str(uuid.uuid4())
    section = dict(section_type="summary", section_index=0, source_text="Synthetic engineer",
                   source_data={}, embedding=VECTOR)
    evidence = dict(evidence_unit_id="synthetic", source_text="Python", source_section="skills", embedding=VECTOR)
    with Session(space_db) as session, session.begin():
        session.add(ResumeSectionEmbedding(resume_fingerprint=marker, owner_id=SYSTEM_OWNER_ID, **section))
        session.add(ResumeEvidenceUnitEmbedding(resume_fingerprint=marker, owner_id=SYSTEM_OWNER_ID, **evidence))
    try:
        with Session(space_db) as session, session.begin():
            repo = ResumeRepository(session)
            assert repo.get_resume_summary_embedding(marker) is None
            repo.save_resume_section_embeddings(marker, [{**section, "query_embedding": QUERY}])
            repo.save_evidence_unit_embeddings(marker, [evidence])
            session.flush()
            assert repo.get_resume_summary_embedding(marker) == QUERY
            assert len(repo.get_resume_section_embeddings(marker)) == 1
            assert len(repo.find_best_evidence_for_requirement(QUERY, marker, top_k=10)) == 1
            judge = object.__new__(MatchLlmEvaluationService)
            judge.db = session
            judge.config = SimpleNamespace(matching=SimpleNamespace(llm_judge=SimpleNamespace()))
            variants = object.__new__(ResumeVariantService)
            variants.db = session
            assert len(judge._load_resume_evidence_units(SYSTEM_OWNER_ID, marker)) == 1
            assert len(variants._resume_evidence_units(SYSTEM_OWNER_ID, marker)) == 1
            for model in (ResumeSectionEmbedding, ResumeEvidenceUnitEmbedding):
                rows = session.scalars(select(model).where(model.resume_fingerprint == marker)).all()
                assert {row.embedding_space for row in rows} == {"legacy", "test-new-space"}
    finally:
        with Session(space_db) as session, session.begin():
            for model in (ResumeSectionEmbedding, ResumeEvidenceUnitEmbedding):
                for row in session.scalars(select(model).where(model.resume_fingerprint == marker)):
                    session.delete(row)


def test_outdated_resume_is_reported_without_reembedding_or_promoting_version(space_db, monkeypatch):
    marker = str(uuid.uuid4())
    baseline = migration_status(space_db)
    with Session(space_db) as session, session.begin():
        # Deliberately unparseable with the current schema: an old upload needs re-extraction.
        row = StructuredResume(owner_id=SYSTEM_OWNER_ID, resume_fingerprint=marker,
                               fingerprint_version=1, extracted_data={"profile": {}})
        session.add(row)
        session.flush()
        row_id = row.id
    def unexpected_provider(*args, **kwargs):
        pytest.fail("Outdated resumes must not be sent to an embedding provider")
    monkeypatch.setattr("scripts.migrate_embedding_space.build_llm_provider", unexpected_provider)
    try:
        assert backfill_resumes(space_db, None) == 0
        status = migration_status(space_db)
        assert status["outdated_resumes"] == baseline["outdated_resumes"] + 1
        assert status["resumes_pending"] == baseline["resumes_pending"]
        with Session(space_db) as session:
            stored = session.get(StructuredResume, row_id)
            assert stored.fingerprint_version == 1
            assert stored.extracted_data == {"profile": {}}
    finally:
        with Session(space_db) as session, session.begin():
            session.delete(session.get(StructuredResume, row_id))
