"""Tests for job_post extraction/embedding stage status semantics.

TestJobPostStageQueries previously inspected SQL string content to verify
column names appeared in query text.  That approach failed to catch the
extraction_status missing-column production bug (column existed in the ORM
query string but not in the actual DB schema).

These tests now run against a real pgvector container via the test_database
fixture, so a missing column raises immediately.
"""

import uuid
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import MagicMock

from database.repositories.job_post import JobPostRepository


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def db_session(test_database):
    """Per-test DB session with automatic rollback for isolation."""
    engine = create_engine(test_database)
    connection = engine.connect()
    transaction = connection.begin()
    session = sessionmaker(bind=connection)()
    yield session
    session.close()
    if transaction.is_active:
        transaction.rollback()
    connection.close()


def _make_job(session, **overrides):
    """Insert and flush a minimal JobPost.  All ORM defaults apply."""
    from database.models import JobPost
    defaults = dict(
        canonical_fingerprint=str(uuid.uuid4()),
        title="Test Job",
        company="Test Co",
        description="Some description",
    )
    defaults.update(overrides)
    job = JobPost(**defaults)
    session.add(job)
    session.flush()
    return job


# ---------------------------------------------------------------------------
# Stage-aware query tests — require a real DB
# ---------------------------------------------------------------------------

@pytest.mark.db
class TestJobPostStageQueries:
    """Stage-aware selection queries execute against a real DB schema.

    Running against a real DB proves the columns exist; mock SQL string
    inspection cannot catch schema drift.
    """

    def test_get_unextracted_jobs_references_correct_columns(self, db_session):
        """Query executes without error — proves extraction_status,
        extraction_next_retry_at, and is_extracted exist in the schema."""
        repo = JobPostRepository(db_session)
        results = repo.get_unextracted_jobs(limit=5)
        assert isinstance(results, list)

    def test_get_unextracted_jobs_returns_pending_jobs(self, db_session):
        """Jobs with extraction_status='pending' are returned."""
        job = _make_job(db_session, extraction_status="pending")
        repo = JobPostRepository(db_session)
        ids = [j.id for j in repo.get_unextracted_jobs(limit=10)]
        assert job.id in ids

    def test_get_unextracted_jobs_returns_failed_retryable_without_future_retry(self, db_session):
        """Jobs with extraction_status='failed_retryable' and no future retry_at are returned."""
        job = _make_job(
            db_session,
            extraction_status="failed_retryable",
            extraction_next_retry_at=None,
        )
        repo = JobPostRepository(db_session)
        ids = [j.id for j in repo.get_unextracted_jobs(limit=10)]
        assert job.id in ids

    def test_get_unextracted_jobs_excludes_succeeded(self, db_session):
        """Jobs with extraction_status='succeeded' are NOT returned."""
        job = _make_job(db_session, extraction_status="succeeded", is_extracted=True)
        repo = JobPostRepository(db_session)
        ids = [j.id for j in repo.get_unextracted_jobs(limit=10)]
        assert job.id not in ids

    def test_get_unembedded_jobs_references_correct_columns(self, db_session):
        """Query executes without error — proves embedding_status,
        embedding_next_retry_at, and is_embedded exist in the schema."""
        repo = JobPostRepository(db_session)
        results = repo.get_unembedded_jobs(limit=5)
        assert isinstance(results, list)

    def test_get_unembedded_jobs_returns_eligible_jobs(self, db_session):
        """Jobs with succeeded extraction and pending embedding are returned."""
        job = _make_job(
            db_session,
            extraction_status="succeeded",
            is_extracted=True,
            embedding_status="pending",
            is_embedded=False,
            summary_embedding=None,
        )
        repo = JobPostRepository(db_session)
        ids = [j.id for j in repo.get_unembedded_jobs(limit=10)]
        assert job.id in ids

    def test_get_unembedded_jobs_excludes_unextracted(self, db_session):
        """Jobs that haven't been extracted yet are NOT eligible for embedding."""
        job = _make_job(db_session, extraction_status="pending", is_extracted=False)
        repo = JobPostRepository(db_session)
        ids = [j.id for j in repo.get_unembedded_jobs(limit=10)]
        assert job.id not in ids


# ---------------------------------------------------------------------------
# Mutation tests — real DB commit verifies column names and types
# ---------------------------------------------------------------------------

@pytest.mark.db
class TestJobPostStageMutationsDB:
    """Mutation helpers write to real DB columns — no SQL string inspection."""

    def test_mark_extraction_retryable_failed_commits(self, db_session):
        """mark_extraction_retryable_failed writes all stage columns without error."""
        job = _make_job(
            db_session,
            extraction_status="in_progress",
            extraction_attempts=1,
        )
        repo = JobPostRepository(db_session)
        repo.mark_extraction_retryable_failed(job.id, "timeout error")
        db_session.flush()
        db_session.expire(job)

        assert job.extraction_status == "failed_retryable"
        assert job.extraction_attempts == 2
        assert job.is_extracted is False
        assert job.extraction_last_error == "timeout error"
        assert job.extraction_next_retry_at is not None

    def test_mark_embedding_retryable_failed_commits(self, db_session):
        """mark_embedding_retryable_failed writes all stage columns without error."""
        job = _make_job(
            db_session,
            extraction_status="succeeded",
            is_extracted=True,
            embedding_status="in_progress",
            embedding_attempts=2,
        )
        repo = JobPostRepository(db_session)
        repo.mark_embedding_retryable_failed(job.id, "bad vector")
        db_session.flush()
        db_session.expire(job)

        assert job.embedding_status == "failed_retryable"
        assert job.embedding_attempts == 3
        assert job.is_embedded is False
        assert job.embedding_last_error == "bad vector"
        assert job.embedding_next_retry_at is not None


# ---------------------------------------------------------------------------
# Business-logic attribute tests — no DB required
# ---------------------------------------------------------------------------

class TestJobPostStageMutations:
    """mark_* helpers set Python attributes correctly (unit tests, no DB)."""

    def test_mark_as_extracted_sets_completed_state(self):
        job = MagicMock()
        job.extraction_attempts = 0

        repo = JobPostRepository(MagicMock())
        repo.mark_as_extracted(job)

        assert job.is_extracted is True
        assert job.extraction_status == "succeeded"
        assert job.extraction_attempts == 1
        assert job.extraction_last_error is None
        assert job.extraction_last_attempt_at is not None
        assert job.extraction_next_retry_at is None

    def test_save_job_embedding_sets_completed_state(self):
        job = MagicMock()
        job.embedding_attempts = 0

        repo = JobPostRepository(MagicMock())
        repo.save_job_embedding(job, [0.1, 0.2])

        assert job.summary_embedding == [0.1, 0.2]
        assert job.is_embedded is True
        assert job.embedding_status == "succeeded"
        assert job.embedding_attempts == 1
        assert job.embedding_last_error is None
        assert job.embedding_last_attempt_at is not None
        assert job.embedding_next_retry_at is None
