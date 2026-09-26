"""Budget deferral survives a session replacement and becomes due at reset."""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session

from database.models import JobPost
from database.repositories.job_post import JobPostRepository
from tests.unit.database.repositories.test_job_post_stage_status import db_session, _make_job


@pytest.mark.db
def test_budget_retry_is_durable_and_does_not_duplicate_queued_work(db_session):
    now = datetime.now(timezone.utc)
    pending = _make_job(db_session, extraction_status="pending")
    queued = _make_job(db_session, extraction_status="queued")
    busy = _make_job(db_session, extraction_status="in_progress", extraction_last_attempt_at=now)
    completed = _make_job(db_session, extraction_status="succeeded", is_extracted=True)
    ids = [pending.id, queued.id, busy.id, completed.id]
    repo = JobPostRepository(db_session)
    repo.defer_extraction_until(ids, now + timedelta(hours=1))
    db_session.flush()
    # New identity map models a restarted worker, retaining the database state.
    with Session(bind=db_session.get_bind()) as replacement:
        new_repo = JobPostRepository(replacement)
        assert not {pending.id, queued.id}.intersection(j.id for j in new_repo.get_unextracted_jobs(100))
        for job_id in (pending.id, queued.id):
            job = replacement.get(JobPost, job_id)
            assert job.extraction_status == "failed_retryable"
            assert job.extraction_last_error == "global_budget_exhausted"
        assert replacement.get(JobPost, busy.id).extraction_status == "in_progress"
        assert replacement.get(JobPost, completed.id).extraction_status == "succeeded"
        new_repo.defer_extraction_until([pending.id, queued.id], now - timedelta(seconds=1))
        replacement.flush()
        assert {pending.id, queued.id}.issubset(j.id for j in new_repo.get_unextracted_jobs(100))
