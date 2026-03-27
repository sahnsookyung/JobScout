"""Integration coverage for latest-upload and stale-result lifecycle rules."""

from __future__ import annotations

import asyncio
import contextlib
import importlib
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import Mock, patch

from core.redis_streams import delete_task_state, get_redis_client, get_task_state, set_task_state
from core.resume_selection import build_resume_fingerprint
from database.models import (
    RESUME_PROCESSING_EMBEDDING,
    RESUME_PROCESSING_READY,
    RESUME_UPLOAD_IN_PROGRESS,
    RESUME_UPLOAD_READY,
    ResumeEvidenceUnitEmbedding,
    ResumeProcessingState,
    ResumeSectionEmbedding,
    ResumeUpload,
    StructuredResume,
    User,
    UserAuthIdentity,
)
from database.repository import JobRepository
from pipeline.runner import MatchingPipelineResult

pytestmark = [
    pytest.mark.integration,
    pytest.mark.db,
    pytest.mark.redis,
]

OWNER_ID = UUID("00000000-0000-0000-0000-000000000001")
OWNER_SUBJECT = "dev-bypass:dev-user@jobscout.local"
VECTOR = [0.0] * 1024


@pytest.fixture
def lifecycle_env(test_db_url, redis_url, monkeypatch):
    """Patch DB/Redis access for the touched modules to use test infrastructure."""
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    monkeypatch.setenv("REDIS_URL", redis_url)
    monkeypatch.setenv("AUTH_MODE", "dev-bypass")
    monkeypatch.setenv("JOBSCOUT_ENV", "test")
    monkeypatch.setenv("DEV_BYPASS_USER_ID", str(OWNER_ID))

    engine = create_engine(test_db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    @contextlib.contextmanager
    def local_job_uow():
        session = SessionLocal()
        try:
            yield JobRepository(session)
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    resume_selection = importlib.import_module("core.resume_selection")
    pipeline_router = importlib.import_module("web.backend.routers.pipeline")
    scorer_matcher = importlib.import_module("services.scorer_matcher.main")
    redis_streams = importlib.import_module("core.redis_streams")

    monkeypatch.setattr(resume_selection, "job_uow", local_job_uow)
    monkeypatch.setattr(pipeline_router, "job_uow", local_job_uow)
    monkeypatch.setattr(scorer_matcher, "job_uow", local_job_uow)
    monkeypatch.setattr(redis_streams, "REDIS_URL", redis_url)
    redis_streams._connection_pool = None

    client = get_redis_client()
    client.flushdb()

    yield SessionLocal

    client.flushdb()
    engine.dispose()


@pytest.fixture(autouse=True)
def clear_resume_tables(lifecycle_env):
    """Keep resume lifecycle tables isolated per test."""
    session = lifecycle_env()
    try:
        session.query(ResumeUpload).delete()
        session.query(ResumeEvidenceUnitEmbedding).delete()
        session.query(ResumeSectionEmbedding).delete()
        session.query(StructuredResume).delete()
        session.query(ResumeProcessingState).delete()
        session.query(UserAuthIdentity).delete()
        session.query(User).delete()
        session.commit()
    finally:
        session.close()


def _seed_user(SessionLocal):
    session = SessionLocal()
    try:
        user = User(
            id=OWNER_ID,
            email="dev-user@jobscout.local",
            display_name="JobScout Dev User",
            is_active=True,
        )
        session.add(user)
        session.flush()
        session.add(
            UserAuthIdentity(
                user_id=OWNER_ID,
                provider="password",
                provider_subject=OWNER_SUBJECT,
                email="dev-user@jobscout.local",
                email_normalized="dev-user@jobscout.local",
                email_verified=True,
            )
        )
        session.commit()
    finally:
        session.close()


def _seed_ready_resume(SessionLocal, *, resume_hash: str, resume_fingerprint: str, original_filename: str = "resume.json") -> str:
    from database.repositories.resume import ResumeUploadCreateParams

    session = SessionLocal()
    try:
        repo = JobRepository(session)
        upload = repo.create_resume_upload(
            ResumeUploadCreateParams(
                owner_id=OWNER_ID,
                resume_hash=resume_hash,
                resume_fingerprint=resume_fingerprint,
                original_filename=original_filename,
                status=RESUME_UPLOAD_READY,
                user_safe_message="Resume ready for matching.",
            )
        )
        repo.save_structured_resume(
            resume_fingerprint,
            {
                "profile": {
                    "summary": {
                        "text": "Experienced engineer",
                        "total_experience_years": 5,
                    }
                },
                "extraction": {"confidence": 0.99, "warnings": []},
            },
            owner_id=OWNER_ID,
            total_experience_years=5,
        )
        repo.save_resume_section_embeddings(
            resume_fingerprint,
            [
                {
                    "section_type": "summary",
                    "section_index": 0,
                    "source_text": "Experienced engineer",
                    "source_data": {"kind": "summary"},
                    "embedding": VECTOR,
                }
            ],
            owner_id=OWNER_ID,
        )
        repo.save_evidence_unit_embeddings(
            resume_fingerprint,
            [
                {
                    "evidence_unit_id": "ev-1",
                    "source_text": "Built production systems",
                    "source_section": "summary",
                    "tags": {"kind": "experience"},
                    "embedding": VECTOR,
                }
            ],
            owner_id=OWNER_ID,
        )
        repo.set_resume_processing_state(
            resume_fingerprint,
            RESUME_PROCESSING_READY,
            owner_id=OWNER_ID,
            user_safe_message="Resume ready for matching.",
        )
        session.commit()
        return str(upload.id)
    finally:
        session.close()


def _seed_processing_upload(SessionLocal, *, resume_hash: str, resume_fingerprint: str, task_id: str) -> str:
    from database.repositories.resume import ResumeUploadCreateParams

    session = SessionLocal()
    try:
        repo = JobRepository(session)
        upload = repo.create_resume_upload(
            ResumeUploadCreateParams(
                owner_id=OWNER_ID,
                resume_hash=resume_hash,
                resume_fingerprint=resume_fingerprint,
                original_filename="resume-new.json",
                status=RESUME_UPLOAD_IN_PROGRESS,
                processing_task_id=task_id,
                retryable=True,
                user_safe_message="Latest uploaded resume is still processing (embedding).",
            )
        )
        repo.set_resume_processing_state(
            resume_fingerprint,
            RESUME_PROCESSING_EMBEDDING,
            owner_id=OWNER_ID,
            user_safe_message="Latest uploaded resume is still processing (embedding).",
            retryable=True,
        )
        session.commit()
        return str(upload.id)
    finally:
        session.close()


def test_resume_eligibility_blocks_matching_when_latest_upload_is_not_ready(lifecycle_env):
    _seed_user(lifecycle_env)
    _seed_ready_resume(
        lifecycle_env,
        resume_hash="hash-old",
        resume_fingerprint="fp-old",
    )
    latest_upload_id = _seed_processing_upload(
        lifecycle_env,
        resume_hash="hash-new",
        resume_fingerprint="fp-new",
        task_id="resume-task-new",
    )

    from web.backend.dependencies import get_current_user
    from web.backend.routers.pipeline import limiter, router

    limiter.enabled = False
    app = FastAPI()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=OWNER_ID)
    app.include_router(router)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/api/pipeline/resume-eligibility")

    assert response.status_code == 200
    data = response.json()
    assert data["can_run"] is False
    assert data["status"] == "embedding"
    assert data["task_id"] == "resume-task-new"
    assert data["upload_id"] == latest_upload_id

    run_response = client.post("/api/pipeline/run-matching")
    assert run_response.status_code == 409
    assert "still processing (embedding)" in run_response.json()["detail"]


def test_selecting_ready_resume_unblocks_matching_from_older_processing_upload(lifecycle_env):
    _seed_user(lifecycle_env)
    ready_fingerprint = build_resume_fingerprint(OWNER_ID, "hash-old")
    processing_fingerprint = build_resume_fingerprint(OWNER_ID, "hash-new")
    _seed_ready_resume(
        lifecycle_env,
        resume_hash="hash-old",
        resume_fingerprint=ready_fingerprint,
    )
    _seed_processing_upload(
        lifecycle_env,
        resume_hash="hash-new",
        resume_fingerprint=processing_fingerprint,
        task_id="resume-task-new",
    )

    from web.backend.dependencies import get_current_user
    from web.backend.routers.pipeline import limiter, router, _latest_upload_task_key

    limiter.enabled = False
    app = FastAPI()
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=OWNER_ID)
    app.include_router(router)
    client = TestClient(app, raise_server_exceptions=False)

    redis = get_redis_client()
    redis.set(_latest_upload_task_key(str(OWNER_ID)), "resume-task-new", ex=3600)
    set_task_state(
        "resume-task-new",
        {
            "status": "processing",
            "step": "embedding",
            "task_type": "resume_upload",
            "upload_id": "upload-processing",
            "owner_id": str(OWNER_ID),
            "resume_fingerprint": processing_fingerprint,
        },
        ttl=3600,
    )

    select_response = client.post(
        "/api/pipeline/select-resume",
        json={"resume_hash": "hash-old", "original_filename": "resume-old.json"},
    )
    assert select_response.status_code == 200
    assert redis.get(_latest_upload_task_key(str(OWNER_ID))) is None

    with patch("web.backend.routers.pipeline.enqueue_job") as mock_enqueue:
        run_response = client.post("/api/pipeline/run-matching")

    assert run_response.status_code == 200
    assert run_response.json()["success"] is True
    mock_enqueue.assert_called_once()


def test_matching_consumer_marks_completed_run_stale_when_newer_upload_exists(lifecycle_env):
    _seed_user(lifecycle_env)
    old_upload_id = _seed_ready_resume(
        lifecycle_env,
        resume_hash="hash-old",
        resume_fingerprint="fp-old",
    )
    latest_upload_id = _seed_ready_resume(
        lifecycle_env,
        resume_hash="hash-new",
        resume_fingerprint="fp-new",
        original_filename="resume-new.json",
    )

    from services.scorer_matcher.main import MatcherConsumer

    consumer = MatcherConsumer(Mock())
    task_id = "matching-task-stale"

    with patch(
        "services.scorer_matcher.main._run_matching_pipeline_sync",
        return_value=MatchingPipelineResult(
            success=True,
            matches_count=2,
            saved_count=2,
            notified_count=1,
            execution_time=0.25,
        ),
    ):
        success, result = asyncio.run(
            consumer._do_process(
                "msg-1",
                {
                    "task_id": task_id,
                    "owner_id": str(OWNER_ID),
                    "resume_upload_id": old_upload_id,
                    "resume_fingerprint": "fp-old",
                },
            )
        )

    assert success is True
    assert result["status"] == "completed"

    task_state = get_task_state(task_id)
    assert task_state is not None
    assert task_state["stale_due_to_newer_upload"] is True
    assert task_state["latest_upload_id"] == latest_upload_id
    assert task_state["latest_resume_fingerprint"] == "fp-new"
    assert "older resume upload" in task_state["stale_message"].lower()

    delete_task_state(task_id)
