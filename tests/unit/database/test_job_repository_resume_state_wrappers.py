#!/usr/bin/env python3
"""Unit tests for JobRepository resume-state wrapper methods."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from database.models import DEFAULT_LEGACY_OWNER_ID
from database.repository import JobRepository


def test_job_repository_forwards_resume_state_methods():
    db = MagicMock()
    repo = JobRepository(db)
    repo._resume_repo = MagicMock()

    state = SimpleNamespace(processing_status="embedding")
    repo._resume_repo.get_resume_processing_state.return_value = state
    repo._resume_repo.get_latest_resume_processing_state.return_value = state
    repo._resume_repo.set_resume_processing_state.return_value = state
    repo._resume_repo.is_resume_ready.return_value = True
    repo._resume_repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
    repo._resume_repo.resume_needs_embedding.return_value = False

    assert repo.get_resume_processing_state("fp-1") is state
    assert repo.get_latest_resume_processing_state() is state
    assert (
        repo.set_resume_processing_state(
            "fp-1",
            "ready",
            error=None,
            extraction_completed_at="extract-ts",
            embedding_completed_at="embed-ts",
        )
        is state
    )
    assert repo.is_resume_ready("fp-1") is True
    assert repo.get_latest_ready_resume_fingerprint() == "fp-ready"
    assert repo.resume_needs_embedding("fp-1") is False

    repo._resume_repo.get_resume_processing_state.assert_called_once_with("fp-1")
    repo._resume_repo.get_latest_resume_processing_state.assert_called_once_with()
    repo._resume_repo.set_resume_processing_state.assert_called_once_with(
        owner_id=DEFAULT_LEGACY_OWNER_ID,
        resume_fingerprint="fp-1",
        status="ready",
        error=None,
        extraction_completed_at="extract-ts",
        embedding_completed_at="embed-ts",
        fingerprint_version=1,
        failure_stage=None,
        failure_class=None,
        retryable=None,
        user_safe_message=None,
    )
    repo._resume_repo.is_resume_ready.assert_called_once_with("fp-1")
    repo._resume_repo.get_latest_ready_resume_fingerprint.assert_called_once_with()
    repo._resume_repo.resume_needs_embedding.assert_called_once_with("fp-1")
