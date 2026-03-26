#!/usr/bin/env python3
"""Unit tests for ResumeRepository lifecycle helpers."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from database.models import RESUME_PROCESSING_READY
from database.repositories.resume import ResumeRepository


def _scalar_result(value):
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def test_get_resume_processing_state_reads_single_state():
    db = MagicMock()
    state = SimpleNamespace(resume_fingerprint="fp-1", processing_status="embedding")
    db.execute.return_value = _scalar_result(state)

    repo = ResumeRepository(db)

    assert repo.get_resume_processing_state("fp-1") is state


def test_get_latest_resume_processing_state_reads_latest_state():
    db = MagicMock()
    state = SimpleNamespace(resume_fingerprint="fp-latest", processing_status="ready")
    db.execute.return_value = _scalar_result(state)

    repo = ResumeRepository(db)

    assert repo.get_latest_resume_processing_state() is state


def test_set_resume_processing_state_creates_new_row():
    db = MagicMock()
    repo = ResumeRepository(db)
    repo.get_resume_processing_state = MagicMock(return_value=None)

    extraction_completed_at = datetime.now(timezone.utc)
    embedding_completed_at = datetime.now(timezone.utc)

    state = repo.set_resume_processing_state(
        "fp-new",
        "ready",
        error="none",
        extraction_completed_at=extraction_completed_at,
        embedding_completed_at=embedding_completed_at,
    )

    assert state.resume_fingerprint == "fp-new"
    assert state.processing_status == "ready"
    assert state.last_error == "none"
    assert state.extraction_completed_at == extraction_completed_at
    assert state.embedding_completed_at == embedding_completed_at
    db.add.assert_called_once_with(state)
    db.flush.assert_called_once_with()


def test_set_resume_processing_state_updates_existing_row():
    db = MagicMock()
    existing = SimpleNamespace(
        resume_fingerprint="fp-existing",
        processing_status="extracting",
        last_error=None,
        extraction_completed_at=None,
        embedding_completed_at=None,
    )
    repo = ResumeRepository(db)
    repo.get_resume_processing_state = MagicMock(return_value=existing)

    state = repo.set_resume_processing_state("fp-existing", "failed", error="boom")

    assert state is existing
    assert state.processing_status == "failed"
    assert state.last_error == "boom"
    db.add.assert_not_called()
    db.flush.assert_called_once_with()


def test_is_resume_ready_requires_ready_state():
    db = MagicMock()
    repo = ResumeRepository(db)
    repo.get_resume_processing_state = MagicMock(
        return_value=SimpleNamespace(processing_status="embedding")
    )

    assert repo.is_resume_ready("fp-1") is False


def test_is_resume_ready_requires_structured_resume():
    db = MagicMock()
    repo = ResumeRepository(db)
    repo.get_resume_processing_state = MagicMock(
        return_value=SimpleNamespace(processing_status=RESUME_PROCESSING_READY)
    )
    repo.get_structured_resume_by_fingerprint = MagicMock(return_value=None)

    assert repo.is_resume_ready("fp-1") is False


def test_is_resume_ready_requires_summary_embedding():
    db = MagicMock()
    repo = ResumeRepository(db)
    repo.get_resume_processing_state = MagicMock(
        return_value=SimpleNamespace(processing_status=RESUME_PROCESSING_READY)
    )
    repo.get_structured_resume_by_fingerprint = MagicMock(return_value=object())
    repo.get_resume_summary_embedding = MagicMock(return_value=None)

    assert repo.is_resume_ready("fp-1") is False


def test_is_resume_ready_checks_evidence_and_sections():
    db = MagicMock()
    repo = ResumeRepository(db)
    repo.get_resume_processing_state = MagicMock(
        return_value=SimpleNamespace(processing_status=RESUME_PROCESSING_READY)
    )
    repo.get_structured_resume_by_fingerprint = MagicMock(return_value=object())
    repo.get_resume_summary_embedding = MagicMock(return_value=[0.1, 0.2])

    db.execute.side_effect = [_scalar_result(123), _scalar_result(456)]

    assert repo.is_resume_ready("fp-1") is True


def test_get_latest_ready_resume_fingerprint_returns_first_verified_ready():
    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value = iter([
        SimpleNamespace(resume_fingerprint="fp-stale"),
        SimpleNamespace(resume_fingerprint="fp-ready"),
    ])
    db.execute.return_value = result

    repo = ResumeRepository(db)
    repo.is_resume_ready = MagicMock(side_effect=[False, True])

    assert repo.get_latest_ready_resume_fingerprint() == "fp-ready"


def test_get_latest_ready_resume_fingerprint_returns_none_when_none_verify():
    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value = iter([
        SimpleNamespace(resume_fingerprint="fp-1"),
    ])
    db.execute.return_value = result

    repo = ResumeRepository(db)
    repo.is_resume_ready = MagicMock(return_value=False)

    assert repo.get_latest_ready_resume_fingerprint() is None


def test_resume_needs_embedding_checks_extracted_state():
    db = MagicMock()
    repo = ResumeRepository(db)
    repo.get_resume_processing_state = MagicMock(
        side_effect=[
            SimpleNamespace(processing_status="extracted"),
            SimpleNamespace(processing_status="ready"),
            None,
        ]
    )

    assert repo.resume_needs_embedding("fp-1") is True
    assert repo.resume_needs_embedding("fp-2") is False
    assert repo.resume_needs_embedding("fp-3") is False
