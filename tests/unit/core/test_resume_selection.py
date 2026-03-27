from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID

from core.resume_selection import (
    build_resume_fingerprint,
    evaluate_resume_eligibility,
    evaluate_resume_preflight,
    resolve_owner_id,
    serialize_owner_id,
)


def test_resolve_owner_id_reads_authenticated_user():
    user = SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000001"))
    assert resolve_owner_id(user) == user.id

def test_resolve_owner_id_rejects_missing_user_id():
    user = SimpleNamespace()
    try:
        resolve_owner_id(user)
    except ValueError as exc:
        assert "Authenticated user is required" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing user id")

def test_serialize_owner_id_returns_string():
    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    assert serialize_owner_id(owner_id) == str(owner_id)


@patch("core.resume_selection.job_uow")
def test_evaluate_resume_eligibility_returns_missing_when_no_upload(mock_uow):
    repo = MagicMock()
    repo.get_latest_resume_upload.return_value = None
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    result = evaluate_resume_eligibility(owner_id)

    assert result.can_run is False
    assert result.processing_status == "missing"


@patch("core.resume_selection.job_uow")
def test_evaluate_resume_eligibility_uses_latest_upload_not_any_ready_resume(mock_uow):
    repo = MagicMock()
    repo.get_latest_resume_upload.return_value = SimpleNamespace(
        id="upload-2",
        resume_hash="hash-new",
        resume_fingerprint="fp-new",
        status="in_progress",
        processing_task_id="task-2",
    )
    repo.get_resume_processing_state.return_value = SimpleNamespace(
        processing_status="embedding",
        last_error=None,
        user_safe_message=None,
    )
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    result = evaluate_resume_eligibility(owner_id)

    assert result.can_run is False
    assert result.processing_status == "embedding"
    assert result.resume_fingerprint == "fp-new"
    assert result.resume_hash == "hash-new"
    assert result.processing_task_id == "task-2"


@patch("core.resume_selection.job_uow")
def test_evaluate_resume_preflight_returns_ready_for_known_resume(mock_uow):
    repo = MagicMock()
    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    resume_hash = "hash-1"
    owned_fingerprint = build_resume_fingerprint(owner_id, resume_hash)
    repo.get_latest_resume_upload_for_hash.return_value = SimpleNamespace(
        id="upload-1",
        processing_task_id=None,
    )
    repo.is_resume_ready.return_value = True
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    result = evaluate_resume_preflight(owner_id, resume_hash)

    assert result.status == "ready_already_known"
    assert result.can_skip_upload is True
    assert result.resume_fingerprint == owned_fingerprint

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_preflight_returns_processing_existing_for_pending_upload(mock_uow):
    repo = MagicMock()
    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    repo.get_latest_resume_upload_for_hash.return_value = SimpleNamespace(
        id="upload-1",
        processing_task_id="task-1",
        status="pending",
    )
    repo.is_resume_ready.return_value = False
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    result = evaluate_resume_preflight(owner_id, "hash-1")

    assert result.status == "processing_existing"
    assert result.processing_task_id == "task-1"
    assert result.can_skip_upload is True

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_preflight_returns_retryable_failure_for_upload(mock_uow):
    repo = MagicMock()
    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    repo.get_latest_resume_upload_for_hash.return_value = SimpleNamespace(
        id="upload-1",
        processing_task_id="task-2",
        status="failed_retryable",
        user_safe_message="Retry me",
        last_error="boom",
    )
    repo.is_resume_ready.return_value = False
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    result = evaluate_resume_preflight(owner_id, "hash-1")

    assert result.status == "failed_retryable"
    assert result.retryable is True
    assert result.message == "Retry me"

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_preflight_returns_reupload_required_for_upload(mock_uow):
    repo = MagicMock()
    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    repo.get_latest_resume_upload_for_hash.return_value = SimpleNamespace(
        id="upload-1",
        processing_task_id="task-3",
        status="failed_reupload_required",
        user_safe_message=None,
        last_error="Need new upload",
    )
    repo.is_resume_ready.return_value = False
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    result = evaluate_resume_preflight(owner_id, "hash-1")

    assert result.status == "failed_reupload_required"
    assert result.retryable is False
    assert result.can_skip_upload is False
    assert result.message == "Need new upload"

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_preflight_falls_back_to_processing_state(mock_uow):
    repo = MagicMock()
    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    repo.get_latest_resume_upload_for_hash.return_value = None
    repo.is_resume_ready.return_value = False
    repo.get_resume_processing_state.return_value = SimpleNamespace(
        processing_status="embedding",
        user_safe_message="Still embedding",
    )
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    result = evaluate_resume_preflight(owner_id, "hash-1")

    assert result.status == "processing_existing"
    assert result.message == "Still embedding"

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_preflight_uses_failed_processing_state_retryable_flag(mock_uow):
    repo = MagicMock()
    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    repo.get_latest_resume_upload_for_hash.return_value = None
    repo.is_resume_ready.return_value = False
    repo.get_resume_processing_state.return_value = SimpleNamespace(
        processing_status="failed",
        retryable=False,
        user_safe_message=None,
        last_error="Need reupload",
    )
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    result = evaluate_resume_preflight(owner_id, "hash-1")

    assert result.status == "failed_reupload_required"
    assert result.can_skip_upload is False

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_preflight_returns_upload_required_when_unknown(mock_uow):
    repo = MagicMock()
    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    repo.get_latest_resume_upload_for_hash.return_value = None
    repo.is_resume_ready.return_value = False
    repo.get_resume_processing_state.return_value = None
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    result = evaluate_resume_preflight(owner_id, "hash-1")

    assert result.status == "upload_required"
    assert result.can_skip_upload is False

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_eligibility_returns_ready_when_latest_upload_ready(mock_uow):
    repo = MagicMock()
    repo.get_latest_resume_upload.return_value = SimpleNamespace(
        id="upload-1",
        resume_hash="hash-1",
        resume_fingerprint="fp-1",
        status="ready",
        processing_task_id=123,
    )
    repo.is_resume_ready.return_value = True
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    result = evaluate_resume_eligibility(owner_id)

    assert result.can_run is True
    assert result.processing_status == "ready"
    assert result.processing_task_id is None

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_eligibility_uses_default_processing_message(mock_uow):
    repo = MagicMock()
    repo.get_latest_resume_upload.return_value = SimpleNamespace(
        id="upload-2",
        resume_hash="hash-2",
        resume_fingerprint="fp-2",
        status="pending",
        processing_task_id="task-2",
    )
    repo.get_resume_processing_state.return_value = None
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    result = evaluate_resume_eligibility(owner_id)

    assert result.processing_status == "processing"
    assert "still processing" in result.message

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_eligibility_returns_failed_latest_upload(mock_uow):
    repo = MagicMock()
    repo.get_latest_resume_upload.return_value = SimpleNamespace(
        id="upload-3",
        resume_hash="hash-3",
        resume_fingerprint="fp-3",
        status="failed_retryable",
        processing_task_id="task-3",
        user_safe_message=None,
        last_error="Retry later",
        retryable=True,
    )
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    result = evaluate_resume_eligibility(owner_id)

    assert result.can_run is False
    assert result.processing_status == "failed_retryable"
    assert result.message == "Retry later"
    assert result.retryable is True

@patch("core.resume_selection.job_uow")
def test_evaluate_resume_eligibility_returns_fallback_for_unready_status(mock_uow):
    repo = MagicMock()
    repo.get_latest_resume_upload.return_value = SimpleNamespace(
        id="upload-4",
        resume_hash="hash-4",
        resume_fingerprint="fp-4",
        status="ready",
        processing_task_id="task-4",
    )
    repo.is_resume_ready.return_value = False
    mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

    owner_id = UUID("00000000-0000-0000-0000-000000000001")
    result = evaluate_resume_eligibility(owner_id)

    assert result.can_run is False
    assert result.message == "Latest uploaded resume is not ready for matching."
