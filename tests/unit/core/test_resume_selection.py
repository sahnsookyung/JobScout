from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID

from core.resume_selection import (
    build_resume_fingerprint,
    evaluate_resume_eligibility,
    evaluate_resume_preflight,
    resolve_owner_id,
)


def test_resolve_owner_id_reads_authenticated_user():
    user = SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000001"))
    assert resolve_owner_id(user) == user.id


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
