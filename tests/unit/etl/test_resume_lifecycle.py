from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from etl.orchestrator import JobETLService


def test_embed_resume_uses_lifecycle_aware_ready_promotion():
    service = JobETLService(ai_service=MagicMock())
    service.ensure_resume_ready = MagicMock()
    repo = MagicMock()
    repo.resume.get_structured_resume_by_fingerprint.return_value = SimpleNamespace(
        extracted_data={"basics": {"name": "Test"}},
    )

    with patch("etl.orchestrator.ResumeSchema.model_validate", return_value=MagicMock()):
        embedded, fingerprint = service.embed_resume(repo, "fp-1")

    assert embedded is True
    assert fingerprint == "fp-1"
    service.ensure_resume_ready.assert_called_once()


def test_extract_resume_marks_extraction_in_progress_before_extracting(tmp_path):
    service = JobETLService(ai_service=MagicMock())
    repo = MagicMock()
    repo.resume.get_structured_resume_by_fingerprint.return_value = None
    service._load_and_check_resume = MagicMock(
        return_value=(True, "fp-1", {"raw_text": "resume"})
    )
    service._extract_resume_data = MagicMock(return_value=(True, "fp-1", {"raw_text": "resume"}))

    extracted, fingerprint, _ = service.extract_resume(repo, str(tmp_path / "resume.txt"))

    assert extracted is True
    assert fingerprint == "fp-1"
    repo.set_resume_processing_state.assert_called_once()
