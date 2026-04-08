"""Unit tests for database/repositories/resume.py"""

import pytest
from unittest.mock import MagicMock

from database.repositories.resume import ResumeRepository
from database.models import StructuredResume, ResumeSectionEmbedding, ResumeEvidenceUnitEmbedding, ResumeUpload


def make_repo():
    mock_db = MagicMock()
    return ResumeRepository(mock_db), mock_db


class TestResumeUploads:
    def test_get_resume_upload_applies_owner_filter_when_provided(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        repo.get_resume_upload("upload-1", owner_id="user-1")

        mock_db.execute.assert_called_once()

    def test_get_latest_resume_upload_for_hash_executes_query(self):
        repo, mock_db = make_repo()
        upload = MagicMock(spec=ResumeUpload)
        mock_db.execute.return_value.scalar_one_or_none.return_value = upload

        result = repo.get_latest_resume_upload_for_hash("user-1", "hash-1")

        assert result is upload

    def test_get_ready_resume_uploads_executes_query(self):
        repo, mock_db = make_repo()
        uploads = [MagicMock(spec=ResumeUpload)]
        mock_db.execute.return_value.scalars.return_value.all.return_value = uploads

        result = repo.get_ready_resume_uploads("user-1")

        assert result == uploads

    def test_get_latest_ready_resume_upload_executes_query(self):
        repo, mock_db = make_repo()
        upload = MagicMock(spec=ResumeUpload)
        mock_db.execute.return_value.scalar_one_or_none.return_value = upload

        result = repo.get_latest_ready_resume_upload("user-1")

        assert result is upload

    def test_update_resume_upload_raises_when_missing(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        with pytest.raises(ValueError, match="Resume upload not found"):
            repo.update_resume_upload("upload-missing")

    def test_update_resume_upload_updates_fields(self):
        repo, mock_db = make_repo()
        upload = MagicMock(spec=ResumeUpload)
        mock_db.execute.return_value.scalar_one_or_none.return_value = upload

        result = repo.update_resume_upload(
            "upload-1",
            status="ready",
            last_error="boom",
            processing_task_id="task-1",
            failure_stage="embedding",
            failure_class="transient",
            retryable=True,
            user_safe_message="Retry later",
            failure_debug_context={"detail": "x"},
        )

        assert result is upload
        assert upload.status == "ready"
        assert upload.last_error == "boom"
        assert upload.processing_task_id == "task-1"
        assert upload.failure_stage == "embedding"
        assert upload.failure_class == "transient"
        assert upload.retryable is True
        assert upload.user_safe_message == "Retry later"
        assert upload.failure_debug_context == {"detail": "x"}
        mock_db.flush.assert_called_once()


# ---------------------------------------------------------------------------
# save_structured_resume
# ---------------------------------------------------------------------------

class TestSaveStructuredResume:
    def test_creates_new_record_when_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None

        repo.save_structured_resume("fp-1", {"name": "Alice"})

        mock_db.add.assert_called_once()
        added = mock_db.add.call_args[0][0]
        assert isinstance(added, StructuredResume)
        assert added.resume_fingerprint == "fp-1"
        assert added.extracted_data == {"name": "Alice"}

    def test_updates_existing_record(self):
        repo, mock_db = make_repo()
        existing = MagicMock(spec=StructuredResume)
        mock_db.execute.return_value.scalar_one_or_none.return_value = existing

        repo.save_structured_resume("fp-1", {"name": "Bob"}, total_experience_years=5.0)

        mock_db.add.assert_not_called()
        assert existing.extracted_data == {"name": "Bob"}
        assert existing.total_experience_years == 5.0

    def test_flushes_after_create(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        repo.save_structured_resume("fp-1", {})
        mock_db.flush.assert_called_once()

    def test_flushes_after_update(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = MagicMock(spec=StructuredResume)
        repo.save_structured_resume("fp-1", {})
        mock_db.flush.assert_called_once()

    def test_extraction_warnings_defaults_to_empty_list(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        repo.save_structured_resume("fp-1", {})
        added = mock_db.add.call_args[0][0]
        assert added.extraction_warnings == []

    def test_returns_existing_when_updating(self):
        repo, mock_db = make_repo()
        existing = MagicMock(spec=StructuredResume)
        mock_db.execute.return_value.scalar_one_or_none.return_value = existing
        result = repo.save_structured_resume("fp-1", {})
        assert result is existing

    def test_extraction_confidence_stored(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        repo.save_structured_resume("fp-1", {}, extraction_confidence=0.95)
        added = mock_db.add.call_args[0][0]
        assert added.extraction_confidence == 0.95


# ---------------------------------------------------------------------------
# save_resume_section_embeddings
# ---------------------------------------------------------------------------

class TestSaveResumeSectionEmbeddings:
    def test_deletes_old_embeddings_before_saving_new(self):
        repo, mock_db = make_repo()
        sections = [{
            'section_type': 'summary',
            'section_index': 0,
            'source_text': 'Engineer',
            'source_data': {},
            'embedding': [0.1, 0.2],
        }]

        repo.save_resume_section_embeddings("fp-1", sections)

        mock_db.execute.assert_called_once()
        assert mock_db.add.call_count == 1
        added = mock_db.add.call_args[0][0]
        assert isinstance(added, ResumeSectionEmbedding)
        assert added.section_type == 'summary'

    def test_flushes_after_saving(self):
        repo, mock_db = make_repo()
        sections = [{'section_type': 'skills', 'section_index': 0, 'source_text': 'x', 'source_data': {}, 'embedding': []}]
        repo.save_resume_section_embeddings("fp-1", sections)
        mock_db.flush.assert_called_once()

    def test_empty_sections_only_deletes(self):
        repo, mock_db = make_repo()
        repo.save_resume_section_embeddings("fp-1", [])
        mock_db.execute.assert_called_once()
        mock_db.add.assert_not_called()

    def test_multiple_sections_all_added(self):
        repo, mock_db = make_repo()
        sections = [
            {'section_type': 'summary', 'section_index': 0, 'source_text': 'A', 'source_data': {}, 'embedding': []},
            {'section_type': 'experience', 'section_index': 0, 'source_text': 'B', 'source_data': {}, 'embedding': []},
            {'section_type': 'skills', 'section_index': 0, 'source_text': 'C', 'source_data': {}, 'embedding': []},
        ]
        result = repo.save_resume_section_embeddings("fp-1", sections)
        assert mock_db.add.call_count == 3
        assert len(result) == 3

    def test_returns_list_of_embedding_records(self):
        repo, _ = make_repo()
        sections = [{'section_type': 'summary', 'section_index': 0, 'source_text': 'x', 'source_data': {}, 'embedding': [0.5]}]
        result = repo.save_resume_section_embeddings("fp-1", sections)
        assert all(isinstance(r, ResumeSectionEmbedding) for r in result)


# ---------------------------------------------------------------------------
# get_resume_section_embeddings
# ---------------------------------------------------------------------------

class TestGetResumeSectionEmbeddings:
    def test_returns_all_sections_when_no_filter(self):
        repo, mock_db = make_repo()
        sections = [MagicMock(), MagicMock()]
        mock_db.execute.return_value.scalars.return_value.all.return_value = sections
        result = repo.get_resume_section_embeddings("fp-1")
        assert result == sections

    def test_filters_by_section_type(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        result = repo.get_resume_section_embeddings("fp-1", section_type="summary")
        mock_db.execute.assert_called_once()
        assert result == []

    def test_returns_empty_when_no_sections(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        result = repo.get_resume_section_embeddings("fp-none")
        assert result == []


# ---------------------------------------------------------------------------
# save_evidence_unit_embeddings
# ---------------------------------------------------------------------------

class TestSaveEvidenceUnitEmbeddings:
    def test_deletes_old_units_before_saving_new(self):
        repo, mock_db = make_repo()
        units = [{
            'evidence_unit_id': 'u-1',
            'source_text': 'Python',
            'source_section': 'Skills',
            'tags': {'lang': 1},
            'embedding': [0.5],
        }]
        repo.save_evidence_unit_embeddings("fp-1", units)
        mock_db.execute.assert_called_once()
        assert mock_db.add.call_count == 1
        added = mock_db.add.call_args[0][0]
        assert isinstance(added, ResumeEvidenceUnitEmbedding)
        assert added.evidence_unit_id == 'u-1'

    def test_flushes_after_saving(self):
        repo, mock_db = make_repo()
        units = [{'evidence_unit_id': 'u-1', 'source_text': 'x', 'embedding': [], 'tags': {}}]
        repo.save_evidence_unit_embeddings("fp-1", units)
        mock_db.flush.assert_called_once()

    def test_empty_units_only_deletes(self):
        repo, mock_db = make_repo()
        repo.save_evidence_unit_embeddings("fp-1", [])
        mock_db.execute.assert_called_once()
        mock_db.add.assert_not_called()

    def test_multiple_units_all_added(self):
        repo, mock_db = make_repo()
        units = [{'evidence_unit_id': f'u-{i}', 'source_text': 'x', 'embedding': [], 'tags': {}} for i in range(3)]
        result = repo.save_evidence_unit_embeddings("fp-1", units)
        assert mock_db.add.call_count == 3
        assert len(result) == 3

    def test_optional_fields_default_correctly(self):
        repo, mock_db = make_repo()
        units = [{'evidence_unit_id': 'u-1', 'source_text': 'x', 'embedding': [], 'tags': {}}]
        repo.save_evidence_unit_embeddings("fp-1", units)
        added = mock_db.add.call_args[0][0]
        assert added.years_value is None
        assert added.years_context is None
        assert added.is_total_years_claim is False


# ---------------------------------------------------------------------------
# get_resume_summary_embedding
# ---------------------------------------------------------------------------

class TestGetResumeSummaryEmbedding:
    def test_returns_embedding_when_summary_found(self):
        repo, mock_db = make_repo()
        mock_section = MagicMock()
        mock_section.embedding = [0.1, 0.2, 0.3]
        mock_db.execute.return_value.scalars.return_value.all.return_value = [mock_section]
        result = repo.get_resume_summary_embedding("fp-1")
        assert result == [0.1, 0.2, 0.3]

    def test_returns_none_when_no_summary(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalars.return_value.all.return_value = []
        result = repo.get_resume_summary_embedding("fp-none")
        assert result is None

    def test_returns_none_when_embedding_is_none(self):
        repo, mock_db = make_repo()
        mock_section = MagicMock()
        mock_section.embedding = None
        mock_db.execute.return_value.scalars.return_value.all.return_value = [mock_section]
        result = repo.get_resume_summary_embedding("fp-1")
        assert result is None

    def test_returns_list_not_original_object(self):
        repo, mock_db = make_repo()
        mock_section = MagicMock()
        mock_section.embedding = (0.1, 0.2)
        mock_db.execute.return_value.scalars.return_value.all.return_value = [mock_section]
        result = repo.get_resume_summary_embedding("fp-1")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# get_structured_resume_by_fingerprint
# ---------------------------------------------------------------------------

class TestGetStructuredResumeByFingerprint:
    def test_returns_resume_when_found(self):
        repo, mock_db = make_repo()
        mock_resume = MagicMock(spec=StructuredResume)
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_resume
        result = repo.get_structured_resume_by_fingerprint("fp-1")
        assert result is mock_resume

    def test_returns_none_when_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        result = repo.get_structured_resume_by_fingerprint("fp-missing")
        assert result is None


# ---------------------------------------------------------------------------
# resume_hash_exists
# ---------------------------------------------------------------------------

class TestResumeHashExists:
    def test_returns_true_when_hash_exists(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = "fp-1"
        assert repo.resume_hash_exists("fp-1") is True

    def test_returns_false_when_hash_not_found(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        assert repo.resume_hash_exists("fp-missing") is False


# ---------------------------------------------------------------------------
# get_latest_stored_resume_fingerprint
# ---------------------------------------------------------------------------

class TestGetLatestStoredResumeFingerprint:
    def test_returns_fingerprint_when_resume_exists(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = "fp-latest"
        result = repo.get_latest_stored_resume_fingerprint()
        assert result == "fp-latest"

    def test_returns_none_when_no_resume_exists(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        result = repo.get_latest_stored_resume_fingerprint()
        assert result is None


# ---------------------------------------------------------------------------
# find_best_evidence_for_requirement
# ---------------------------------------------------------------------------

class TestFindBestEvidenceForRequirement:
    def test_returns_empty_when_no_rows(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.all.return_value = []
        result = repo.find_best_evidence_for_requirement([0.1, 0.2], "fp-1")
        assert result == []

    def test_returns_evidence_with_similarity(self):
        repo, mock_db = make_repo()
        mock_unit = MagicMock()

        row = MagicMock()
        row.__getitem__.return_value = mock_unit
        row._mapping = {'distance': 0.2}
        mock_db.execute.return_value.all.return_value = [row]

        result = repo.find_best_evidence_for_requirement([0.1], "fp-1", top_k=1)

        assert len(result) == 1
        unit, similarity = result[0]
        assert unit is mock_unit
        # cosine_similarity_from_distance(0.2) = 1 - 0.2 = 0.8
        assert similarity == pytest.approx(0.8)

    def test_multiple_results_returned(self):
        repo, mock_db = make_repo()
        rows = []
        for d in [0.1, 0.3, 0.5]:
            row = MagicMock()
            row.__getitem__.return_value = MagicMock()
            row._mapping = {'distance': d}
            rows.append(row)
        mock_db.execute.return_value.all.return_value = rows

        result = repo.find_best_evidence_for_requirement([0.1], "fp-1", top_k=3)
        assert len(result) == 3

    def test_similarity_values_computed_correctly(self):
        repo, mock_db = make_repo()
        row = MagicMock()
        row.__getitem__.return_value = MagicMock()
        row._mapping = {'distance': 0.0}
        mock_db.execute.return_value.all.return_value = [row]

        result = repo.find_best_evidence_for_requirement([0.1], "fp-1")
        _, similarity = result[0]
        assert similarity == pytest.approx(1.0)
