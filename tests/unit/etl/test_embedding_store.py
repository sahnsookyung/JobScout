"""Unit tests for etl/resume/embedding_store.py"""

from unittest.mock import MagicMock

from etl.resume.embedding_store import (
    ResumeSectionEmbeddingStore,
    ResumeEvidenceUnitEmbeddingStore,
    InMemoryEmbeddingStore,
    JobRepositoryAdapter,
)


# ---------------------------------------------------------------------------
# Protocol abstract method bodies (super() delegation)
# ---------------------------------------------------------------------------

class TestResumeSectionEmbeddingStoreProtocol:
    def test_save_pass_returns_none(self):
        class Concrete(ResumeSectionEmbeddingStore):
            def save_resume_section_embeddings(self, fp, sections):
                return super().save_resume_section_embeddings(fp, sections)
            def get_resume_section_embeddings(self, fp, section_type=None):
                return super().get_resume_section_embeddings(fp, section_type)
        c = Concrete()
        assert c.save_resume_section_embeddings("fp", []) is None

    def test_get_pass_returns_none(self):
        class Concrete(ResumeSectionEmbeddingStore):
            def save_resume_section_embeddings(self, fp, sections):
                return super().save_resume_section_embeddings(fp, sections)
            def get_resume_section_embeddings(self, fp, section_type=None):
                return super().get_resume_section_embeddings(fp, section_type)
        c = Concrete()
        assert c.get_resume_section_embeddings("fp") is None


class TestResumeEvidenceUnitEmbeddingStoreProtocol:
    def test_save_pass_returns_none(self):
        class Concrete(ResumeEvidenceUnitEmbeddingStore):
            def save_evidence_unit_embeddings(self, fp, units):
                return super().save_evidence_unit_embeddings(fp, units)
            def get_evidence_unit_embeddings(self, fp):
                return super().get_evidence_unit_embeddings(fp)
        c = Concrete()
        assert c.save_evidence_unit_embeddings("fp", []) is None

    def test_get_pass_returns_none(self):
        class Concrete(ResumeEvidenceUnitEmbeddingStore):
            def save_evidence_unit_embeddings(self, fp, units):
                return super().save_evidence_unit_embeddings(fp, units)
            def get_evidence_unit_embeddings(self, fp):
                return super().get_evidence_unit_embeddings(fp)
        c = Concrete()
        assert c.get_evidence_unit_embeddings("fp") is None


# ---------------------------------------------------------------------------
# InMemoryEmbeddingStore
# ---------------------------------------------------------------------------

class TestInMemoryEmbeddingStore:
    def test_init_creates_empty_storage(self):
        store = InMemoryEmbeddingStore()
        assert store._storage == {}

    def test_save_and_retrieve_section_embeddings(self):
        store = InMemoryEmbeddingStore()
        sections = [{'section_type': 'summary', 'embedding': [0.1, 0.2]}]
        store.save_resume_section_embeddings("fp-1", sections)
        result = store.get_resume_section_embeddings("fp-1")
        assert result == sections

    def test_save_appends_to_existing(self):
        store = InMemoryEmbeddingStore()
        store.save_resume_section_embeddings("fp-1", [{'section_type': 'summary'}])
        store.save_resume_section_embeddings("fp-1", [{'section_type': 'skills'}])
        result = store.get_resume_section_embeddings("fp-1")
        assert len(result) == 2

    def test_get_filters_by_section_type(self):
        store = InMemoryEmbeddingStore()
        sections = [
            {'section_type': 'summary', 'embedding': [0.1]},
            {'section_type': 'skills', 'embedding': [0.2]},
        ]
        store.save_resume_section_embeddings("fp-1", sections)
        result = store.get_resume_section_embeddings("fp-1", section_type="summary")
        assert len(result) == 1
        assert result[0]['section_type'] == 'summary'

    def test_get_without_filter_returns_all(self):
        store = InMemoryEmbeddingStore()
        sections = [
            {'section_type': 'summary'},
            {'section_type': 'skills'},
        ]
        store.save_resume_section_embeddings("fp-1", sections)
        result = store.get_resume_section_embeddings("fp-1")
        assert len(result) == 2

    def test_get_empty_fingerprint_returns_empty(self):
        store = InMemoryEmbeddingStore()
        result = store.get_resume_section_embeddings("fp-missing")
        assert result == []

    def test_save_evidence_unit_embeddings(self):
        store = InMemoryEmbeddingStore()
        units = [{'evidence_unit_id': 'u-1', 'embedding': [0.5]}]
        store.save_evidence_unit_embeddings("fp-1", units)
        result = store.get_evidence_unit_embeddings("fp-1")
        assert result == units

    def test_save_evidence_appends_to_existing(self):
        store = InMemoryEmbeddingStore()
        store.save_evidence_unit_embeddings("fp-1", [{'id': 'u-1'}])
        store.save_evidence_unit_embeddings("fp-1", [{'id': 'u-2'}])
        result = store.get_evidence_unit_embeddings("fp-1")
        assert len(result) == 2

    def test_get_evidence_empty_fingerprint(self):
        store = InMemoryEmbeddingStore()
        result = store.get_evidence_unit_embeddings("fp-missing")
        assert result == []

    def test_clear_removes_all_data(self):
        store = InMemoryEmbeddingStore()
        store.save_resume_section_embeddings("fp-1", [{'section_type': 'summary'}])
        store.clear()
        assert store._storage == {}
        assert store.get_resume_section_embeddings("fp-1") == []

    def test_multiple_fingerprints_isolated(self):
        store = InMemoryEmbeddingStore()
        store.save_resume_section_embeddings("fp-1", [{'section_type': 'summary'}])
        store.save_resume_section_embeddings("fp-2", [{'section_type': 'skills'}])
        assert len(store.get_resume_section_embeddings("fp-1")) == 1
        assert len(store.get_resume_section_embeddings("fp-2")) == 1


# ---------------------------------------------------------------------------
# JobRepositoryAdapter
# ---------------------------------------------------------------------------

class TestJobRepositoryAdapter:
    def _make_adapter(self):
        mock_repo = MagicMock()
        adapter = JobRepositoryAdapter(mock_repo)
        return adapter, mock_repo

    def test_save_resume_section_embeddings_delegates(self):
        adapter, mock_repo = self._make_adapter()
        sections = [{'section_type': 'summary'}]
        adapter.save_resume_section_embeddings("fp-1", sections)
        mock_repo.save_resume_section_embeddings.assert_called_once_with(
            resume_fingerprint="fp-1", sections=sections
        )

    def test_get_resume_section_embeddings_delegates(self):
        adapter, mock_repo = self._make_adapter()
        mock_repo.get_resume_section_embeddings.return_value = ["sec-1"]
        result = adapter.get_resume_section_embeddings("fp-1", section_type="summary")
        mock_repo.get_resume_section_embeddings.assert_called_once_with(
            resume_fingerprint="fp-1", section_type="summary"
        )
        assert result == ["sec-1"]

    def test_get_resume_section_embeddings_no_filter(self):
        adapter, mock_repo = self._make_adapter()
        mock_repo.get_resume_section_embeddings.return_value = []
        adapter.get_resume_section_embeddings("fp-1")
        mock_repo.get_resume_section_embeddings.assert_called_once_with(
            resume_fingerprint="fp-1", section_type=None
        )

    def test_save_evidence_unit_embeddings_with_attr(self):
        adapter, mock_repo = self._make_adapter()
        units = [{'id': 'u-1'}]
        adapter.save_evidence_unit_embeddings("fp-1", units)
        mock_repo.save_evidence_unit_embeddings.assert_called_once_with(
            resume_fingerprint="fp-1", evidence_units=units
        )

    def test_save_evidence_unit_embeddings_without_attr(self):
        """If repo doesn't have save_evidence_unit_embeddings, no error raised."""
        mock_repo = MagicMock(spec=[])  # No attributes
        adapter = JobRepositoryAdapter(mock_repo)
        adapter.save_evidence_unit_embeddings("fp-1", [{'id': 'u-1'}])
        # No exception - hasattr returns False and we skip
