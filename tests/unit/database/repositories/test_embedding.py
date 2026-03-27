"""Unit tests for database/repositories/embedding.py"""

import pytest
from unittest.mock import MagicMock

from database.repositories.embedding import EmbeddingRepository


def make_repo():
    mock_db = MagicMock()
    return EmbeddingRepository(mock_db), mock_db


# ---------------------------------------------------------------------------
# find_similar_resume_sections
# ---------------------------------------------------------------------------

class TestFindSimilarResumeSections:
    def test_returns_empty_when_no_results(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.all.return_value = []

        result = repo.find_similar_resume_sections([0.1, 0.2])
        assert result == []

    def test_returns_section_with_similarity(self):
        repo, mock_db = make_repo()
        mock_section = MagicMock()

        row = MagicMock()
        row.__getitem__.return_value = mock_section
        row._mapping = {'distance': 0.3}
        mock_db.execute.return_value.all.return_value = [row]

        result = repo.find_similar_resume_sections([0.1])

        assert len(result) == 1
        section, similarity = result[0]
        assert section is mock_section
        assert similarity == pytest.approx(0.7)  # 1 - 0.3

    def test_returns_multiple_results(self):
        repo, mock_db = make_repo()
        rows = []
        for d in [0.1, 0.2, 0.4]:
            row = MagicMock()
            row.__getitem__.return_value = MagicMock()
            row._mapping = {'distance': d}
            rows.append(row)
        mock_db.execute.return_value.all.return_value = rows

        result = repo.find_similar_resume_sections([0.1], top_k=3)
        assert len(result) == 3

    def test_filters_by_section_type(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.all.return_value = []

        repo.find_similar_resume_sections([0.1], section_type="summary")
        mock_db.execute.assert_called_once()

    def test_no_section_type_filter(self):
        repo, mock_db = make_repo()
        mock_db.execute.return_value.all.return_value = []

        repo.find_similar_resume_sections([0.1])
        mock_db.execute.assert_called_once()

    def test_similarity_computed_from_distance(self):
        repo, mock_db = make_repo()
        row = MagicMock()
        row.__getitem__.return_value = MagicMock()
        row._mapping = {'distance': 0.0}
        mock_db.execute.return_value.all.return_value = [row]

        result = repo.find_similar_resume_sections([0.1])
        _, sim = result[0]
        assert sim == pytest.approx(1.0)

    def test_default_top_k_is_ten(self):
        """Default top_k=10 doesn't raise and queries correctly."""
        repo, mock_db = make_repo()
        mock_db.execute.return_value.all.return_value = []

        repo.find_similar_resume_sections([0.1, 0.2, 0.3])
        mock_db.execute.assert_called_once()
