"""
Unit tests for database/repositories/base.py

Directly tests BaseRepository to ensure all branches are covered.
"""

from unittest.mock import MagicMock
from database.repositories.base import BaseRepository


class TestBaseRepository:
    """Direct tests for BaseRepository."""

    def test_init_stores_db_session(self):
        mock_db = MagicMock()
        repo = BaseRepository(mock_db)
        assert repo.db is mock_db

    def test_commit_delegates_to_db(self):
        mock_db = MagicMock()
        repo = BaseRepository(mock_db)
        repo.commit()
        mock_db.commit.assert_called_once_with()

    def test_rollback_delegates_to_db(self):
        mock_db = MagicMock()
        repo = BaseRepository(mock_db)
        repo.rollback()
        mock_db.rollback.assert_called_once_with()

    def test_commit_multiple_times(self):
        mock_db = MagicMock()
        repo = BaseRepository(mock_db)
        repo.commit()
        repo.commit()
        assert mock_db.commit.call_count == 2

    def test_rollback_multiple_times(self):
        mock_db = MagicMock()
        repo = BaseRepository(mock_db)
        repo.rollback()
        repo.rollback()
        assert mock_db.rollback.call_count == 2

    def test_db_is_accessible_after_init(self):
        mock_db = MagicMock()
        repo = BaseRepository(mock_db)
        # Ensure db attribute is the same object
        assert repo.db is mock_db
        assert repo.db is not MagicMock()
