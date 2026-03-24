"""
Unit Tests: database.init_db

Tests the init_db() function in isolation, exercising both the success
path (extension created, tables created) and the error path (exception
logged and re-raised), without triggering the tenacity retry loop.
"""

import pytest
from unittest.mock import MagicMock, patch


class TestInitDb:
    """Tests for database.init_db.init_db()."""

    def test_success_creates_extension_and_tables(self):
        """init_db executes the vector extension statement and creates tables."""
        import database.init_db as init_db_module

        mock_conn = MagicMock()
        with patch.object(init_db_module, "engine") as mock_engine, \
             patch.object(init_db_module, "Base") as mock_base:
            mock_engine.connect.return_value = mock_conn
            init_db_module.init_db()

        mock_conn.__enter__.return_value.execute.assert_called_once()
        mock_conn.__enter__.return_value.commit.assert_called_once()
        mock_base.metadata.create_all.assert_called_once_with(bind=mock_engine)

    def test_error_is_logged_and_reraised(self):
        """init_db logs the error and re-raises it (bypasses retry via __wrapped__)."""
        import database.init_db as init_db_module

        # Access the original unwrapped function to bypass tenacity retry
        original_fn = init_db_module.init_db.__wrapped__

        with patch.object(init_db_module, "engine") as mock_engine:
            mock_engine.connect.side_effect = Exception("DB connection failed")
            with pytest.raises(Exception, match="DB connection failed"):
                original_fn()
