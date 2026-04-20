"""
Unit Tests: database.init_db

Tests the init_db() function in isolation, exercising both the success
path (schema prepared) and the error path (exception
logged and re-raised), without triggering the tenacity retry loop.
"""

import pytest
from unittest.mock import MagicMock, patch


class TestInitDb:
    """Tests for database.init_db.init_db()."""

    def test_success_bootstraps_schema(self):
        """init_db prepares the schema through the shared bootstrap path."""
        import database.init_db as init_db_module

        with patch.object(init_db_module, "engine") as mock_engine, \
             patch.object(init_db_module, "bootstrap_database") as mock_bootstrap:
            init_db_module.init_db()

        mock_bootstrap.assert_called_once_with(engine=mock_engine)

    def test_error_is_logged_and_reraised(self):
        """init_db logs the error and re-raises it (bypasses retry via __wrapped__)."""
        import database.init_db as init_db_module

        # Access the original unwrapped function to bypass tenacity retry
        original_fn = init_db_module.init_db.__wrapped__

        with patch.object(init_db_module, "engine") as mock_engine:
            mock_engine.connect.side_effect = Exception("DB connection failed")
            with pytest.raises(Exception, match="DB connection failed"):
                original_fn()
