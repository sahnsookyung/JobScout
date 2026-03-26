"""
Fixtures for service unit tests.

Service lifespans now call init_db() on startup. Unit tests use TestClient
which triggers the lifespan, so init_db must be mocked out — there is no
real database in the unit test environment.
"""

import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def mock_init_db():
    """Suppress init_db in all service lifespan handlers during unit tests."""
    with (
        patch("services.orchestrator.main.init_db"),
        patch("services.extraction.main.init_db"),
        patch("services.embeddings.main.init_db"),
        patch("services.scorer_matcher.main.init_db"),
    ):
        yield
