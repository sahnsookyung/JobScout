"""
Pytest configuration and fixtures.

This file provides pytest-specific configuration and fixtures.
For standard test utilities, see tests/__init__.py
"""

import pytest


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "db: marks tests as requiring database (deselect with '-m \"not db\"')"
    )


@pytest.fixture(scope="session")
def database_available():
    """Check if database is available for tests."""
    from tests import check_db_available
    return check_db_available()


@pytest.fixture(scope="session")
def test_db_url():
    """Get test database URL."""
    from tests import get_test_db_url
    return get_test_db_url()
