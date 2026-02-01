"""
Pytest configuration and fixtures.

This file provides pytest-specific configuration and fixtures.
For standard test utilities, see tests/__init__.py
"""

import os
import pytest


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "db: marks tests as requiring database (deselect with '-m \"not db\"')"
    )


@pytest.fixture(scope="session")
def test_database():
    """
    Session-scoped fixture that automatically manages the test database container.
    
    Uses testcontainers to start a PostgreSQL with pgvector container before tests
    and stops it after all tests complete. Falls back to external database if
    TEST_DATABASE_URL is set.
    """
    # If TEST_DATABASE_URL is set, use external database
    external_url = os.environ.get("TEST_DATABASE_URL")
    if external_url:
        from tests import check_db_available
        if check_db_available():
            yield external_url
            return
        else:
            pytest.skip("External database not available")
    
    # Try to use testcontainers for automatic container management
    try:
        from testcontainers.postgres import PostgresContainer
        
        # Start PostgreSQL with pgvector
        postgres = PostgresContainer(
            image="ankane/pgvector:latest",
            username="testuser",
            password="testpass",
            dbname="jobscout_test",
            port=5432
        )
        postgres.start()
        
        # Get connection URL
        db_url = postgres.get_connection_url()
        
        # Set environment variable for tests to use
        os.environ["TEST_DATABASE_URL"] = db_url
        
        # Create tables (first create pgvector extension)
        from sqlalchemy import create_engine, text
        from database.models import Base
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            conn.commit()
        Base.metadata.create_all(engine)
        
        print(f"\n✓ Test database started: {db_url}")
        
        yield db_url
        
        # Cleanup after all tests
        postgres.stop()
        print("\n✓ Test database stopped")
        
    except Exception as e:
        import traceback
        print(f"\n⚠ Failed to start test database container:")
        print(f"   {e}")
        print(f"\n   Full traceback:")
        traceback.print_exc()
        pytest.skip(f"Could not start test database container: {e}")


@pytest.fixture(scope="session")
def database_available(test_database):
    """Check if database is available for tests."""
    return True  # If we get here, test_database fixture succeeded


@pytest.fixture(scope="session")
def test_db_url(test_database):
    """Get test database URL."""
    return test_database
