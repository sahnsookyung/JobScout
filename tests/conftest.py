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
        
        # Create ENUM types for user_files table (not created by Base.metadata.create_all)
        with engine.connect() as conn:
            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'upload_status') THEN
                        CREATE TYPE upload_status AS ENUM ('pending', 'scanned', 'rejected', 'ready');
                    END IF;
                END
                $$;
            """))
            
            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'file_type') THEN
                        CREATE TYPE file_type AS ENUM ('resume');
                    END IF;
                END
                $$;
            """))
            
            # Check and alter upload_status column
            result = conn.execute(text("""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name = 'user_files' AND column_name = 'upload_status'
            """))
            row = result.fetchone()
            if row and row[0] != 'USER-DEFINED':
                conn.execute(text("""
                    ALTER TABLE user_files ALTER COLUMN upload_status TYPE upload_status 
                    USING upload_status::upload_status
                """))
            
            # Check and alter file_type column
            result = conn.execute(text("""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name = 'user_files' AND column_name = 'file_type'
            """))
            row = result.fetchone()
            if row and row[0] != 'USER-DEFINED':
                conn.execute(text("""
                    ALTER TABLE user_files ALTER COLUMN file_type TYPE file_type 
                    USING file_type::file_type
                """))
            
            # Always set the default for upload_status
            conn.execute(text("""
                ALTER TABLE user_files ALTER COLUMN upload_status SET DEFAULT 'pending'::upload_status
            """))
            
            conn.commit()
        
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


@pytest.fixture(autouse=True)
def redirect_resume_files(monkeypatch, tmp_path):
    """Redirect resume file writes from project root to temp directory during tests.

    This fixture ensures tests don't pollute the project root directory with resume files.
    Intercepts writes to:
    - Relative paths that resolve to project root
    - Absolute paths to project root (after config resolution)

    Note: Uses closure over tmp_path to ensure each test/worker gets isolated temp directory.
    """
    import os
    from pathlib import Path

    _original_open = open
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    tests_dir = os.path.join(project_root, 'tests')

    def patched_open(filename, mode='r', *args, **kwargs):
        """Patch open() to redirect resume file writes from project root to temp."""
        if mode.startswith('w') or mode.startswith('a') or mode.startswith('x'):
            filename_str = str(filename) if isinstance(filename, (str, Path)) else None
            if filename_str:
                abs_path = os.path.abspath(filename_str)
                in_project_root = abs_path.startswith(project_root) and not abs_path.startswith(tests_dir)

                if in_project_root:
                    new_path = str(tmp_path / os.path.basename(filename_str))
                    return _original_open(new_path, mode, *args, **kwargs)

        return _original_open(filename, mode, *args, **kwargs)

    monkeypatch.setattr('builtins.open', patched_open)
