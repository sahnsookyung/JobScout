"""
Pytest configuration and fixtures.

This file provides pytest-specific configuration and fixtures.
For standard test utilities, see tests/__init__.py
"""

import os
import pytest


@pytest.fixture(autouse=True)
def clean_env():
    """Backup and restore environment to prevent test pollution.

    This fixture ensures environment variables set by one test don't
    affect other tests. It backs up os.environ before each test
    and restores it after, preventing pollution from integration
    tests that set module-level environment variables.
    """
    env_backup = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(env_backup)


@pytest.fixture(autouse=True)
def _block_production_db(clean_env):  # noqa: PT004  (runs after clean_env saves backup)
    """Block production database access during tests.

    Removes DATABASE_URL so tests cannot accidentally write to the production DB.
    Tests that need a database must use the ``test_database`` fixture, which
    provides an isolated container.  ``clean_env`` (a dependency here) has
    already saved the env backup before this fixture removes the variable,
    so the original value is automatically restored after each test.
    """
    if os.environ.pop("DATABASE_URL", None):
        import warnings
        warnings.warn(
            "Test blocked from accessing production DATABASE_URL. "
            "Use the test_database fixture for DB tests.",
            stacklevel=2,
        )
    yield


# Test database credentials (not production credentials)
TEST_DB_USER = "testuser"
TEST_DB_PASSWORD = os.environ.get("TEST_DB_PASSWORD", "testpass")
TEST_DB_NAME = "jobscout_test"


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "db: marks tests as requiring database (deselect with '-m \"not db\"')"
    )
    config.addinivalue_line(
        "markers", "redis: marks tests as requiring Redis (deselect with '-m \"not redis\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as cross-service/integration coverage"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slower end-to-end or container-backed coverage"
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
            username=TEST_DB_USER,
            password=TEST_DB_PASSWORD,
            dbname=TEST_DB_NAME,
            port=5432
        )
        postgres.start()
        
        # Get connection URL
        db_url = postgres.get_connection_url()
        
        # Set environment variable for tests to use
        os.environ["TEST_DATABASE_URL"] = db_url
        
        # Create tables (first create pgvector extension)
        from sqlalchemy import create_engine
        from database.migrate import migrate_database
        engine = create_engine(db_url)
        migrate_database(engine=engine)
        
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


@pytest.fixture(scope="session")
def redis_container():
    """Session-scoped fixture that provides a Redis container for tests.

    Uses testcontainers to start a Redis container before tests and stop it after.
    Falls back to external Redis if TEST_REDIS_URL is set.
    """
    # If TEST_REDIS_URL is set, use external Redis
    external_url = os.environ.get("TEST_REDIS_URL")
    if external_url:
        port = external_url.split(":")[-1].split("/")[0] if ":" in external_url else "6379"
        yield {"url": external_url, "port": port}
        return

    # Try to use testcontainers for automatic container management
    try:
        from testcontainers.redis import RedisContainer

        # Start Redis container
        redis = RedisContainer("redis:7-alpine")
        redis.start()

        # Build connection URL from exposed host/port
        host = redis.get_container_host_ip()
        port = redis.get_exposed_port(6379)
        redis_url = f"redis://{host}:{port}"
        os.environ["TEST_REDIS_URL"] = redis_url

        print(f"\n✓ Test Redis container started: {redis_url}")

        yield {"container": redis, "url": redis_url, "port": port}

        # Cleanup after all tests
        redis.stop()
        print("\n✓ Test Redis container stopped")

    except Exception as e:
        import traceback
        print(f"\n⚠ Failed to start test Redis container:")
        print(f"   {e}")
        print(f"\n   Full traceback:")
        traceback.print_exc()
        pytest.skip(f"Could not start test Redis container: {e}")


@pytest.fixture
def redis_url(redis_container):
    """Get Redis connection URL from container."""
    return redis_container["url"]


@pytest.fixture(autouse=True)
def reset_redis_module_state():
    """Reset Redis module state between tests to prevent pollution.

    This fixture resets the connection pool in redis_streams
    to ensure tests don't share state.
    """
    from core import redis_streams
    # Backup original state
    original_connection_pool = redis_streams._connection_pool

    yield

    # Reset connection pool to force recreation
    redis_streams._connection_pool = original_connection_pool
    if original_connection_pool is not None:
        try:
            original_connection_pool.disconnect()
        except Exception:
            pass  # Ignore errors on disconnect


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
