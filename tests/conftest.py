"""
Pytest configuration and fixtures.

This file provides pytest-specific configuration and fixtures.
For standard test utilities, see tests/__init__.py
"""

import os
from urllib.parse import urlparse, urlunparse
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
    current_db_url = os.environ.get("DATABASE_URL")
    test_db_url = os.environ.get("TEST_DATABASE_URL")
    if current_db_url and current_db_url != test_db_url:
        os.environ.pop("DATABASE_URL", None)
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
TEST_DB_IMAGE = "pgvector/pgvector:pg17"


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
    config.addinivalue_line(
        "markers", "security: marks tests as security or tenant-isolation coverage"
    )
    config.addinivalue_line(
        "markers", "concurrency: marks tests as race/concurrency coverage"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as bounded scalability/performance coverage"
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
            from sqlalchemy import create_engine
            from database.bootstrap import bootstrap_database

            os.environ["DATABASE_URL"] = external_url
            engine = create_engine(external_url)
            try:
                bootstrap_database(engine=engine)
            finally:
                engine.dispose()
            yield external_url
            return
        else:
            pytest.skip("External database not available")
    
    # Try to use testcontainers for automatic container management
    postgres = None
    engine = None
    db_url = None

    try:
        from testcontainers.postgres import PostgresContainer
        
        # Start PostgreSQL with pgvector
        postgres = PostgresContainer(
            image=TEST_DB_IMAGE,
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
        os.environ["DATABASE_URL"] = db_url
        
        # Bootstrap via the same schema entrypoint used by the application.
        from sqlalchemy import create_engine
        from database.bootstrap import bootstrap_database
        from tests.fixtures import schema_snapshot
        engine = create_engine(db_url)
        bootstrap_database(engine=engine)
        if schema_snapshot.SNAPSHOT_PATH.exists():
            assert schema_snapshot.capture(engine) == schema_snapshot.load(), (
                "Bootstrapped schema drifted from the checked-in snapshot. "
                "Regenerate intentionally if this change is expected."
            )
        
        _parsed = urlparse(db_url)
        if _parsed.password:
            safe_netloc = _parsed.netloc.replace(f":{_parsed.password}@", ":***@")
            _safe = urlunparse(_parsed._replace(netloc=safe_netloc))
        else:
            _safe = db_url
        print(f"\n✓ Test database started: {_safe}")
        
        yield db_url
        
    except Exception as e:
        import traceback
        print(f"\n⚠ Failed to start test database container:")
        print(f"   {e}")
        print(f"\n   Full traceback:")
        traceback.print_exc()
        pytest.skip(f"Could not start test database container: {e}")
    finally:
        if engine is not None:
            engine.dispose()
        if postgres is not None:
            postgres.stop()
            print("\n✓ Test database stopped")
        if db_url and os.environ.get("TEST_DATABASE_URL") == db_url:
            os.environ.pop("TEST_DATABASE_URL", None)
        if db_url and os.environ.get("DATABASE_URL") == db_url:
            os.environ.pop("DATABASE_URL", None)


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
    redis = None
    redis_url = None

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

        print(f"\n✓ Test Redis container started: {redis_url}")  # codeql[py/clear-text-logging-sensitive-data] no credentials in redis_url

        yield {"container": redis, "url": redis_url, "port": port}

    except Exception as e:
        import traceback
        print(f"\n⚠ Failed to start test Redis container:")
        print(f"   {e}")
        print(f"\n   Full traceback:")
        traceback.print_exc()
        pytest.skip(f"Could not start test Redis container: {e}")
    finally:
        if redis is not None:
            redis.stop()
            print("\n✓ Test Redis container stopped")
        if redis_url and os.environ.get("TEST_REDIS_URL") == redis_url:
            os.environ.pop("TEST_REDIS_URL", None)


@pytest.fixture
def redis_url(redis_container):
    """Get Redis connection URL from container."""
    return redis_container["url"]


@pytest.fixture(autouse=True)
def _reset_prometheus_metrics():
    """Zero Prometheus counter/histogram children between tests.

    Module-scope Counter/Histogram singletons in ``core.metrics`` would
    otherwise leak counts across tests. Walks the default REGISTRY and
    resets every child sample value. O(metrics × children) per test — tiny.
    """
    from prometheus_client import REGISTRY

    yield

    def _reset_child(child) -> None:
        value = getattr(child, "_value", None)
        if value is not None and hasattr(value, "set"):
            value.set(0)
        # Histogram children expose _buckets (list of counters) and _sum.
        buckets = getattr(child, "_buckets", None)
        if buckets:
            for bucket in buckets:
                if hasattr(bucket, "set"):
                    bucket.set(0)
        bucket_sum = getattr(child, "_sum", None)
        if bucket_sum is not None and hasattr(bucket_sum, "set"):
            bucket_sum.set(0)

    for collector in list(REGISTRY._collector_to_names):
        children = getattr(collector, "_metrics", None)
        if not children:
            # Unlabeled metrics store their single child directly on the
            # collector (no ``_metrics`` map).
            _reset_child(collector)
            continue
        iterable = children.values() if isinstance(children, dict) else children
        for child in iterable:
            _reset_child(child)


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
