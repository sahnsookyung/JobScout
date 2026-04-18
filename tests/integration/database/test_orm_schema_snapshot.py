"""Parity test: migration chain vs ORM create_all() vs checked-in snapshot.

Uses testcontainers to spin up two ephemeral Postgres instances. One is migrated
via ``database.migrate.migrate_database``, the other built via ``Base.metadata
.create_all()``. The two captured snapshots must match each other and must also
match the checked-in ``tests/fixtures/schema_snapshot.json``.

If the checked-in snapshot is missing, the test reports a clear regeneration
command instead of failing silently.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from tests.fixtures import schema_snapshot

pytestmark = pytest.mark.integration


def _start_pg():
    from tests.conftest import TEST_DB_IMAGE, TEST_DB_NAME, TEST_DB_PASSWORD, TEST_DB_USER
    from testcontainers.postgres import PostgresContainer

    postgres = PostgresContainer(
        image=TEST_DB_IMAGE,
        username=TEST_DB_USER,
        password=TEST_DB_PASSWORD,
        dbname=TEST_DB_NAME,
        port=5432,
    )
    postgres.start()
    return postgres


def _capture_migrations_path() -> dict:
    postgres = _start_pg()
    try:
        from database.migrate import migrate_database

        url = postgres.get_connection_url()
        engine = create_engine(url)
        try:
            migrate_database(engine=engine)
            return schema_snapshot.capture(engine)
        finally:
            engine.dispose()
    finally:
        postgres.stop()


def _capture_create_all_path() -> dict:
    postgres = _start_pg()
    try:
        from database.models import Base

        url = postgres.get_connection_url()
        engine = create_engine(url)
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            Base.metadata.create_all(engine)
            return schema_snapshot.capture(engine)
        finally:
            engine.dispose()
    finally:
        postgres.stop()


def test_migration_path_matches_create_all_path():
    """Dropping the DB and running create_all() produces the same schema as migrations."""
    pytest.importorskip("testcontainers")
    migrations_snapshot = _capture_migrations_path()
    create_all_snapshot = _capture_create_all_path()
    assert migrations_snapshot == create_all_snapshot, (
        "ORM models and migration chain have drifted apart. Compare:\n"
        f"migrations: {schema_snapshot.dump(migrations_snapshot)[:500]}...\n"
        f"create_all: {schema_snapshot.dump(create_all_snapshot)[:500]}...\n"
        "Fix by updating the ORM model (or the migration) so that create_all() matches the migrated schema."
    )


def test_migration_path_matches_checked_in_snapshot():
    """Regressions in the migration chain must be explicit — the snapshot is checked in."""
    pytest.importorskip("testcontainers")
    if not schema_snapshot.SNAPSHOT_PATH.exists():
        pytest.skip(
            "Baseline snapshot missing. Generate via: "
            "`uv run python -m tests.fixtures.schema_snapshot --write --url=$TEST_DATABASE_URL`"
        )

    current = _capture_migrations_path()
    checked_in = schema_snapshot.load()
    assert current == checked_in, (
        "Migration chain schema has drifted from the checked-in snapshot. "
        "If the change is intentional, regenerate the snapshot with: "
        "`uv run python -m tests.fixtures.schema_snapshot --write --url=<db_url>` "
        "and include the diff in your PR description."
    )
