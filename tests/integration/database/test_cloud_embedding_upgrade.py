"""Reproduce an upgrade from stamped schema 104 with private cloud columns."""

import pytest
from sqlalchemy import create_engine, text

from database import bootstrap
from database.models import Base
from database.schema_snapshot import capture, load
from tests.integration.database.test_orm_schema_snapshot import _start_pg

pytestmark = [pytest.mark.integration, pytest.mark.db]


def test_embedding_upgrade_preserves_private_retention_column():
    postgres = _start_pg()
    engine = create_engine(postgres.get_connection_url())
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            Base.metadata.create_all(conn)
            conn.execute(text("ALTER TABLE users ADD COLUMN retention_claimed_at timestamptz"))
            for table in ('job_post', 'job_match', 'job_requirement_unit_embedding',
                          'resume_section_embedding', 'resume_evidence_unit_embedding'):
                conn.exec_driver_sql(f'ALTER TABLE {table} DROP COLUMN embedding_space')
            conn.execute(text("ALTER TABLE resume_section_embedding DROP COLUMN query_embedding"))
            bootstrap._ensure_schema_migrations_table(conn)
            bootstrap._record_migration(conn, version=bootstrap.CURRENT_SCHEMA_VERSION, checksum='previous-baseline')
            for path in bootstrap._numbered_migration_paths():
                if path.name < '105_':
                    bootstrap._record_migration(conn, version=path.name, checksum=bootstrap._migration_checksum(path))
        applied = bootstrap.bootstrap_database(engine=engine)
        assert '105_embedding_spaces.sql' in applied
        actual = capture(engine)
        assert 'retention_claimed_at' in {column['name'] for column in actual['tables']['users']['columns']}
        expected = load()
        assert bootstrap._snapshot_limited_to_expected_tables(actual, expected) == expected
        assert bootstrap.bootstrap_database(engine=engine) == []
    finally:
        engine.dispose()
        postgres.stop()
