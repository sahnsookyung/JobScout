"""Unit tests for the database migration runner."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _write_migration(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "001_initial_schema.py"
    path.write_text(body)
    return path


def _mock_engine_with_connection() -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    return engine, conn


def test_check_requires_initialized_schema(tmp_path: Path) -> None:
    """check_database_schema should fail on an empty database."""
    _write_migration(tmp_path, "def migrate(conn):\n    conn.execute('SELECT 1')\n")

    import database.migrate as migrate_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(migrate_module, "_schema_migrations_exists", return_value=False),
        patch.object(migrate_module, "_app_tables_present", return_value=set()),
    ):
        with pytest.raises(migrate_module.DatabaseSchemaError, match="not initialized"):
            migrate_module.check_database_schema(engine=engine, migrations_dir=tmp_path)


def test_check_rejects_stale_pre_reset_database(tmp_path: Path) -> None:
    """check_database_schema should reject old databases without the new baseline marker."""
    _write_migration(tmp_path, "def migrate(conn):\n    conn.execute('SELECT 1')\n")

    import database.migrate as migrate_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(migrate_module, "_schema_migrations_exists", return_value=False),
        patch.object(migrate_module, "_app_tables_present", return_value={"job_post"}),
    ):
        with pytest.raises(migrate_module.DatabaseSchemaError, match="Unsupported pre-reset"):
            migrate_module.check_database_schema(engine=engine, migrations_dir=tmp_path)


def test_migrate_applies_baseline_and_records_checksum(tmp_path: Path) -> None:
    """migrate_database should run the baseline migration and record it."""
    _write_migration(
        tmp_path,
        "from sqlalchemy import text\n"
        "def migrate(conn):\n"
        "    conn.execute(text('SELECT 1'))\n",
    )

    import database.migrate as migrate_module

    engine, conn = _mock_engine_with_connection()
    with (
        patch.object(migrate_module, "_schema_migrations_exists", return_value=False),
        patch.object(migrate_module, "_app_tables_present", return_value=set()),
    ):
        applied = migrate_module.migrate_database(engine=engine, migrations_dir=tmp_path)

    assert applied == ["001_initial_schema.py"]
    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("CREATE EXTENSION IF NOT EXISTS vector" in sql for sql in executed_sql)
    assert any("INSERT INTO schema_migrations" in sql for sql in executed_sql)


def test_migrate_rejects_stale_pre_reset_database(tmp_path: Path) -> None:
    """migrate_database should refuse to stamp or mutate an unsupported old schema."""
    _write_migration(tmp_path, "def migrate(conn):\n    conn.execute('SELECT 1')\n")

    import database.migrate as migrate_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(migrate_module, "_schema_migrations_exists", return_value=False),
        patch.object(migrate_module, "_app_tables_present", return_value={"job_post"}),
    ):
        with pytest.raises(migrate_module.DatabaseSchemaError, match="Unsupported pre-reset"):
            migrate_module.migrate_database(engine=engine, migrations_dir=tmp_path)


def test_check_fails_on_checksum_mismatch(tmp_path: Path) -> None:
    """check_database_schema should hard fail when a committed migration changes."""
    path = _write_migration(tmp_path, "def migrate(conn):\n    conn.execute('SELECT 1')\n")

    import database.migrate as migrate_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(migrate_module, "_schema_migrations_exists", return_value=True),
        patch.object(migrate_module, "_app_tables_present", return_value={"job_post"}),
        patch.object(
            migrate_module,
            "_applied_migrations",
            return_value={path.name: "incorrect-checksum"},
        ),
    ):
        with pytest.raises(migrate_module.DatabaseSchemaError, match="checksum mismatch"):
            migrate_module.check_database_schema(engine=engine, migrations_dir=tmp_path)
