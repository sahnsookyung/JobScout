"""Unit tests for the database migration runner."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
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


def test_migrate_rolls_back_before_unlocking_on_failure(tmp_path: Path) -> None:
    _write_migration(
        tmp_path,
        "def migrate(conn):\n"
        "    raise RuntimeError('boom')\n",
    )

    import database.migrate as migrate_module

    engine, conn = _mock_engine_with_connection()
    with (
        patch.object(migrate_module, "_schema_migrations_exists", return_value=False),
        patch.object(migrate_module, "_app_tables_present", return_value=set()),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            migrate_module.migrate_database(engine=engine, migrations_dir=tmp_path)

    rollback_index = next(
        i for i, call in enumerate(conn.method_calls) if call[0] == "rollback"
    )
    unlock_index = next(
        i
        for i, call in enumerate(conn.method_calls)
        if call[0] == "execute" and "SELECT pg_advisory_unlock" in str(call[1][0])
    )
    assert rollback_index < unlock_index


def test_applied_migrations_returns_empty_when_tracking_table_missing() -> None:
    import database.migrate as migrate_module

    conn = MagicMock()

    with patch.object(migrate_module, "_schema_migrations_exists", return_value=False):
        assert migrate_module._applied_migrations(conn) == {}


def test_applied_migrations_returns_version_map() -> None:
    import database.migrate as migrate_module

    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        SimpleNamespace(version="001_initial_schema.py", checksum="abc"),
        SimpleNamespace(version="002_next.py", checksum="def"),
    ]

    with patch.object(migrate_module, "_schema_migrations_exists", return_value=True):
        assert migrate_module._applied_migrations(conn) == {
            "001_initial_schema.py": "abc",
            "002_next.py": "def",
        }


def test_load_migration_module_requires_loader(tmp_path: Path) -> None:
    path = _write_migration(tmp_path, "def migrate():\n    pass\n")

    import database.migrate as migrate_module

    with patch.object(
        migrate_module.importlib.util,
        "spec_from_file_location",
        return_value=SimpleNamespace(loader=None),
    ):
        with pytest.raises(migrate_module.DatabaseSchemaError, match="Unable to load"):
            migrate_module._load_migration_module(path)


def test_run_migration_callable_supports_zero_and_one_argument_signatures() -> None:
    import database.migrate as migrate_module

    zero_arg = MagicMock()
    conn = MagicMock()

    def zero_arg_migrate() -> None:
        zero_arg()

    migrate_module._run_migration_callable(
        SimpleNamespace(__name__="zero_arg", migrate=zero_arg_migrate),
        conn,
    )
    zero_arg.assert_called_once_with()

    one_arg = MagicMock()

    def one_arg_migrate(connection) -> None:
        one_arg(connection)

    migrate_module._run_migration_callable(
        SimpleNamespace(__name__="one_arg", migrate=one_arg_migrate),
        conn,
    )
    one_arg.assert_called_once_with(conn)


def test_run_migration_callable_rejects_missing_or_invalid_migrate_function() -> None:
    import database.migrate as migrate_module

    with pytest.raises(migrate_module.DatabaseSchemaError, match="has no migrate"):
        migrate_module._run_migration_callable(SimpleNamespace(__name__="missing"), MagicMock())

    def too_many_args(arg1, arg2):
        return None

    with pytest.raises(migrate_module.DatabaseSchemaError, match="must accept 0 or 1 args"):
        migrate_module._run_migration_callable(
            SimpleNamespace(__name__="bad_sig", migrate=too_many_args),
            MagicMock(),
        )


def test_check_database_schema_requires_migration_files(tmp_path: Path) -> None:
    import database.migrate as migrate_module

    with pytest.raises(migrate_module.DatabaseSchemaError, match="No migration files found"):
        migrate_module.check_database_schema(engine=MagicMock(), migrations_dir=tmp_path)


def test_check_database_schema_disposes_created_engine(tmp_path: Path) -> None:
    _write_migration(tmp_path, "def migrate(conn):\n    conn.execute('SELECT 1')\n")

    import database.migrate as migrate_module

    engine, conn = _mock_engine_with_connection()
    with (
        patch.object(migrate_module, "create_engine", return_value=engine),
        patch.object(migrate_module, "get_database_url", return_value="postgresql://example"),
        patch.object(migrate_module, "_schema_migrations_exists", return_value=True),
        patch.object(migrate_module, "_app_tables_present", return_value={"job_post"}),
        patch.object(migrate_module, "_validate_schema_state"),
    ):
        migrate_module.check_database_schema(engine=None, migrations_dir=tmp_path)

    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("SELECT pg_advisory_lock" in sql for sql in executed_sql)
    engine.dispose.assert_called_once()


@pytest.mark.parametrize(
    ("argv", "applied", "expected"),
    [
        (["database.migrate", "--check"], None, 0),
        (["database.migrate"], ["001_initial_schema.py"], 0),
        (["database.migrate"], [], 0),
    ],
)
def test_main_success_paths(monkeypatch: pytest.MonkeyPatch, argv: list[str], applied: list[str] | None, expected: int) -> None:
    import database.migrate as migrate_module

    monkeypatch.setattr("sys.argv", argv)

    with (
        patch.object(migrate_module, "check_database_schema") as check_schema,
        patch.object(migrate_module, "migrate_database", return_value=applied or []) as migrate_database,
    ):
        assert migrate_module.main() == expected

    if "--check" in argv:
        check_schema.assert_called_once_with()
        migrate_database.assert_not_called()
    else:
        migrate_database.assert_called_once_with()


def test_main_returns_non_zero_on_schema_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import database.migrate as migrate_module

    monkeypatch.setattr("sys.argv", ["database.migrate", "--check"])

    with patch.object(
        migrate_module,
        "check_database_schema",
        side_effect=migrate_module.DatabaseSchemaError("schema broken"),
    ):
        assert migrate_module.main() == 1
