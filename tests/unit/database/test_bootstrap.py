"""Unit tests for the database schema bootstrap runner."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def _mock_engine_with_connection() -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    return engine, conn


def test_check_requires_initialized_schema() -> None:
    """check_database_schema should fail on an empty database."""
    import database.bootstrap as bootstrap_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=False),
        patch.object(bootstrap_module, "_app_tables_present", return_value=set()),
    ):
        with pytest.raises(bootstrap_module.DatabaseSchemaError, match="not initialized"):
            bootstrap_module.check_database_schema(engine=engine)


def test_check_rejects_unstamped_existing_database() -> None:
    """check_database_schema should reject old databases without a schema stamp."""
    import database.bootstrap as bootstrap_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=False),
        patch.object(bootstrap_module, "_app_tables_present", return_value={"job_post"}),
    ):
        with pytest.raises(bootstrap_module.DatabaseSchemaError, match="Unsupported existing"):
            bootstrap_module.check_database_schema(engine=engine)


def test_bootstrap_creates_empty_database() -> None:
    """bootstrap_database should bootstrap an empty database and stamp the current schema."""
    import database.bootstrap as bootstrap_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=False),
        patch.object(bootstrap_module, "_app_tables_present", return_value=set()),
        patch.object(bootstrap_module, "_ensure_extension") as ensure_extension,
        patch.object(bootstrap_module, "_bootstrap_schema") as bootstrap_schema,
    ):
        applied = bootstrap_module.bootstrap_database(engine=engine)

    assert applied == [bootstrap_module.CURRENT_SCHEMA_VERSION]
    ensure_extension.assert_called_once()
    bootstrap_schema.assert_called_once()


def test_bootstrap_rejects_unstamped_existing_database() -> None:
    """bootstrap_database should refuse to mutate an unsupported existing schema."""
    import database.bootstrap as bootstrap_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=False),
        patch.object(bootstrap_module, "_app_tables_present", return_value={"job_post"}),
    ):
        with pytest.raises(bootstrap_module.DatabaseSchemaError, match="Unsupported existing"):
            bootstrap_module.bootstrap_database(engine=engine)


def test_check_fails_on_checksum_mismatch() -> None:
    """check_database_schema should hard fail when the schema stamp drifts."""
    import database.bootstrap as bootstrap_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=True),
        patch.object(bootstrap_module, "_app_tables_present", return_value={"job_post"}),
        patch.object(
            bootstrap_module,
            "_applied_migrations",
            return_value={bootstrap_module.CURRENT_SCHEMA_VERSION: "incorrect-checksum"},
        ),
        patch.object(bootstrap_module, "_schema_checksum", return_value="expected-checksum"),
    ):
        with pytest.raises(bootstrap_module.DatabaseSchemaError, match="schema stamp"):
            bootstrap_module.check_database_schema(engine=engine)


def test_bootstrap_returns_empty_when_schema_is_current() -> None:
    """bootstrap_database should become a no-op when the schema stamp is current."""
    import database.bootstrap as bootstrap_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=True),
        patch.object(bootstrap_module, "_app_tables_present", return_value={"job_post"}),
        patch.object(
            bootstrap_module,
            "_applied_migrations",
            return_value={bootstrap_module.CURRENT_SCHEMA_VERSION: "expected-checksum"},
        ),
        patch.object(bootstrap_module, "_schema_checksum", return_value="expected-checksum"),
    ):
        assert bootstrap_module.bootstrap_database(engine=engine) == []


def test_validate_known_versions_rejects_unknown_schema_versions() -> None:
    import database.bootstrap as bootstrap_module

    with pytest.raises(bootstrap_module.DatabaseSchemaError, match="unknown schema versions"):
        bootstrap_module._validate_known_versions(
            {
                bootstrap_module.CURRENT_SCHEMA_VERSION: "ok",
                "legacy_migration_head": "old",
            }
        )


def test_applied_migrations_returns_empty_when_tracking_table_missing() -> None:
    import database.bootstrap as bootstrap_module

    conn = MagicMock()

    with patch.object(bootstrap_module, "_schema_migrations_exists", return_value=False):
        assert bootstrap_module._applied_migrations(conn) == {}


def test_applied_migrations_returns_version_map() -> None:
    import database.bootstrap as bootstrap_module

    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        SimpleNamespace(version=bootstrap_module.CURRENT_SCHEMA_VERSION, checksum="abc"),
    ]

    with patch.object(bootstrap_module, "_schema_migrations_exists", return_value=True):
        assert bootstrap_module._applied_migrations(conn) == {
            bootstrap_module.CURRENT_SCHEMA_VERSION: "abc",
        }


def test_bootstrap_rolls_back_before_unlocking_on_failure() -> None:
    import database.bootstrap as bootstrap_module

    engine, conn = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=False),
        patch.object(bootstrap_module, "_app_tables_present", return_value=set()),
        patch.object(bootstrap_module, "_bootstrap_schema", side_effect=RuntimeError("boom")),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            bootstrap_module.bootstrap_database(engine=engine)

    rollback_index = next(
        index for index, call in enumerate(conn.method_calls) if call[0] == "rollback"
    )
    unlock_index = next(
        index
        for index, call in enumerate(conn.method_calls)
        if call[0] == "execute" and "SELECT pg_advisory_unlock" in str(call[1][0])
    )
    assert rollback_index < unlock_index


def test_check_database_schema_disposes_created_engine() -> None:
    import database.bootstrap as bootstrap_module

    engine, conn = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "create_engine", return_value=engine),
        patch.object(bootstrap_module, "get_database_url", return_value="postgresql://example"),
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=True),
        patch.object(bootstrap_module, "_app_tables_present", return_value={"job_post"}),
        patch.object(bootstrap_module, "_validate_schema_state"),
    ):
        bootstrap_module.check_database_schema(engine=None)

    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("SELECT pg_advisory_lock" in sql for sql in executed_sql)
    engine.dispose.assert_called_once()


@pytest.mark.parametrize(
    ("argv", "applied", "expected"),
    [
        (["database.bootstrap", "--check"], None, 0),
        (["database.bootstrap"], ["orm_bootstrap"], 0),
        (["database.bootstrap"], [], 0),
    ],
)
def test_main_success_paths(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    applied: list[str] | None,
    expected: int,
) -> None:
    import database.bootstrap as bootstrap_module

    monkeypatch.setattr("sys.argv", argv)

    with (
        patch.object(bootstrap_module, "check_database_schema") as check_schema,
        patch.object(bootstrap_module, "bootstrap_database", return_value=applied or []) as bootstrap_database,
    ):
        assert bootstrap_module.main() == expected

    if "--check" in argv:
        check_schema.assert_called_once_with()
        bootstrap_database.assert_not_called()
    else:
        bootstrap_database.assert_called_once_with()


def test_main_returns_non_zero_on_schema_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import database.bootstrap as bootstrap_module

    monkeypatch.setattr("sys.argv", ["database.bootstrap", "--check"])

    with patch.object(
        bootstrap_module,
        "check_database_schema",
        side_effect=bootstrap_module.DatabaseSchemaError("schema broken"),
    ):
        assert bootstrap_module.main() == 1
