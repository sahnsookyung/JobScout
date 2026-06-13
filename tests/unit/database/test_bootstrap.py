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


def test_bootstrap_adopts_matching_legacy_migration_history() -> None:
    """bootstrap_database should stamp old numbered migrations after snapshot verification."""
    import database.bootstrap as bootstrap_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=True),
        patch.object(bootstrap_module, "_app_tables_present", return_value={"job_post"}),
        patch.object(
            bootstrap_module,
            "_applied_migrations",
            return_value={"001_initial_schema.py": "legacy-checksum"},
        ),
        patch.object(bootstrap_module, "_upgrade_legacy_schema_to_current") as upgrade_schema,
        patch.object(bootstrap_module, "_verify_bootstrapped_schema") as verify_schema,
        patch.object(bootstrap_module, "_stamp_current_schema") as stamp_schema,
    ):
        assert bootstrap_module.bootstrap_database(engine=engine) == [
            bootstrap_module.CURRENT_SCHEMA_VERSION
        ]

    upgrade_schema.assert_called_once()
    verify_schema.assert_called_once()
    stamp_schema.assert_called_once()


def test_bootstrap_upgrades_stamped_schema_checksum_mismatch() -> None:
    """bootstrap_database should apply safe additive ORM upgrades for stamped schemas."""
    import database.bootstrap as bootstrap_module

    engine, _ = _mock_engine_with_connection()
    with (
        patch.object(bootstrap_module, "_schema_migrations_exists", return_value=True),
        patch.object(bootstrap_module, "_app_tables_present", return_value={"job_post"}),
        patch.object(
            bootstrap_module,
            "_applied_migrations",
            return_value={bootstrap_module.CURRENT_SCHEMA_VERSION: "old-checksum"},
        ),
        patch.object(bootstrap_module, "_schema_checksum", return_value="expected-checksum"),
        patch.object(bootstrap_module.Base.metadata, "create_all") as create_all,
        patch.object(
            bootstrap_module,
            "_apply_current_additive_schema_upgrades",
        ) as apply_additive_upgrades,
        patch.object(bootstrap_module, "_verify_bootstrapped_schema") as verify_schema,
        patch.object(bootstrap_module, "_stamp_current_schema") as stamp_schema,
    ):
        assert bootstrap_module.bootstrap_database(engine=engine) == [
            bootstrap_module.CURRENT_SCHEMA_VERSION
        ]

    create_all.assert_called_once()
    apply_additive_upgrades.assert_called_once()
    verify_schema.assert_called_once()
    stamp_schema.assert_called_once()


def test_verify_bootstrapped_schema_ignores_non_oss_tables() -> None:
    """OSS schema verification should tolerate private SaaS tables in the same DB."""
    import database.bootstrap as bootstrap_module

    expected = {
        "extensions": ["vector"],
        "enums": [{"name": "file_type", "labels": ["resume"]}],
        "tables": {
            "users": {
                "columns": [
                    {
                        "name": "id",
                        "type": "uuid",
                        "nullable": False,
                        "default": None,
                    }
                ]
            }
        },
        "indexes": [
            {
                "name": "idx_users_email",
                "table": "users",
                "access_method": "btree",
                "unique": True,
                "predicate": None,
                "reloptions": None,
                "definition": "CREATE UNIQUE INDEX idx_users_email ON public.users USING btree (email)",
            }
        ],
        "constraints": [
            {
                "table": "users",
                "name": "users_pkey",
                "type": "PRIMARY KEY",
                "definition": "PRIMARY KEY (id)",
            }
        ],
    }
    actual = {
        **expected,
        "extensions": ["plpgsql", *expected["extensions"]],
        "enums": [
            *expected["enums"],
            {"name": "saas_status", "labels": ["active"]},
        ],
        "tables": {
            **expected["tables"],
            "tenant_integration": {"columns": []},
        },
        "indexes": [
            *expected["indexes"],
            {
                "name": "idx_tenant_integration",
                "table": "tenant_integration",
                "access_method": "btree",
                "unique": False,
                "predicate": None,
                "reloptions": None,
                "definition": "CREATE INDEX idx_tenant_integration ON public.tenant_integration USING btree (tenant_id)",
            },
            {
                "name": "idx_users_saas_shadow",
                "table": "users",
                "access_method": "btree",
                "unique": False,
                "predicate": None,
                "reloptions": None,
                "definition": "CREATE INDEX idx_users_saas_shadow ON public.users USING btree (created_at)",
            },
        ],
        "constraints": [
            *expected["constraints"],
            {
                "table": "tenant_integration",
                "name": "tenant_integration_pkey",
                "type": "PRIMARY KEY",
                "definition": "PRIMARY KEY (id)",
            },
            {
                "table": "users",
                "name": "users_saas_shadow_chk",
                "type": "CHECK",
                "definition": "CHECK (email IS NOT NULL)",
            },
        ],
    }

    with (
        patch.object(bootstrap_module, "load", return_value=expected),
        patch.object(bootstrap_module, "capture", return_value=actual),
    ):
            bootstrap_module._verify_bootstrapped_schema(MagicMock())


def test_verify_bootstrapped_schema_still_detects_missing_extension() -> None:
    import database.bootstrap as bootstrap_module

    expected = {
        "extensions": ["vector"],
        "enums": [],
        "tables": {"users": {"columns": [{"name": "id", "type": "uuid"}]}},
        "indexes": [],
        "constraints": [],
    }
    actual = {**expected, "extensions": ["plpgsql"]}

    with (
        patch.object(bootstrap_module, "load", return_value=expected),
        patch.object(bootstrap_module, "capture", return_value=actual),
    ):
        with pytest.raises(bootstrap_module.DatabaseSchemaError, match="schema drifted"):
            bootstrap_module._verify_bootstrapped_schema(MagicMock())


def test_verify_bootstrapped_schema_still_detects_missing_index() -> None:
    import database.bootstrap as bootstrap_module

    expected = {
        "extensions": ["vector"],
        "enums": [],
        "tables": {"users": {"columns": [{"name": "id", "type": "uuid"}]}},
        "indexes": [
            {
                "name": "idx_users_email",
                "table": "users",
                "access_method": "btree",
                "unique": True,
                "predicate": None,
                "reloptions": None,
                "definition": "CREATE UNIQUE INDEX idx_users_email ON public.users USING btree (email)",
            }
        ],
        "constraints": [],
    }
    actual = {**expected, "indexes": []}

    with (
        patch.object(bootstrap_module, "load", return_value=expected),
        patch.object(bootstrap_module, "capture", return_value=actual),
    ):
        with pytest.raises(bootstrap_module.DatabaseSchemaError, match="schema drifted"):
            bootstrap_module._verify_bootstrapped_schema(MagicMock())


def test_verify_bootstrapped_schema_still_detects_oss_drift() -> None:
    import database.bootstrap as bootstrap_module

    expected = {
        "extensions": ["vector"],
        "enums": [],
        "tables": {"users": {"columns": [{"name": "id", "type": "uuid"}]}},
        "indexes": [],
        "constraints": [],
    }
    actual = {
        **expected,
        "tables": {"users": {"columns": [{"name": "email", "type": "text"}]}},
    }

    with (
        patch.object(bootstrap_module, "load", return_value=expected),
        patch.object(bootstrap_module, "capture", return_value=actual),
    ):
        with pytest.raises(bootstrap_module.DatabaseSchemaError, match="schema drifted"):
            bootstrap_module._verify_bootstrapped_schema(MagicMock())


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
