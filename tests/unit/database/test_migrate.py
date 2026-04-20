"""Unit tests for the deprecated migration shim."""

import pytest


def test_migrate_module_rejects_check_calls() -> None:
    import database.migrate as migrate_module

    with pytest.raises(migrate_module.DatabaseMigrationError, match="no longer supported"):
        migrate_module.check_database_schema()


def test_migrate_module_rejects_migrate_calls() -> None:
    import database.migrate as migrate_module

    with pytest.raises(migrate_module.DatabaseMigrationError, match="no longer supported"):
        migrate_module.migrate_database()


def test_migrate_main_returns_non_zero(caplog) -> None:
    import database.migrate as migrate_module

    result = migrate_module.main()

    assert result == 1
    assert migrate_module.UNSUPPORTED_MESSAGE in caplog.text
