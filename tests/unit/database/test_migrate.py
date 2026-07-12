"""Unit tests for the stable database migration entrypoint."""

from unittest.mock import patch


def test_migrate_module_delegates_check_calls() -> None:
    import database.migrate as migrate_module

    with patch.object(migrate_module, "_check_database_schema") as check_schema:
        migrate_module.check_database_schema()

    check_schema.assert_called_once_with(engine=None)


def test_migrate_module_delegates_migrate_calls() -> None:
    import database.migrate as migrate_module

    with patch.object(
        migrate_module,
        "bootstrap_database",
        return_value=["100_example.sql"],
    ) as bootstrap:
        assert migrate_module.migrate_database() == ["100_example.sql"]

    bootstrap.assert_called_once_with(engine=None)


def test_migrate_main_returns_zero() -> None:
    import database.migrate as migrate_module

    with patch.object(migrate_module, "migrate_database", return_value=[]):
        result = migrate_module.main()

    assert result == 0
