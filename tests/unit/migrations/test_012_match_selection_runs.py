"""Unit tests for match selection run migration."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import Mock


MODULE_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "012_match_selection_runs.py"
)


def _load_module():
    spec = spec_from_file_location("migration_012_match_selection_runs", MODULE_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_migrate_creates_selection_run_and_item_tables() -> None:
    module = _load_module()
    conn = Mock()

    module.migrate(conn)

    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS match_selection_run" in sql for sql in executed_sql)
    assert any("CREATE TABLE IF NOT EXISTS match_selection_item" in sql for sql in executed_sql)
    assert any("idx_match_selection_run_current" in sql for sql in executed_sql)


def test_rollback_drops_selection_tables() -> None:
    module = _load_module()
    conn = Mock()

    module.rollback(conn)

    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert "DROP TABLE IF EXISTS match_selection_item" in executed_sql[0]
    assert "DROP TABLE IF EXISTS match_selection_run" in executed_sql[1]
