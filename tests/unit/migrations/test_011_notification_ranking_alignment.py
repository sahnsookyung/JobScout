"""Unit tests for notification ranking alignment migration."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import Mock


MODULE_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "011_notification_ranking_alignment.py"
)


def _load_module():
    spec = spec_from_file_location("migration_011_notification_ranking_alignment", MODULE_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_migrate_adds_ranking_snapshot_and_renames_threshold() -> None:
    module = _load_module()
    conn = Mock()

    module.migrate(conn)

    assert conn.execute.call_count == 2
    rename_sql = str(conn.execute.call_args_list[0].args[0])
    add_sql = str(conn.execute.call_args_list[1].args[0])
    assert "min_fit_for_alerts" in rename_sql
    assert "ADD COLUMN IF NOT EXISTS ranking_snapshot JSONB" in add_sql


def test_rollback_drops_snapshot_and_restores_threshold_name() -> None:
    module = _load_module()
    conn = Mock()

    module.rollback(conn)

    assert conn.execute.call_count == 2
    drop_sql = str(conn.execute.call_args_list[0].args[0])
    rename_sql = str(conn.execute.call_args_list[1].args[0])
    assert "DROP COLUMN IF EXISTS ranking_snapshot" in drop_sql
    assert "min_score_threshold" in rename_sql
