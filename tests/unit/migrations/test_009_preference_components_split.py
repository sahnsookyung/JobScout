"""Unit tests for preference_components split migration."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import Mock


MODULE_PATH = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "009_preference_components_split.py"
)


def _load_module():
    spec = spec_from_file_location("migration_009_preference_components_split", MODULE_PATH)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_rollback_merges_preference_components_back_before_drop() -> None:
    module = _load_module()
    conn = Mock()

    module.rollback(conn)

    assert conn.execute.call_count == 3
    merge_sql = str(conn.execute.call_args_list[0].args[0])
    rename_sql = str(conn.execute.call_args_list[1].args[0])
    drop_sql = str(conn.execute.call_args_list[2].args[0])
    assert "UPDATE job_match" in merge_sql
    assert "fit_components = COALESCE" in merge_sql
    assert "preferred_requirement_coverage" in rename_sql
    assert "DROP COLUMN IF EXISTS preference_components" in drop_sql
