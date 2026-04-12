"""Tests for the job_post fingerprint uniqueness repair migration."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock


def _load_migration_module():
    path = (
        Path(__file__).resolve().parents[3]
        / "migrations"
        / "014_job_post_fingerprint_scope.py"
    )
    spec = importlib.util.spec_from_file_location("migration_014", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_rebuilds_fingerprint_uniqueness_for_tenant_and_global_rows() -> None:
    conn = MagicMock()
    migration_014 = _load_migration_module()

    migration_014.migrate(conn)

    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any(
        "DROP CONSTRAINT IF EXISTS uq_job_post_fingerprint" in sql
        for sql in executed_sql
    )
    assert any(
        "PARTITION BY" in sql and "canonical_fingerprint" in sql and "__global__" in sql
        for sql in executed_sql
    )
    assert any(
        "CREATE UNIQUE INDEX uq_job_post_tenant_fingerprint" in sql
        for sql in executed_sql
    )
    assert any(
        "CREATE UNIQUE INDEX uq_job_post_global_fingerprint" in sql
        for sql in executed_sql
    )


def test_rollback_restores_legacy_unique_constraint_after_deduplication() -> None:
    conn = MagicMock()
    migration_014 = _load_migration_module()

    migration_014.rollback(conn)

    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any(
        "DROP INDEX IF EXISTS uq_job_post_tenant_fingerprint" in sql
        for sql in executed_sql
    )
    assert any(
        "DROP INDEX IF EXISTS uq_job_post_global_fingerprint" in sql
        for sql in executed_sql
    )
    assert any(
        "PARTITION BY fingerprint_version, canonical_fingerprint" in sql
        for sql in executed_sql
    )
    assert any(
        "ADD CONSTRAINT uq_job_post_fingerprint" in sql
        for sql in executed_sql
    )
