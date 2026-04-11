"""Tests for the source uniqueness repair migration."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock


def _load_migration_module():
    path = (
        Path(__file__).resolve().parents[3]
        / "migrations"
        / "013_job_post_source_tenant_scope.py"
    )
    spec = importlib.util.spec_from_file_location("migration_013", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_rebuilds_tenant_aware_indexes_from_any_prior_shape() -> None:
    conn = MagicMock()
    migration_013 = _load_migration_module()

    migration_013.migrate(conn)

    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any(
        "DROP CONSTRAINT IF EXISTS uq_job_post_source_job_site_url" in sql
        for sql in executed_sql
    )
    assert any(
        "DROP CONSTRAINT IF EXISTS uq_job_post_source_site_url" in sql
        for sql in executed_sql
    )
    assert any(
        "ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenant (id) ON DELETE CASCADE" in sql
        for sql in executed_sql
    )
    assert any(
        "SET tenant_id = job_post.tenant_id" in sql
        for sql in executed_sql
    )
    assert any(
        "CREATE UNIQUE INDEX uq_job_post_source_tenant_site_url" in sql
        for sql in executed_sql
    )
    assert any(
        "CREATE UNIQUE INDEX uq_job_post_source_global_site_url" in sql
        for sql in executed_sql
    )


def test_rollback_deduplicates_before_restoring_global_constraint() -> None:
    conn = MagicMock()
    migration_013 = _load_migration_module()

    migration_013.rollback(conn)

    executed_sql = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert any(
        "DROP INDEX IF EXISTS uq_job_post_source_tenant_site_url" in sql
        for sql in executed_sql
    )
    assert any(
        "DROP INDEX IF EXISTS uq_job_post_source_global_site_url" in sql
        for sql in executed_sql
    )
    assert any(
        "PARTITION BY site, job_url" in sql
        for sql in executed_sql
    )
    assert any(
        "ADD CONSTRAINT uq_job_post_source_site_url" in sql
        for sql in executed_sql
    )
