"""Repair job_post fingerprint uniqueness for both tenant and global imports."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE job_post
        DROP CONSTRAINT IF EXISTS uq_job_post_fingerprint
    """))
    conn.execute(text("""
        DROP INDEX IF EXISTS uq_job_post_tenant_fingerprint
    """))
    conn.execute(text("""
        DROP INDEX IF EXISTS uq_job_post_global_fingerprint
    """))
    conn.execute(text("""
        WITH ranked_jobs AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        COALESCE(tenant_id::text, '__global__'),
                        fingerprint_version,
                        canonical_fingerprint
                    ORDER BY last_seen_at DESC, first_seen_at DESC, id DESC
                ) AS row_num
            FROM job_post
        )
        DELETE FROM job_post
        WHERE id IN (
            SELECT id
            FROM ranked_jobs
            WHERE row_num > 1
        )
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX uq_job_post_tenant_fingerprint
        ON job_post (tenant_id, fingerprint_version, canonical_fingerprint)
        WHERE tenant_id IS NOT NULL
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX uq_job_post_global_fingerprint
        ON job_post (fingerprint_version, canonical_fingerprint)
        WHERE tenant_id IS NULL
    """))


def rollback(conn: Connection) -> None:
    conn.execute(text("""
        DROP INDEX IF EXISTS uq_job_post_tenant_fingerprint
    """))
    conn.execute(text("""
        DROP INDEX IF EXISTS uq_job_post_global_fingerprint
    """))
    conn.execute(text("""
        WITH ranked_jobs AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY fingerprint_version, canonical_fingerprint
                    ORDER BY last_seen_at DESC, first_seen_at DESC, id DESC
                ) AS row_num
            FROM job_post
        )
        DELETE FROM job_post
        WHERE id IN (
            SELECT id
            FROM ranked_jobs
            WHERE row_num > 1
        )
    """))
    conn.execute(text("""
        ALTER TABLE job_post
        ADD CONSTRAINT uq_job_post_fingerprint
        UNIQUE (tenant_id, fingerprint_version, canonical_fingerprint)
    """))
