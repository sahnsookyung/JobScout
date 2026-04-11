"""Restore tenant-aware source uniqueness and repair earlier source-constraint drift."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE job_post_source
        ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenant (id) ON DELETE CASCADE
    """))
    conn.execute(text("""
        ALTER TABLE job_post_source
        DROP CONSTRAINT IF EXISTS uq_job_post_source_job_site_url
    """))
    conn.execute(text("""
        ALTER TABLE job_post_source
        DROP CONSTRAINT IF EXISTS uq_job_post_source_site_url
    """))
    conn.execute(text("""
        DROP INDEX IF EXISTS uq_job_post_source_tenant_site_url
    """))
    conn.execute(text("""
        DROP INDEX IF EXISTS uq_job_post_source_global_site_url
    """))
    conn.execute(text("""
        UPDATE job_post_source
        SET tenant_id = job_post.tenant_id
        FROM job_post
        WHERE job_post.id = job_post_source.job_post_id
          AND job_post_source.tenant_id IS DISTINCT FROM job_post.tenant_id
    """))
    conn.execute(text("""
        WITH ranked_sources AS (
            SELECT
                id,
                job_post_id,
                tenant_id,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(tenant_id::text, '__global__'), site, job_url
                    ORDER BY is_active DESC, last_seen_at DESC, first_seen_at DESC, id DESC
                ) AS row_num
            FROM job_post_source
        ),
        deleted_sources AS (
            DELETE FROM job_post_source
            WHERE id IN (
                SELECT id
                FROM ranked_sources
                WHERE row_num > 1
            )
            RETURNING job_post_id
        )
        UPDATE job_post
        SET status = 'inactive'
        WHERE id IN (SELECT DISTINCT job_post_id FROM deleted_sources)
          AND NOT EXISTS (
              SELECT 1
              FROM job_post_source
              WHERE job_post_source.job_post_id = job_post.id
                AND job_post_source.is_active
          )
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX uq_job_post_source_tenant_site_url
        ON job_post_source (tenant_id, site, job_url)
        WHERE tenant_id IS NOT NULL
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX uq_job_post_source_global_site_url
        ON job_post_source (site, job_url)
        WHERE tenant_id IS NULL
    """))


def rollback(conn: Connection) -> None:
    conn.execute(text("""
        DROP INDEX IF EXISTS uq_job_post_source_tenant_site_url
    """))
    conn.execute(text("""
        DROP INDEX IF EXISTS uq_job_post_source_global_site_url
    """))
    conn.execute(text("""
        WITH ranked_sources AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY site, job_url
                    ORDER BY is_active DESC, last_seen_at DESC, first_seen_at DESC, id DESC
                ) AS row_num
            FROM job_post_source
        ),
        deleted_sources AS (
            DELETE FROM job_post_source
            WHERE id IN (
                SELECT id
                FROM ranked_sources
                WHERE row_num > 1
            )
            RETURNING job_post_id
        )
        UPDATE job_post
        SET status = 'inactive'
        WHERE id IN (SELECT DISTINCT job_post_id FROM deleted_sources)
          AND NOT EXISTS (
              SELECT 1
              FROM job_post_source
              WHERE job_post_source.job_post_id = job_post.id
                AND job_post_source.is_active
          )
    """))
    conn.execute(text("""
        ALTER TABLE job_post_source
        DROP COLUMN IF EXISTS tenant_id
    """))
    conn.execute(text("""
        ALTER TABLE job_post_source
        ADD CONSTRAINT uq_job_post_source_site_url
        UNIQUE (site, job_url)
    """))
