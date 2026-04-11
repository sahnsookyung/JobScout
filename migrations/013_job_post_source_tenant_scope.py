"""Restore global source uniqueness and repair mistaken tenant-scope changes."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE job_post_source
        DROP CONSTRAINT IF EXISTS uq_job_post_source_job_site_url
    """))
    conn.execute(text("""
        WITH ranked_sources AS (
            SELECT
                id,
                job_post_id,
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
        ADD CONSTRAINT uq_job_post_source_site_url
        UNIQUE (site, job_url)
    """))


def rollback(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE job_post_source
        DROP CONSTRAINT IF EXISTS uq_job_post_source_site_url
    """))
    conn.execute(text("""
        ALTER TABLE job_post_source
        ADD CONSTRAINT uq_job_post_source_job_site_url
        UNIQUE (job_post_id, site, job_url)
    """))
