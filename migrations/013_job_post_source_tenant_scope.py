"""Scope job post source uniqueness to each job post."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE job_post_source
        DROP CONSTRAINT IF EXISTS uq_job_post_source_site_url
    """))
    conn.execute(text("""
        ALTER TABLE job_post_source
        DROP CONSTRAINT IF EXISTS uq_job_post_source_job_site_url
    """))
    conn.execute(text("""
        ALTER TABLE job_post_source
        ADD CONSTRAINT uq_job_post_source_job_site_url
        UNIQUE (job_post_id, site, job_url)
    """))


def rollback(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE job_post_source
        DROP CONSTRAINT IF EXISTS uq_job_post_source_job_site_url
    """))
    conn.execute(text("""
        ALTER TABLE job_post_source
        ADD CONSTRAINT uq_job_post_source_site_url
        UNIQUE (site, job_url)
    """))
