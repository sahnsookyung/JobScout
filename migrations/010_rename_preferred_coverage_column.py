"""Rename preferred_coverage to preferred_requirement_coverage on job_match."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'job_match'
                  AND column_name = 'preferred_coverage'
            ) AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'job_match'
                  AND column_name = 'preferred_requirement_coverage'
            ) THEN
                ALTER TABLE job_match
                RENAME COLUMN preferred_coverage TO preferred_requirement_coverage;
            END IF;
        END $$;
    """))


def rollback(conn: Connection) -> None:
    conn.execute(text("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'job_match'
                  AND column_name = 'preferred_requirement_coverage'
            ) AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'job_match'
                  AND column_name = 'preferred_coverage'
            ) THEN
                ALTER TABLE job_match
                RENAME COLUMN preferred_requirement_coverage TO preferred_coverage;
            END IF;
        END $$;
    """))
