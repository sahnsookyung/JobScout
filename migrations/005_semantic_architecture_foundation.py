"""Add canonical job summaries and preference semantics metadata."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

DDL_STATEMENTS = [
    "ALTER TABLE job_post ADD COLUMN IF NOT EXISTS canonical_job_summary TEXT",
    "ALTER TABLE job_post ADD COLUMN IF NOT EXISTS canonical_job_summary_version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE job_post ADD COLUMN IF NOT EXISTS canonical_job_summary_hash TEXT",
    "ALTER TABLE candidate_preferences ADD COLUMN IF NOT EXISTS soft_preference_summary TEXT",
    "ALTER TABLE candidate_preferences ADD COLUMN IF NOT EXISTS preference_mode TEXT NOT NULL DEFAULT 'semantic_rerank'",
    "ALTER TABLE candidate_preferences ADD COLUMN IF NOT EXISTS preference_profile JSONB",
]


def migrate(conn: Connection) -> None:
    for statement in DDL_STATEMENTS:
        conn.execute(text(statement))


def rollback(conn: Connection) -> None:
    conn.execute(text("ALTER TABLE candidate_preferences DROP COLUMN IF EXISTS preference_profile"))
    conn.execute(text("ALTER TABLE candidate_preferences DROP COLUMN IF EXISTS preference_mode"))
    conn.execute(text("ALTER TABLE candidate_preferences DROP COLUMN IF EXISTS soft_preference_summary"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS canonical_job_summary_hash"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS canonical_job_summary_version"))
    conn.execute(text("ALTER TABLE job_post DROP COLUMN IF EXISTS canonical_job_summary"))
