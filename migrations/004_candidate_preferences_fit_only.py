"""Replace facet-based wants storage with candidate preferences and fit-only matches.

Historical note: `candidate_preferences` is already created in
`001_initial_schema.py`. The repeated `CREATE TABLE IF NOT EXISTS` here is
redundant but intentionally left in place because migrations are append-only.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS candidate_preferences (
        owner_id UUID PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
        remote_mode TEXT NOT NULL DEFAULT 'any',
        target_locations JSONB NOT NULL DEFAULT '[]',
        visa_sponsorship_required BOOLEAN NOT NULL DEFAULT FALSE,
        salary_min INTEGER,
        employment_types JSONB NOT NULL DEFAULT '[]',
        soft_preferences TEXT NOT NULL DEFAULT '',
        revision INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    "DROP INDEX IF EXISTS idx_job_match_want",
    "ALTER TABLE job_match DROP COLUMN IF EXISTS want_score",
    "ALTER TABLE job_match DROP COLUMN IF EXISTS want_components",
    "ALTER TABLE job_match DROP COLUMN IF EXISTS fit_weight",
    "ALTER TABLE job_match DROP COLUMN IF EXISTS want_weight",
    "DROP TABLE IF EXISTS user_wants CASCADE",
]


def migrate(conn: Connection) -> None:
    """Create candidate_preferences and remove fit/want blended match fields."""
    for statement in DDL_STATEMENTS:
        conn.execute(text(statement))


def rollback(conn: Connection) -> None:
    """Rollback local schema changes for development only."""
    conn.execute(text("DROP TABLE IF EXISTS candidate_preferences CASCADE"))
    conn.execute(text("CREATE TABLE IF NOT EXISTS user_wants (id UUID PRIMARY KEY, owner_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE, wants_text TEXT NOT NULL, embedding VECTOR(1024) NOT NULL, facet_key TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()))"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_user_wants_owner ON user_wants (owner_id)"))
    conn.execute(text("ALTER TABLE job_match ADD COLUMN IF NOT EXISTS want_score NUMERIC(5, 2)"))
    conn.execute(text("ALTER TABLE job_match ADD COLUMN IF NOT EXISTS want_components JSONB"))
    conn.execute(text("ALTER TABLE job_match ADD COLUMN IF NOT EXISTS fit_weight NUMERIC(3, 2)"))
    conn.execute(text("ALTER TABLE job_match ADD COLUMN IF NOT EXISTS want_weight NUMERIC(3, 2)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_job_match_want ON job_match (want_score)"))
