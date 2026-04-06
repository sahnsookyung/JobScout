"""Add preference_score column; drop overall_score.

preference_score promotes the value that was previously buried in the fit_components
JSONB dict to a first-class indexable column, enabling the ranking engine to access
it without JSON traversal.

overall_score is removed because it encoded a now-deleted opaque weighted blend.
The ranking engine derives ordering directly from preference_score, fit_score, and
job_similarity at query time, so overall_score is no longer meaningful.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    # Add preference_score as nullable (existing rows get NULL, backfilled below)
    conn.execute(text(
        "ALTER TABLE job_match "
        "ADD COLUMN IF NOT EXISTS preference_score NUMERIC(5, 4)"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_job_match_pref "
        "ON job_match (preference_score)"
    ))

    # Backfill from existing fit_components JSONB for rows that already have it
    conn.execute(text("""
        UPDATE job_match
        SET preference_score = (fit_components->>'preference_score')::NUMERIC
        WHERE fit_components ? 'preference_score'
          AND (fit_components->>'preference_score') IS NOT NULL
          AND preference_score IS NULL
    """))

    # Drop overall_score — the ranking engine no longer uses it
    conn.execute(text("DROP INDEX IF EXISTS idx_job_match_score"))
    conn.execute(text(
        "ALTER TABLE job_match DROP COLUMN IF EXISTS overall_score"
    ))


def rollback(conn: Connection) -> None:
    conn.execute(text(
        "ALTER TABLE job_match "
        "ADD COLUMN IF NOT EXISTS overall_score NUMERIC(5, 2)"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_job_match_score "
        "ON job_match (overall_score)"
    ))
    conn.execute(text("DROP INDEX IF EXISTS idx_job_match_pref"))
    conn.execute(text(
        "ALTER TABLE job_match DROP COLUMN IF EXISTS preference_score"
    ))
