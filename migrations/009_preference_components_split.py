"""Add preference_components column and backfill legacy preference metadata.

Preference metadata previously lived inside job_match.fit_components. This
migration adds a dedicated JSONB column so fit-only diagnostics and
preference-specific explanations can be stored separately.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text(
        "ALTER TABLE job_match "
        "ADD COLUMN IF NOT EXISTS preference_components JSONB"
    ))

    conn.execute(text("""
        UPDATE job_match
        SET preference_components = jsonb_strip_nulls(
            jsonb_build_object(
                'preference_confidence', fit_components->'preference_confidence',
                'preference_reason_codes', fit_components->'preference_reason_codes',
                'preference_explanation', fit_components->'preference_explanation',
                'preference_mode_requested', fit_components->'preference_mode_requested',
                'preference_mode_effective', fit_components->'preference_mode_effective',
                'preference_mode_used', fit_components->'preference_mode_used',
                'preference_fallback_reason', fit_components->'preference_fallback_reason'
            )
        )
        WHERE (preference_components IS NULL OR preference_components = '{}'::jsonb)
          AND fit_components IS NOT NULL
          AND (
            fit_components ? 'preference_confidence'
            OR fit_components ? 'preference_reason_codes'
            OR fit_components ? 'preference_explanation'
            OR fit_components ? 'preference_mode_requested'
            OR fit_components ? 'preference_mode_effective'
            OR fit_components ? 'preference_mode_used'
            OR fit_components ? 'preference_fallback_reason'
          )
    """))


def rollback(conn: Connection) -> None:
    conn.execute(text(
        "ALTER TABLE job_match "
        "DROP COLUMN IF EXISTS preference_components"
    ))
