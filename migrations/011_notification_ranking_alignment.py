"""Align notification settings naming and add persisted ranking snapshots."""

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
                WHERE table_name = 'user_notification_settings'
                  AND column_name = 'min_score_threshold'
            ) AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'user_notification_settings'
                  AND column_name = 'min_fit_for_alerts'
            ) THEN
                ALTER TABLE user_notification_settings
                RENAME COLUMN min_score_threshold TO min_fit_for_alerts;
            END IF;
        END $$;
    """))

    conn.execute(text(
        "ALTER TABLE job_match "
        "ADD COLUMN IF NOT EXISTS ranking_snapshot JSONB"
    ))


def rollback(conn: Connection) -> None:
    conn.execute(text(
        "ALTER TABLE job_match "
        "DROP COLUMN IF EXISTS ranking_snapshot"
    ))

    conn.execute(text("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'user_notification_settings'
                  AND column_name = 'min_fit_for_alerts'
            ) AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'user_notification_settings'
                  AND column_name = 'min_score_threshold'
            ) THEN
                ALTER TABLE user_notification_settings
                RENAME COLUMN min_fit_for_alerts TO min_score_threshold;
            END IF;
        END $$;
    """))
