"""Per-user notification settings and channel secrets."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS user_notification_settings (
        owner_id UUID PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
        notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE,
        min_score_threshold INTEGER NOT NULL DEFAULT 70,
        notify_on_new_match BOOLEAN NOT NULL DEFAULT TRUE,
        notify_on_batch_complete BOOLEAN NOT NULL DEFAULT TRUE,
        revision INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_notification_channel (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES user_notification_settings (owner_id) ON DELETE CASCADE,
        channel_type TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        configured BOOLEAN NOT NULL DEFAULT FALSE,
        masked_recipient TEXT,
        secret_ciphertext TEXT,
        secret_key_version TEXT,
        config_json JSONB NOT NULL DEFAULT '{}',
        last_test_status TEXT,
        last_tested_at TIMESTAMPTZ,
        last_test_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        CONSTRAINT uq_user_notification_channel_owner_type UNIQUE (owner_id, channel_type)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_user_notification_channel_owner ON user_notification_channel (owner_id)",
    "CREATE INDEX IF NOT EXISTS idx_user_notification_channel_last_tested ON user_notification_channel (last_tested_at)",
]


def migrate(conn: Connection) -> None:
    """Create per-user notification settings tables."""
    for statement in DDL_STATEMENTS:
        conn.execute(text(statement))


def rollback(conn: Connection) -> None:
    """Drop per-user notification settings tables."""
    conn.execute(text("DROP TABLE IF EXISTS user_notification_channel CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS user_notification_settings CASCADE"))
