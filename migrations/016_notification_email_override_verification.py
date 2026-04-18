"""Email override verification fields for notification settings."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


def migrate(conn: Connection) -> None:
    conn.execute(text("""
        ALTER TABLE user_notification_channel
            ADD COLUMN IF NOT EXISTS override_address TEXT,
            ADD COLUMN IF NOT EXISTS override_verified_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS verification_token_hash TEXT,
            ADD COLUMN IF NOT EXISTS verification_token_expires_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS verification_sent_at TIMESTAMPTZ
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_unc_verif_hash_pending
        ON user_notification_channel(verification_token_hash)
        WHERE verification_token_hash IS NOT NULL
    """))


def rollback(conn: Connection) -> None:
    conn.execute(text("DROP INDEX IF EXISTS idx_unc_verif_hash_pending"))
    conn.execute(text("""
        ALTER TABLE user_notification_channel
            DROP COLUMN IF EXISTS verification_sent_at,
            DROP COLUMN IF EXISTS verification_token_expires_at,
            DROP COLUMN IF EXISTS verification_token_hash,
            DROP COLUMN IF EXISTS override_verified_at,
            DROP COLUMN IF EXISTS override_address
    """))
