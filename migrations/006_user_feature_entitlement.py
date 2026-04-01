"""Add per-user feature entitlements for semantic-fit mode gating."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS user_feature_entitlement (
        id UUID PRIMARY KEY,
        owner_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        feature_key TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        value_json JSONB,
        source TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_user_feature_entitlement_owner_feature
    ON user_feature_entitlement (owner_id, feature_key)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_user_feature_entitlement_feature
    ON user_feature_entitlement (feature_key)
    """,
]


def migrate(conn: Connection) -> None:
    for statement in DDL_STATEMENTS:
        conn.execute(text(statement))


def rollback(conn: Connection) -> None:
    conn.execute(text("DROP TABLE IF EXISTS user_feature_entitlement"))
