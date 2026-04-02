"""Add per-user feature capabilities for semantic-fit mode gating."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS user_feature_capability (
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
    CREATE UNIQUE INDEX IF NOT EXISTS idx_user_feature_capability_owner_feature
    ON user_feature_capability (owner_id, feature_key)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_user_feature_capability_feature
    ON user_feature_capability (feature_key)
    """,
]


def migrate(conn: Connection) -> None:
    for statement in DDL_STATEMENTS:
        conn.execute(text(statement))


def rollback(conn: Connection) -> None:
    conn.execute(text("DROP TABLE IF EXISTS user_feature_capability"))
