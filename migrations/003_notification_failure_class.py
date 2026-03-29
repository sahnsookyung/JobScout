"""Add failure_class column to notification_tracker."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection


DDL_STATEMENTS = [
    "ALTER TABLE notification_tracker ADD COLUMN IF NOT EXISTS failure_class TEXT",
    """
    CREATE INDEX IF NOT EXISTS idx_notification_failure_class
        ON notification_tracker (failure_class)
        WHERE failure_class IS NOT NULL
    """,
]


def migrate(conn: Connection) -> None:
    """Add indexed failure_class column to notification_tracker."""
    for statement in DDL_STATEMENTS:
        conn.execute(text(statement))


def rollback(conn: Connection) -> None:
    """Remove failure_class column from notification_tracker."""
    conn.execute(text("DROP INDEX IF EXISTS idx_notification_failure_class"))
    conn.execute(text("ALTER TABLE notification_tracker DROP COLUMN IF EXISTS failure_class"))
