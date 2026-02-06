"""Rename extracted_at to created_at in structured_resume table.

Revision ID: 001_rename_extracted_at
Revises:
Create Date: 2026-02-06
"""

from alembic import op
import sqlalchemy as sa

revision = '001_rename_extracted_at'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE structured_resume RENAME COLUMN extracted_at TO created_at")


def downgrade() -> None:
    op.execute("ALTER TABLE structured_resume RENAME COLUMN created_at TO extracted_at")
