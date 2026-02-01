"""Add notified column to job_match table

Revision ID: add_notified_column
Revises: 
Create Date: 2026-02-01
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = 'add_notified_column'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Add notified column to job_match table with server_default for DB-level default
    op.add_column('job_match', sa.Column('notified', sa.Boolean(), nullable=True, server_default=sa.text('false')))
    
    # Create index on notified column
    op.create_index('idx_job_match_notified', 'job_match', ['notified'])
    
    # Update existing rows to set notified = False
    op.execute("UPDATE job_match SET notified = FALSE WHERE notified IS NULL")
    
    # Make column non-nullable after setting defaults
    op.alter_column('job_match', 'notified', nullable=False)
    
    # Drop the server_default since column is now non-nullable
    op.alter_column('job_match', 'notified', server_default=None)


def downgrade():
    # Drop index first
    op.drop_index('idx_job_match_notified', table_name='job_match')
    
    # Drop column
    op.drop_column('job_match', 'notified')
