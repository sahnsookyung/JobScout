"""
Migration: add robust extraction/embedding stage status fields to job_post.

This migration is idempotent and safe to run multiple times.
"""

import sys
from pathlib import Path

from sqlalchemy import create_engine, text

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def migrate() -> None:
    """Add stage status columns and backfill from compatibility flags."""
    from database.database import DATABASE_URL

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("""
            ALTER TABLE job_post
            ADD COLUMN IF NOT EXISTS extraction_status TEXT,
            ADD COLUMN IF NOT EXISTS extraction_attempts INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS extraction_last_error TEXT,
            ADD COLUMN IF NOT EXISTS extraction_last_attempt_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS extraction_next_retry_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS embedding_status TEXT,
            ADD COLUMN IF NOT EXISTS embedding_attempts INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS embedding_last_error TEXT,
            ADD COLUMN IF NOT EXISTS embedding_last_attempt_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS embedding_next_retry_at TIMESTAMPTZ
        """))

        # Backfill missing status values from existing compatibility booleans.
        conn.execute(text("""
            UPDATE job_post
            SET extraction_status = CASE
                WHEN is_extracted THEN 'succeeded'
                ELSE 'pending'
            END
            WHERE extraction_status IS NULL
        """))
        conn.execute(text("""
            UPDATE job_post
            SET embedding_status = CASE
                WHEN is_embedded THEN 'succeeded'
                ELSE 'pending'
            END
            WHERE embedding_status IS NULL
        """))

        conn.execute(text("""
            UPDATE job_post
            SET extraction_attempts = COALESCE(extraction_attempts, 0),
                embedding_attempts = COALESCE(embedding_attempts, 0)
        """))

        conn.execute(text("""
            ALTER TABLE job_post
            ALTER COLUMN extraction_status SET NOT NULL,
            ALTER COLUMN extraction_status SET DEFAULT 'pending',
            ALTER COLUMN extraction_attempts SET NOT NULL,
            ALTER COLUMN extraction_attempts SET DEFAULT 0,
            ALTER COLUMN embedding_status SET NOT NULL,
            ALTER COLUMN embedding_status SET DEFAULT 'pending',
            ALTER COLUMN embedding_attempts SET NOT NULL,
            ALTER COLUMN embedding_attempts SET DEFAULT 0
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_job_post_extraction_retry
            ON job_post (extraction_status, extraction_next_retry_at)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_job_post_embedding_retry
            ON job_post (embedding_status, embedding_next_retry_at)
        """))

        conn.commit()
        print("Successfully migrated job_post stage status fields")


def rollback() -> None:
    """Rollback migration by dropping stage status fields."""
    from database.database import DATABASE_URL

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("""
            DROP INDEX IF EXISTS idx_job_post_extraction_retry;
            DROP INDEX IF EXISTS idx_job_post_embedding_retry;
            ALTER TABLE job_post
            DROP COLUMN IF EXISTS extraction_status,
            DROP COLUMN IF EXISTS extraction_attempts,
            DROP COLUMN IF EXISTS extraction_last_error,
            DROP COLUMN IF EXISTS extraction_last_attempt_at,
            DROP COLUMN IF EXISTS extraction_next_retry_at,
            DROP COLUMN IF EXISTS embedding_status,
            DROP COLUMN IF EXISTS embedding_attempts,
            DROP COLUMN IF EXISTS embedding_last_error,
            DROP COLUMN IF EXISTS embedding_last_attempt_at,
            DROP COLUMN IF EXISTS embedding_next_retry_at
        """))
        conn.commit()
        print("Rolled back job_post stage status fields")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Add extraction/embedding stage status fields")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    args = parser.parse_args()

    if args.rollback:
        rollback()
    else:
        migrate()
