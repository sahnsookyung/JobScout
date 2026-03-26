#!/usr/bin/env python3
"""
Migration: Add durable resume_processing_state table.

This migration adds a fingerprint-scoped processing state table used to track:
1. Extraction in progress / completed
2. Embedding in progress / completed
3. Ready and failed resume states

Date: 2026-03-26
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine, text
from database.database import DATABASE_URL


def migrate():
    """Create resume_processing_state table and indexes."""
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS resume_processing_state (
                resume_fingerprint TEXT PRIMARY KEY,
                processing_status TEXT NOT NULL,
                last_error TEXT,
                extraction_completed_at TIMESTAMPTZ,
                embedding_completed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
            )
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_resume_processing_state_status
            ON resume_processing_state (processing_status)
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_resume_processing_state_updated_at
            ON resume_processing_state (updated_at DESC)
        """))

        conn.execute(text("""
            INSERT INTO resume_processing_state (
                resume_fingerprint,
                processing_status,
                extraction_completed_at,
                embedding_completed_at,
                created_at,
                updated_at
            )
            SELECT
                sr.resume_fingerprint,
                'ready',
                sr.created_at,
                sr.updated_at,
                sr.created_at,
                sr.updated_at
            FROM structured_resume sr
            ON CONFLICT (resume_fingerprint) DO NOTHING
        """))

        conn.commit()
        print("Successfully created resume_processing_state table")


def rollback():
    """Drop resume_processing_state table."""
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS resume_processing_state"))
        conn.commit()
        print("Successfully dropped resume_processing_state table")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Migration for resume_processing_state")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")

    args = parser.parse_args()

    if args.rollback:
        rollback()
    else:
        migrate()
