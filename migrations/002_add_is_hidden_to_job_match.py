#!/usr/bin/env python3
"""
Migration: Add is_hidden column to job_match table

This migration adds an is_hidden boolean column to the job_match table
to track which matches have been hidden by the user from the frontend.

Date: 2026-02-08
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine, text
from database.database import DATABASE_URL


def migrate():
    """Add is_hidden column to job_match table."""
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # Check if column already exists
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'job_match' AND column_name = 'is_hidden'
        """))
        
        if result.fetchone():
            print("Column 'is_hidden' already exists in job_match table. Skipping migration.")
            return
        
        # Add the is_hidden column
        conn.execute(text("""
            ALTER TABLE job_match 
            ADD COLUMN is_hidden BOOLEAN DEFAULT FALSE
        """))
        
        # Create index for efficient querying
        conn.execute(text("""
            CREATE INDEX idx_job_match_hidden ON job_match (is_hidden)
        """))
        
        conn.commit()
        print("Successfully added 'is_hidden' column to job_match table")
        print("Successfully created index 'idx_job_match_hidden'")


def rollback():
    """Remove is_hidden column from job_match table."""
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # Drop index first
        conn.execute(text("""
            DROP INDEX IF EXISTS idx_job_match_hidden
        """))
        
        # Drop the column
        conn.execute(text("""
            ALTER TABLE job_match 
            DROP COLUMN IF EXISTS is_hidden
        """))
        
        conn.commit()
        print("Successfully removed 'is_hidden' column from job_match table")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migration for adding is_hidden column")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    
    args = parser.parse_args()
    
    if args.rollback:
        rollback()
    else:
        migrate()
