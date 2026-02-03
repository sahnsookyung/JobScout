#!/usr/bin/env python3
"""
Migration script to add content_hash columns for smarter match invalidation.

This adds:
- job_post.content_hash: Hash of job content to detect actual changes
- job_match.job_content_hash: Hash of job content at match time

Run this if you have an existing database and need to add the new columns.
"""
import logging
import sys
from sqlalchemy import text, inspect
from database.database import engine
from database.models import Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def column_exists(table_name, column_name):
    """Check if a column exists in a table."""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return any(col['name'] == column_name for col in columns)


def index_exists(table_name, index_name):
    """Check if an index exists on a table."""
    inspector = inspect(engine)
    indexes = inspector.get_indexes(table_name)
    return any(idx['name'] == index_name for idx in indexes)


def migrate():
    """Add content_hash columns and indexes."""
    logger.info("Starting migration: Add content_hash columns")

    try:
        # Report current state for debugging partial migrations
        jp_col = column_exists('job_post', 'content_hash')
        jp_idx = index_exists('job_post', 'idx_job_post_content_hash')
        jm_col = column_exists('job_match', 'job_content_hash')
        logger.info(f"Current state - job_post.content_hash: {jp_col}, index: {jp_idx}, job_match.job_content_hash: {jm_col}")

        with engine.connect() as connection:
            # Add content_hash to job_post table
            if not column_exists('job_post', 'content_hash'):
                logger.info("Adding 'content_hash' column to job_post table...")
                connection.execute(text("""
                    ALTER TABLE job_post 
                    ADD COLUMN content_hash TEXT
                """))
                logger.info("Column added successfully")
            else:
                logger.info("'content_hash' column already exists in job_post, skipping")
            
            # Create index on job_post.content_hash
            if not index_exists('job_post', 'idx_job_post_content_hash'):
                logger.info("Creating index on job_post.content_hash column...")
                connection.execute(text("""
                    CREATE INDEX idx_job_post_content_hash ON job_post (content_hash)
                """))
                logger.info("Index created successfully")
            else:
                logger.info("Index already exists, skipping")
            
            # Add job_content_hash to job_match table
            if not column_exists('job_match', 'job_content_hash'):
                logger.info("Adding 'job_content_hash' column to job_match table...")
                connection.execute(text("""
                    ALTER TABLE job_match 
                    ADD COLUMN job_content_hash TEXT
                """))
                logger.info("Column added successfully")
            else:
                logger.info("'job_content_hash' column already exists in job_match, skipping")
            
            connection.commit()
            
        logger.info("Migration completed successfully!")
        logger.info("Note: Content hashes will be populated as jobs are re-scraped and re-matched.")
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False


if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
