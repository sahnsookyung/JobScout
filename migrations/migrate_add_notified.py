#!/usr/bin/env python3
"""
Migration script to add 'notified' column to job_match table.
Run this if you have an existing database and need to add the new column.
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
    """Add notified column and index to job_match table."""
    logger.info("Starting migration: Add notified column to job_match")
    
    try:
        with engine.connect() as connection:
            # Add notified column if it doesn't exist
            if not column_exists('job_match', 'notified'):
                logger.info("Adding 'notified' column to job_match table...")
                connection.execute(text("""
                    ALTER TABLE job_match 
                    ADD COLUMN notified BOOLEAN DEFAULT FALSE
                """))
                logger.info("Column added successfully")
            else:
                logger.info("'notified' column already exists, skipping")
            
            # Create index if it doesn't exist
            if not index_exists('job_match', 'idx_job_match_notified'):
                logger.info("Creating index on notified column...")
                connection.execute(text("""
                    CREATE INDEX idx_job_match_notified ON job_match (notified)
                """))
                logger.info("Index created successfully")
            else:
                logger.info("Index already exists, skipping")
            
            # Update existing rows to set notified = FALSE
            logger.info("Updating existing rows...")
            result = connection.execute(text("""
                UPDATE job_match 
                SET notified = FALSE 
                WHERE notified IS NULL
            """))
            logger.info(f"Updated {result.rowcount} rows")
            
            connection.commit()
            
        logger.info("Migration completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False


if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
