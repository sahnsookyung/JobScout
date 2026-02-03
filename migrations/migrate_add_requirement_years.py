#!/usr/bin/env python3
"""
Migration script to add all schema changes for experience-based matching.

This handles:
- job_requirement_unit.min_years (extracted from requirements)
- job_requirement_unit.years_context (what the years refer to)

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
    """Add min_years and years_context columns to job_requirement_unit table."""
    logger.info("Starting migration: Add experience columns to job_requirement_unit")
    
    try:
        with engine.connect() as connection:
            # Add min_years column
            if not column_exists('job_requirement_unit', 'min_years'):
                logger.info("Adding 'min_years' column to job_requirement_unit table...")
                connection.execute(text("""
                    ALTER TABLE job_requirement_unit 
                    ADD COLUMN min_years INTEGER
                """))
                logger.info("Column added successfully")
            else:
                logger.info("'min_years' column already exists, skipping")
            
            # Add years_context column
            if not column_exists('job_requirement_unit', 'years_context'):
                logger.info("Adding 'years_context' column to job_requirement_unit table...")
                connection.execute(text("""
                    ALTER TABLE job_requirement_unit 
                    ADD COLUMN years_context TEXT
                """))
                logger.info("Column added successfully")
            else:
                logger.info("'years_context' column already exists, skipping")
            
            # Create index on min_years for filtering
            if not index_exists('job_requirement_unit', 'idx_jru_min_years'):
                logger.info("Creating index on min_years column...")
                connection.execute(text("""
                    CREATE INDEX idx_jru_min_years ON job_requirement_unit (min_years)
                """))
                logger.info("Index created successfully")
            else:
                logger.info("Index already exists, skipping")
            
            connection.commit()
            
        logger.info("Migration completed successfully!")
        logger.info("Note: min_years will be populated as requirements are re-extracted.")
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False


if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
