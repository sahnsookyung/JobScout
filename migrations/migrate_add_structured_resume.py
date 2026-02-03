#!/usr/bin/env python3
"""
Migration script to add structured_resume table for AI-extracted resume data.

This table stores comprehensive structured resume extraction with:
- Date-based experience calculations
- Validation of claimed vs calculated years
- Extraction confidence and warnings

Run this if you have an existing database and need to add the new table.
"""
import logging
import sys
from sqlalchemy import text, inspect
from database.database import engine
from database.models import Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def table_exists(table_name):
    """Check if a table exists."""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()


def index_exists(table_name, index_name):
    """Check if an index exists on a table."""
    inspector = inspect(engine)
    indexes = inspector.get_indexes(table_name)
    return any(idx['name'] == index_name for idx in indexes)


def migrate():
    """Add structured_resume table."""
    logger.info("Starting migration: Add structured_resume table")
    
    try:
        with engine.connect() as connection:
            if not table_exists('structured_resume'):
                logger.info("Creating structured_resume table...")
                connection.execute(text("""
                    CREATE TABLE structured_resume (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        resume_fingerprint TEXT NOT NULL UNIQUE,
                        extracted_data JSONB NOT NULL,
                        calculated_total_years NUMERIC(4, 1),
                        claimed_total_years NUMERIC(4, 1),
                        experience_validated BOOLEAN DEFAULT FALSE,
                        validation_message TEXT,
                        extraction_confidence NUMERIC(3, 2),
                        extraction_warnings JSONB DEFAULT '[]',
                        extracted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT timezone('UTC', now()),
                        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT timezone('UTC', now())
                    )
                """))
                logger.info("Table created successfully")
                
                # Create indexes
                if not index_exists('structured_resume', 'idx_structured_resume_fingerprint'):
                    logger.info("Creating fingerprint index...")
                    connection.execute(text("""
                        CREATE INDEX idx_structured_resume_fingerprint ON structured_resume (resume_fingerprint)
                    """))
                
                if not index_exists('structured_resume', 'idx_structured_resume_years'):
                    logger.info("Creating years index...")
                    connection.execute(text("""
                        CREATE INDEX idx_structured_resume_years ON structured_resume (calculated_total_years)
                    """))
                
                logger.info("Indexes created successfully")
            else:
                logger.info("structured_resume table already exists, skipping")
            
            connection.commit()
            
        logger.info("Migration completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False


if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
