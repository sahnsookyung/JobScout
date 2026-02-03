#!/usr/bin/env python3
"""
Migration script to add resume_section_embeddings table.

This table stores embeddings for individual resume sections (experience, projects, skills, summary)
to enable more granular matching against job requirements.
"""
import logging
import sys
from sqlalchemy import text, inspect
from database.database import engine

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
    """Add resume_section_embeddings table."""
    logger.info("Starting migration: Add resume_section_embeddings table")
    
    try:
        with engine.connect() as connection:
            if not table_exists('resume_section_embeddings'):
                logger.info("Creating resume_section_embeddings table...")
                connection.execute(text("""
                    CREATE TABLE resume_section_embeddings (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        resume_fingerprint TEXT NOT NULL,
                        section_type TEXT NOT NULL,
                        section_index INTEGER NOT NULL,
                        source_text TEXT NOT NULL,
                        source_data JSONB NOT NULL,
                        embedding VECTOR(1024) NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT timezone('UTC', now())
                    )
                """))
                logger.info("Table created successfully")
                
                # Create indexes
                logger.info("Creating indexes...")
                connection.execute(text("""
                    CREATE INDEX idx_rse_resume_section ON resume_section_embeddings (resume_fingerprint, section_type, section_index)
                """))
                
                connection.execute(text("""
                    CREATE INDEX idx_rse_embedding_hnsw ON resume_section_embeddings
                    USING hnsw (embedding vector_cosine_ops)
                    WITH (m = 16, ef_construction = 64)
                """))
                
                logger.info("Indexes created successfully")
            else:
                logger.info("resume_section_embeddings table already exists, skipping")
            
            connection.commit()
            
        logger.info("Migration completed successfully!")
        logger.info("Note: Section embeddings will be populated as resumes are processed.")
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False


if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
