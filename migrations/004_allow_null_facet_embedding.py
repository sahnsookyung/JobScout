#!/usr/bin/env python3
"""
Migration: Allow NULL embeddings in job_facet_embedding table.

The facet extraction and embedding are two separate steps:
1. extract_facets_one() extracts facet text and saves with NULL embedding
2. embed_facets_one() generates embeddings for existing facet texts

This migration allows the embedding column to be NULL so that newly
extracted facets can be saved before their embeddings are generated.

Date: 2026-03-01
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine, text


def migrate():
    """Allow NULL for embedding column in job_facet_embedding table."""
    from database.database import DATABASE_URL
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        conn.execute(text("""
            ALTER TABLE job_facet_embedding 
            ALTER COLUMN embedding DROP NOT NULL
        """))
        conn.commit()
        print("Successfully allowed NULL embeddings in job_facet_embedding table")


def rollback():
    """Revert to NOT NULL constraint."""
    from database.database import DATABASE_URL
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            DELETE FROM job_facet_embedding 
            WHERE embedding IS NULL
        """))
        print(f"Deleted {result.rowcount} rows with NULL embeddings")
        
        conn.execute(text("""
            ALTER TABLE job_facet_embedding 
            ALTER COLUMN embedding SET NOT NULL
        """))
        conn.commit()
        print("Successfully restored NOT NULL constraint on embedding column")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migration for job_facet_embedding NULL embedding")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    
    args = parser.parse_args()
    
    if args.rollback:
        rollback()
    else:
        migrate()
