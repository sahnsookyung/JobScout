#!/usr/bin/env python3
"""
Migration script to fix req_type values in job_requirement_unit table.

Maps old AI extraction values to database values:
- must_have -> required
- nice_to_have -> preferred

Usage:
    uv run python migrations/fix_req_types.py
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def migrate_req_types():
    """Fix req_type values in the database."""
    
    # Get database URL from environment or use default
    db_url = os.environ.get('DATABASE_URL', 'postgresql://user:password@localhost:5432/jobscout')
    
    print(f"Connecting to database...")
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    
    with SessionLocal() as session:
        # Check current distribution
        print("\nCurrent req_type distribution:")
        result = session.execute(text("""
            SELECT req_type, COUNT(*) as count
            FROM job_requirement_unit
            GROUP BY req_type
            ORDER BY count DESC
        """))
        
        for row in result:
            print(f"  {row.req_type}: {row.count}")
        
        # Update must_have -> required
        print("\nUpdating 'must_have' to 'required'...")
        result = session.execute(text("""
            UPDATE job_requirement_unit
            SET req_type = 'required'
            WHERE req_type = 'must_have'
        """))
        must_have_updated = result.rowcount
        print(f"  Updated {must_have_updated} rows")
        
        # Update nice_to_have -> preferred
        print("\nUpdating 'nice_to_have' to 'preferred'...")
        result = session.execute(text("""
            UPDATE job_requirement_unit
            SET req_type = 'preferred'
            WHERE req_type = 'nice_to_have'
        """))
        nice_to_have_updated = result.rowcount
        print(f"  Updated {nice_to_have_updated} rows")
        
        session.commit()
        
        # Check new distribution
        print("\nNew req_type distribution:")
        result = session.execute(text("""
            SELECT req_type, COUNT(*) as count
            FROM job_requirement_unit
            GROUP BY req_type
            ORDER BY count DESC
        """))
        
        for row in result:
            print(f"  {row.req_type}: {row.count}")
        
        print(f"\n✓ Migration complete!")
        print(f"  - {must_have_updated} requirements changed from 'must_have' to 'required'")
        print(f"  - {nice_to_have_updated} requirements changed from 'nice_to_have' to 'preferred'")
        
        # Now recalculate all match scores
        if must_have_updated > 0 or nice_to_have_updated > 0:
            print("\n⚠ Note: You'll need to re-run the matching process to recalculate scores")
            print("  Run: uv run python main.py --mode matching")

if __name__ == "__main__":
    migrate_req_types()
