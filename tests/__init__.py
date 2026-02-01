#!/usr/bin/env python3
"""
Test suite configuration and utilities.

This module provides test configuration and database setup utilities.
All tests can be run with standard Python tools:

    # Run all tests (unit + DB if available)
    uv run python -m pytest tests/ -v
    
    # Run only unit tests (no DB required)
    uv run python -m pytest tests/ -v -m "not db"
    
    # Run only DB tests
    uv run python -m pytest tests/ -v -m "db"
    
    # Using unittest
    uv run python -m unittest discover tests -v

Database Setup:
    The test suite will automatically detect if a test database is available.
    To run DB tests, start the test database:
    
    docker-compose -f docker-compose.test.yml up -d
    
    Or use an existing PostgreSQL with pgvector and set:
    export TEST_DATABASE_URL="postgresql://user:pass@localhost:5433/jobscout_test"
"""

import os
import sys
from typing import Optional

# Database configuration
TEST_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://testuser:testpass@localhost:5433/jobscout_test"
)

# Check if we should force skip DB tests
SKIP_DB_TESTS = os.environ.get("SKIP_DB_TESTS", "false").lower() == "true"


def is_database_available() -> bool:
    """
    Check if the test database is accessible.
    
    Returns True if PostgreSQL with pgvector is available,
    False otherwise.
    """
    if SKIP_DB_TESTS:
        return False
    
    try:
        from sqlalchemy import create_engine, text
        
        engine = create_engine(TEST_DB_URL)
        with engine.connect() as conn:
            # Test basic connectivity
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
            
            # Check for pgvector extension
            result = conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')"
            ))
            row = result.fetchone()
            has_vector = row[0] if row else False
            
            if not has_vector:
                print("⚠ Database available but pgvector extension not installed")
                return False
            
            return True
            
    except Exception as e:
        return False


def get_test_db_url() -> str:
    """Get the test database URL."""
    return TEST_DB_URL


def setup_test_database():
    """
    Set up test database tables.
    Call this once before running DB tests.
    """
    try:
        from sqlalchemy import create_engine
        from database.models import Base
        
        engine = create_engine(TEST_DB_URL)
        Base.metadata.create_all(engine)
        print("✓ Test database tables created")
        return True
        
    except Exception as e:
        print(f"✗ Failed to setup test database: {e}")
        return False


def teardown_test_database():
    """
    Clean up test database tables.
    Call this after DB tests complete.
    """
    try:
        from sqlalchemy import create_engine
        from database.models import Base
        
        engine = create_engine(TEST_DB_URL)
        Base.metadata.drop_all(engine)
        print("✓ Test database tables dropped")
        return True
        
    except Exception as e:
        print(f"✗ Failed to teardown test database: {e}")
        return False


# Global flag to cache DB availability check
_db_available: Optional[bool] = None


def check_db_available() -> bool:
    """Cached check for database availability."""
    global _db_available
    if _db_available is None:
        _db_available = is_database_available()
    return _db_available
