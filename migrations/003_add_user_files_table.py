#!/usr/bin/env python3
"""
Migration: Add users table and user_files table for file upload system

This migration creates:
1. The users table with authentication and audit fields
2. The user_files table for tracking uploaded files with UUID-based storage keys
3. ENUM types for upload_status and file_type
4. Proper indexes and constraints

Date: 2026-02-19
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine, text
from database.database import DATABASE_URL


def migrate():
    """Create users and user_files tables with enums and constraints."""
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # Create ENUM types
        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'upload_status') THEN
                    CREATE TYPE upload_status AS ENUM ('pending', 'scanned', 'rejected', 'ready');
                END IF;
            END
            $$;
        """))
        
        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'file_type') THEN
                    CREATE TYPE file_type AS ENUM ('resume');
                END IF;
            END
            $$;
        """))
        
        # Create users table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                display_name TEXT,
                email_verified_at TIMESTAMPTZ,
                is_active BOOLEAN NOT NULL DEFAULT true,
                failed_login_attempts INTEGER NOT NULL DEFAULT 0,
                locked_until TIMESTAMPTZ,
                last_login_at TIMESTAMPTZ,
                last_login_ip INET,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )
        """))
        
        # Create users indexes
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)
        """))
        
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_users_deleted_at ON users (deleted_at) 
            WHERE deleted_at IS NULL
        """))
        
        # Create user_files table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_files (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                owner_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                original_filename TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                size_bytes BIGINT NOT NULL,
                storage_key TEXT NOT NULL UNIQUE,
                upload_status upload_status DEFAULT 'pending',
                file_type file_type NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))
        
        # Create user_files indexes
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_user_files_owner_type ON user_files (owner_id, file_type)
        """))
        
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_user_files_storage_key ON user_files (storage_key)
        """))
        
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_user_files_created_at ON user_files (created_at)
        """))
        
        # Insert dev user for testing
        conn.execute(text("""
            INSERT INTO users (id, email, password_hash, display_name, is_active)
            VALUES (
                '00000000-0000-0000-0000-000000000000'::UUID,
                'dev@localhost',
                'password',
                'Developer',
                true
            )
            ON CONFLICT (id) DO NOTHING
        """))
        
        conn.commit()
        print("Successfully created users and user_files tables")
        print("Successfully created ENUM types: upload_status, file_type")
        print("Successfully created indexes")
        print("Successfully inserted dev user")


def rollback():
    """Drop users and user_files tables with enums."""
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # Drop user_files table first (has FK reference)
        conn.execute(text("DROP TABLE IF EXISTS user_files"))
        
        # Drop users table
        conn.execute(text("DROP TABLE IF EXISTS users"))
        
        # Drop ENUM types
        conn.execute(text("DROP TYPE IF EXISTS upload_status"))
        conn.execute(text("DROP TYPE IF EXISTS file_type"))
        
        conn.commit()
        print("Successfully dropped users and user_files tables")
        print("Successfully dropped ENUM types")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migration for users and user_files tables")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    
    args = parser.parse_args()
    
    if args.rollback:
        rollback()
    else:
        migrate()
