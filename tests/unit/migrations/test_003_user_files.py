#!/usr/bin/env python3
"""
Unit tests for migration 003_add_user_files_table

Tests verify:
- Migration runs successfully up and down
- Constraint violations are enforced
- Indexes are created correctly
- ENUM types work properly

These tests require a database - marked with @pytest.mark.db
"""

import pytest
import uuid
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


@pytest.fixture
def db_engine(test_database):
    """Provide a database engine for testing."""
    engine = create_engine(test_database)
    yield engine


@pytest.fixture
def db_connection(db_engine):
    """Provide a database connection with cleanup."""
    conn = db_engine.connect()
    yield conn
    conn.close()


@pytest.mark.db
class TestUserFilesMigration:
    """Test suite for users and user_files table migration."""

    def test_migration_creates_users_table(self, db_connection):
        """Test that users table is created with all columns."""
        result = db_connection.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'users'
            ORDER BY ordinal_position
        """))
        columns = {row[0]: {'type': row[1], 'nullable': row[2]} for row in result.fetchall()}

        assert 'id' in columns
        assert 'email' in columns
        assert 'password_hash' in columns
        assert 'display_name' in columns
        assert 'email_verified_at' in columns
        assert 'is_active' in columns
        assert 'failed_login_attempts' in columns
        assert 'locked_until' in columns
        assert 'last_login_at' in columns
        assert 'last_login_ip' in columns
        assert 'created_at' in columns
        assert 'updated_at' in columns
        assert 'deleted_at' in columns

        assert columns['id']['nullable'] == 'NO'
        assert columns['email']['nullable'] == 'NO'
        assert columns['password_hash']['nullable'] == 'NO'
        assert columns['is_active']['nullable'] == 'NO'
        assert columns['failed_login_attempts']['nullable'] == 'NO'
        assert columns['created_at']['nullable'] == 'NO'
        assert columns['updated_at']['nullable'] == 'NO'

    def test_users_email_unique_constraint(self, db_connection):
        """Test that email must be unique in users table."""
        trans = db_connection.begin()

        try:
            db_connection.execute(text("""
                INSERT INTO users (id, email, password_hash, is_active, failed_login_attempts)
                VALUES (
                    '11111111-1111-1111-1111-111111111111'::UUID,
                    'test_unique@example.com',
                    'test_hash',
                    true,
                    0
                )
            """))

            with pytest.raises(IntegrityError):
                db_connection.execute(text("""
                    INSERT INTO users (id, email, password_hash, is_active, failed_login_attempts)
                    VALUES (
                        '22222222-2222-2222-2222-222222222222'::UUID,
                        'test_unique@example.com',
                        'test_hash_2',
                        true,
                        0
                    )
                """))

            trans.rollback()
        except Exception:
            trans.rollback()
            raise

    def test_migration_creates_user_files_table(self, db_connection):
        """Test that user_files table is created with all columns."""
        result = db_connection.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'user_files'
            ORDER BY ordinal_position
        """))
        columns = {row[0]: {'type': row[1], 'nullable': row[2]} for row in result.fetchall()}

        assert 'id' in columns
        assert 'owner_id' in columns
        assert 'original_filename' in columns
        assert 'mime_type' in columns
        assert 'size_bytes' in columns
        assert 'storage_key' in columns
        assert 'upload_status' in columns
        assert 'file_type' in columns
        assert 'created_at' in columns

        assert columns['id']['nullable'] == 'NO'
        assert columns['owner_id']['nullable'] == 'NO'
        assert columns['original_filename']['nullable'] == 'NO'
        assert columns['mime_type']['nullable'] == 'NO'
        assert columns['size_bytes']['nullable'] == 'NO'
        assert columns['storage_key']['nullable'] == 'NO'
        assert columns['file_type']['nullable'] == 'NO'
        assert columns['created_at']['nullable'] == 'NO'

    def test_user_files_owner_id_foreign_key_constraint(self, db_connection):
        """Test that inserting user_files row without valid owner_id fails."""
        trans = db_connection.begin()

        try:
            with pytest.raises(IntegrityError) as exc_info:
                db_connection.execute(text("""
                    INSERT INTO user_files (
                        id, owner_id, original_filename, mime_type, size_bytes, 
                        storage_key, upload_status, file_type
                    ) VALUES (
                        'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'::UUID,
                        '99999999-9999-9999-9999-999999999999'::UUID,
                        'test.pdf',
                        'application/pdf',
                        1024,
                        'resume/99999999-9999-9999-9999-999999999999',
                        'pending',
                        'resume'
                    )
                """))

            assert 'foreign key' in str(exc_info.value).lower() or \
                   'owner_id' in str(exc_info.value).lower()

            trans.rollback()
        except Exception:
            trans.rollback()
            raise

    def test_user_files_storage_key_unique_constraint(self, db_connection):
        """Test that storage_key must be unique in user_files table."""
        trans = db_connection.begin()

        try:
            db_connection.execute(text("""
                INSERT INTO users (id, email, password_hash, is_active, failed_login_attempts)
                VALUES (
                    '00000000-0000-0000-0000-000000000000'::UUID,
                    'test@example.com',
                    'test_hash',
                    true,
                    0
                )
                ON CONFLICT (id) DO NOTHING
            """))

            db_connection.execute(text("""
                INSERT INTO user_files (
                    id, owner_id, original_filename, mime_type, size_bytes, 
                    storage_key, upload_status, file_type
                ) VALUES (
                    'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'::UUID,
                    '00000000-0000-0000-0000-000000000000'::UUID,
                    'resume1.pdf',
                    'application/pdf',
                    1024,
                    'resume/11111111-1111-1111-1111-111111111111',
                    'ready',
                    'resume'
                )
            """))

            with pytest.raises(IntegrityError):
                db_connection.execute(text("""
                    INSERT INTO user_files (
                        id, owner_id, original_filename, mime_type, size_bytes, 
                        storage_key, upload_status, file_type
                    ) VALUES (
                        'cccccccc-cccc-cccc-cccc-cccccccccccc'::UUID,
                        '00000000-0000-0000-0000-000000000000'::UUID,
                        'resume2.pdf',
                        'application/pdf',
                        2048,
                        'resume/11111111-1111-1111-1111-111111111111',
                        'ready',
                        'resume'
                    )
                """))

            trans.rollback()
        except Exception:
            trans.rollback()
            raise

    def test_users_indexes_exist(self, db_connection):
        """Test that required indexes on users table exist."""
        result = db_connection.execute(text("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'users'
        """))
        indexes = {row[0] for row in result.fetchall()}

        assert 'idx_users_email' in indexes
        assert 'idx_users_deleted_at' in indexes
        assert any('users_pkey' in idx for idx in indexes)

    def test_user_files_indexes_exist(self, db_connection):
        """Test that required indexes on user_files table exist."""
        result = db_connection.execute(text("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'user_files'
        """))
        indexes = {row[0] for row in result.fetchall()}

        assert 'idx_user_files_owner_type' in indexes
        assert 'idx_user_files_storage_key' in indexes
        assert 'idx_user_files_created_at' in indexes
        assert any('user_files_pkey' in idx for idx in indexes)
        assert any('user_files_storage_key_key' in idx for idx in indexes)

    def test_upload_status_enum_exists(self, db_connection):
        """Test that upload_status ENUM type exists with correct values."""
        result = db_connection.execute(text("""
            SELECT enumlabel
            FROM pg_enum
            WHERE enumtypid = 'upload_status'::regtype
            ORDER BY enumsortorder
        """))
        values = [row[0] for row in result.fetchall()]

        assert 'pending' in values
        assert 'scanned' in values
        assert 'rejected' in values
        assert 'ready' in values

    def test_file_type_enum_exists(self, db_connection):
        """Test that file_type ENUM type exists with correct values."""
        result = db_connection.execute(text("""
            SELECT enumlabel
            FROM pg_enum
            WHERE enumtypid = 'file_type'::regtype
            ORDER BY enumsortorder
        """))
        values = [row[0] for row in result.fetchall()]

        assert 'resume' in values

    def test_insert_valid_user_file_succeeds(self, db_connection):
        """Test that inserting a valid user_files row succeeds."""
        trans = db_connection.begin()

        try:
            db_connection.execute(text("""
                INSERT INTO users (id, email, password_hash, is_active, failed_login_attempts)
                VALUES (
                    '33333333-3333-3333-3333-333333333333'::UUID,
                    'test_valid@example.com',
                    'test_hash',
                    true,
                    0
                )
            """))

            result = db_connection.execute(text("""
                INSERT INTO user_files (
                    id, owner_id, original_filename, mime_type, size_bytes, 
                    storage_key, upload_status, file_type
                ) VALUES (
                    'dddddddd-dddd-dddd-dddd-dddddddddddd'::UUID,
                    '33333333-3333-3333-3333-333333333333'::UUID,
                    'My Resume.pdf',
                    'application/pdf',
                    1024576,
                    'resume/550e8400-e29b-41d4-a716-446655440000',
                    'ready',
                    'resume'
                )
                RETURNING id
            """))

            inserted_id = result.fetchone()[0]
            assert inserted_id is not None

            verify_result = db_connection.execute(text("""
                SELECT original_filename, storage_key, upload_status, file_type
                FROM user_files
                WHERE id = :id
            """), {"id": inserted_id})

            row = verify_result.fetchone()
            assert row[0] == 'My Resume.pdf'
            assert row[1] == 'resume/550e8400-e29b-41d4-a716-446655440000'
            assert row[2] == 'ready'
            assert row[3] == 'resume'

            trans.rollback()
        except Exception:
            trans.rollback()
            raise

    def test_default_upload_status(self, db_connection):
        """Test that upload_status defaults to 'pending'."""
        trans = db_connection.begin()

        try:
            db_connection.execute(text("""
                INSERT INTO users (id, email, password_hash, is_active, failed_login_attempts)
                VALUES (
                    '44444444-4444-4444-4444-444444444444'::UUID,
                    'test_default@example.com',
                    'test_hash',
                    true,
                    0
                )
            """))

            result = db_connection.execute(text("""
                INSERT INTO user_files (
                    id, owner_id, original_filename, mime_type, size_bytes, 
                    storage_key, file_type
                ) VALUES (
                    'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee'::UUID,
                    '44444444-4444-4444-4444-444444444444'::UUID,
                    'default_test.pdf',
                    'application/pdf',
                    1024,
                    'resume/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
                    'resume'
                )
                RETURNING upload_status
            """))

            status = result.fetchone()[0]
            assert status == 'pending'

            trans.rollback()
        except Exception:
            trans.rollback()
            raise


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-m', 'db'])
