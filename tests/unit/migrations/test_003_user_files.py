"""Database schema tests for users and user_files in the baseline migration."""

import pytest

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


@pytest.fixture
def db_engine(test_database):
    engine = create_engine(test_database)
    yield engine


@pytest.fixture
def db_connection(db_engine):
    conn = db_engine.connect()
    yield conn
    conn.close()


@pytest.mark.db
class TestUserFilesMigration:
    def test_migration_creates_users_table(self, db_connection):
        result = db_connection.execute(
            text(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'users'
                ORDER BY ordinal_position
                """
            )
        )
        columns = {row[0]: {"type": row[1], "nullable": row[2]} for row in result.fetchall()}

        assert set(columns) >= {
            "id",
            "email",
            "display_name",
            "email_verified_at",
            "is_active",
            "created_at",
        }
        assert columns["id"]["nullable"] == "NO"
        assert columns["email"]["nullable"] == "NO"
        assert columns["is_active"]["nullable"] == "NO"
        assert columns["created_at"]["nullable"] == "NO"

    def test_users_email_unique_constraint(self, db_connection):
        trans = db_connection.begin()
        try:
            db_connection.execute(
                text(
                    """
                    INSERT INTO users (id, email, is_active)
                    VALUES (
                        '11111111-1111-1111-1111-111111111111'::UUID,
                        'test_unique@example.com',
                        true
                    )
                    """
                )
            )

            with pytest.raises(IntegrityError):
                db_connection.execute(
                    text(
                        """
                        INSERT INTO users (id, email, is_active)
                        VALUES (
                            '22222222-2222-2222-2222-222222222222'::UUID,
                            'test_unique@example.com',
                            true
                        )
                        """
                    )
                )
        finally:
            trans.rollback()

    def test_migration_creates_user_files_table(self, db_connection):
        result = db_connection.execute(
            text(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'user_files'
                ORDER BY ordinal_position
                """
            )
        )
        columns = {row[0]: {"type": row[1], "nullable": row[2]} for row in result.fetchall()}

        assert set(columns) >= {
            "id",
            "owner_id",
            "original_filename",
            "mime_type",
            "size_bytes",
            "storage_key",
            "upload_status",
            "file_type",
            "created_at",
        }
        assert columns["owner_id"]["nullable"] == "NO"
        assert columns["storage_key"]["nullable"] == "NO"
        assert columns["file_type"]["nullable"] == "NO"

    def test_user_files_owner_id_foreign_key_constraint(self, db_connection):
        trans = db_connection.begin()
        try:
            with pytest.raises(IntegrityError):
                db_connection.execute(
                    text(
                        """
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
                        """
                    )
                )
        finally:
            trans.rollback()

    def test_user_files_storage_key_unique_constraint(self, db_connection):
        trans = db_connection.begin()
        try:
            db_connection.execute(
                text(
                    """
                    INSERT INTO users (id, email, is_active)
                    VALUES (
                        '33333333-3333-3333-3333-333333333333'::UUID,
                        'test@example.com',
                        true
                    )
                    ON CONFLICT (id) DO NOTHING
                    """
                )
            )
            db_connection.execute(
                text(
                    """
                    INSERT INTO user_files (
                        id, owner_id, original_filename, mime_type, size_bytes,
                        storage_key, upload_status, file_type
                    ) VALUES (
                        'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'::UUID,
                        '33333333-3333-3333-3333-333333333333'::UUID,
                        'resume1.pdf',
                        'application/pdf',
                        1024,
                        'resume/11111111-1111-1111-1111-111111111111',
                        'ready',
                        'resume'
                    )
                    """
                )
            )

            with pytest.raises(IntegrityError):
                db_connection.execute(
                    text(
                        """
                        INSERT INTO user_files (
                            id, owner_id, original_filename, mime_type, size_bytes,
                            storage_key, upload_status, file_type
                        ) VALUES (
                            'cccccccc-cccc-cccc-cccc-cccccccccccc'::UUID,
                            '33333333-3333-3333-3333-333333333333'::UUID,
                            'resume2.pdf',
                            'application/pdf',
                            2048,
                            'resume/11111111-1111-1111-1111-111111111111',
                            'ready',
                            'resume'
                        )
                        """
                    )
                )
        finally:
            trans.rollback()

    def test_users_indexes_exist(self, db_connection):
        result = db_connection.execute(
            text(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'users'
                """
            )
        )
        indexes = {row[0] for row in result.fetchall()}

        assert "idx_users_email" in indexes
        assert any("users_pkey" in idx for idx in indexes)

    def test_user_files_indexes_exist(self, db_connection):
        result = db_connection.execute(
            text(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'user_files'
                """
            )
        )
        indexes = {row[0] for row in result.fetchall()}

        assert "idx_user_files_owner_type" in indexes
        assert "idx_user_files_storage_key" in indexes
        assert "idx_user_files_created_at" in indexes
        assert any("user_files_pkey" in idx for idx in indexes)

    def test_upload_status_enum_exists(self, db_connection):
        result = db_connection.execute(
            text(
                """
                SELECT enumlabel
                FROM pg_enum
                WHERE enumtypid = 'upload_status'::regtype
                ORDER BY enumsortorder
                """
            )
        )
        values = [row[0] for row in result.fetchall()]
        assert values == ["pending", "scanned", "rejected", "ready"]

    def test_file_type_enum_exists(self, db_connection):
        result = db_connection.execute(
            text(
                """
                SELECT enumlabel
                FROM pg_enum
                WHERE enumtypid = 'file_type'::regtype
                ORDER BY enumsortorder
                """
            )
        )
        values = [row[0] for row in result.fetchall()]
        assert values == ["resume"]

    def test_insert_valid_user_file_succeeds(self, db_connection):
        trans = db_connection.begin()
        try:
            db_connection.execute(
                text(
                    """
                    INSERT INTO users (id, email, is_active)
                    VALUES (
                        '44444444-4444-4444-4444-444444444444'::UUID,
                        'test_valid@example.com',
                        true
                    )
                    """
                )
            )

            result = db_connection.execute(
                text(
                    """
                    INSERT INTO user_files (
                        id, owner_id, original_filename, mime_type, size_bytes,
                        storage_key, upload_status, file_type
                    ) VALUES (
                        'dddddddd-dddd-dddd-dddd-dddddddddddd'::UUID,
                        '44444444-4444-4444-4444-444444444444'::UUID,
                        'My Resume.pdf',
                        'application/pdf',
                        1024576,
                        'resume/550e8400-e29b-41d4-a716-446655440000',
                        'ready',
                        'resume'
                    )
                    RETURNING id
                    """
                )
            )

            inserted_id = result.fetchone()[0]
            assert inserted_id is not None
        finally:
            trans.rollback()

    def test_default_upload_status(self, db_connection):
        trans = db_connection.begin()
        try:
            db_connection.execute(
                text(
                    """
                    INSERT INTO users (id, email, is_active)
                    VALUES (
                        '55555555-5555-5555-5555-555555555555'::UUID,
                        'test_default@example.com',
                        true
                    )
                    """
                )
            )

            result = db_connection.execute(
                text(
                    """
                    INSERT INTO user_files (
                        id, owner_id, original_filename, mime_type, size_bytes,
                        storage_key, file_type
                    ) VALUES (
                        'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee'::UUID,
                        '55555555-5555-5555-5555-555555555555'::UUID,
                        'default_test.pdf',
                        'application/pdf',
                        1024,
                        'resume/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
                        'resume'
                    )
                    RETURNING upload_status
                    """
                )
            )

            assert result.fetchone()[0] == "pending"
        finally:
            trans.rollback()
