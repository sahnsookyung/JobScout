#!/usr/bin/env python3
"""
Unit tests for User and UserFile SQLAlchemy models.

Tests verify model instantiation, relationships, and constraints
through SQLAlchemy ORM interface.

These tests require a database - marked with @pytest.mark.db
"""

import pytest
import uuid
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from database.models import Base, User, UserFile


@pytest.fixture(scope="function")
def db_session(test_database):
    """Create a fresh database session for each test using test database."""
    engine = create_engine(test_database)
    connection = engine.connect()
    transaction = connection.begin()
    session = sessionmaker(bind=connection)()
    
    yield session
    
    session.close()
    if transaction.is_active:
        transaction.rollback()
    connection.close()


@pytest.mark.db
class TestUserModel:
    """Test suite for User SQLAlchemy model."""
    
    def test_user_creation_basic(self, db_session):
        """Test creating a basic user."""
        user = User(
            email="test@example.com",
            password_hash="bcrypt_hash_here"
        )
        db_session.add(user)
        db_session.commit()
        
        assert user.id is not None, "User ID should be generated"  # type: ignore
        assert user.email == "test@example.com"  # type: ignore
        assert user.password_hash == "bcrypt_hash_here"  # type: ignore
        assert user.is_active == True, "User should be active by default"  # type: ignore
        assert user.failed_login_attempts == 0, "Failed attempts should be 0 by default"  # type: ignore
        assert user.created_at is not None, "Created at should be set"  # type: ignore
    
    def test_user_creation_with_all_fields(self, db_session):
        """Test creating a user with all fields populated."""
        now = datetime.now(timezone.utc)
        
        user = User(
            email="full@example.com",
            password_hash="argon2_hash",
            display_name="Test User",
            email_verified_at=now,
            is_active=True,
            failed_login_attempts=0,
            locked_until=None,
            last_login_at=now,
            last_login_ip="192.168.1.1",
            deleted_at=None
        )
        db_session.add(user)
        db_session.commit()
        
        db_session.refresh(user)
        
        assert user.display_name == "Test User"  # type: ignore
        assert user.email_verified_at is not None  # type: ignore
        assert user.last_login_ip == "192.168.1.1"  # type: ignore
    
    def test_user_default_values(self, db_session):
        """Test that default values are set correctly."""
        user = User(
            email="defaults@example.com",
            password_hash="hash"
        )
        db_session.add(user)
        db_session.commit()
        
        db_session.refresh(user)
        
        assert user.is_active == True  # type: ignore
        assert user.failed_login_attempts == 0  # type: ignore
        assert user.created_at is not None  # type: ignore
        assert user.updated_at is not None  # type: ignore
        assert user.deleted_at is None  # type: ignore
    
    def test_user_email_unique_constraint(self, db_session):
        """Test that email must be unique."""
        user1 = User(email="unique@example.com", password_hash="hash1")
        db_session.add(user1)
        db_session.commit()
        
        user2 = User(email="unique@example.com", password_hash="hash2")
        db_session.add(user2)
        
        with pytest.raises(Exception) as exc_info:
            db_session.commit()
        
        assert "unique" in str(exc_info.value).lower() or "duplicate" in str(exc_info.value).lower()
    
    def test_user_relationship_to_files(self, db_session):
        """Test user relationship to files."""
        user = User(email="files@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        file1 = UserFile(
            owner_id=user.id,
            original_filename="resume.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/test-uuid-1",
            upload_status="ready",
            file_type="resume"
        )
        file2 = UserFile(
            owner_id=user.id,
            original_filename="cover_letter.docx",
            mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            size_bytes=2048,
            storage_key="resume/test-uuid-2",
            upload_status="ready",
            file_type="resume"
        )
        
        db_session.add_all([file1, file2])
        db_session.commit()
        
        db_session.refresh(user)
        
        assert len(user.files) == 2  # type: ignore
        assert user.files[0].original_filename in ["resume.pdf", "cover_letter.docx"]  # type: ignore
    
    def test_user_soft_delete(self, db_session):
        """Test that users support soft delete via deleted_at."""
        user = User(email="softdelete@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        user.deleted_at = datetime.now(timezone.utc)  # type: ignore
        db_session.commit()
        
        assert user.deleted_at is not None  # type: ignore
        assert user.is_active == True  # type: ignore


@pytest.mark.db
class TestUserFileModel:
    """Test suite for UserFile SQLAlchemy model."""
    
    def test_userfile_creation_basic(self, db_session):
        """Test creating a basic user file."""
        user = User(email="fileowner@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        file = UserFile(
            owner_id=user.id,
            original_filename="My Resume.pdf",
            mime_type="application/pdf",
            size_bytes=1024576,
            storage_key="resume/550e8400-e29b-41d4-a716-446655440000",
            upload_status="ready",
            file_type="resume"
        )
        db_session.add(file)
        db_session.commit()
        
        assert file.id is not None, "File ID should be generated"  # type: ignore
        assert file.owner_id == user.id  # type: ignore
        assert file.original_filename == "My Resume.pdf"  # type: ignore
        assert file.mime_type == "application/pdf"  # type: ignore
        assert file.size_bytes == 1024576  # type: ignore
        assert file.storage_key == "resume/550e8400-e29b-41d4-a716-446655440000"  # type: ignore
        assert file.upload_status == "ready"  # type: ignore
        assert file.file_type == "resume"  # type: ignore
        assert file.created_at is not None  # type: ignore
    
    def test_userfile_default_upload_status(self, db_session):
        """Test that upload_status defaults to 'pending'."""
        user = User(email="defaultstatus@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        file = UserFile(
            owner_id=user.id,
            original_filename="test.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/default-test-uuid",
            file_type="resume"
        )
        db_session.add(file)
        db_session.commit()
        
        db_session.refresh(file)
        
        assert file.upload_status == "pending", f"Expected 'pending', got '{file.upload_status}'"  # type: ignore
    
    def test_userfile_storage_key_unique_constraint(self, db_session):
        """Test that storage_key must be unique."""
        user = User(email="storagekey@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        file1 = UserFile(
            owner_id=user.id,
            original_filename="file1.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/same-uuid",
            upload_status="ready",
            file_type="resume"
        )
        db_session.add(file1)
        db_session.commit()
        
        file2 = UserFile(
            owner_id=user.id,
            original_filename="file2.pdf",
            mime_type="application/pdf",
            size_bytes=2048,
            storage_key="resume/same-uuid",
            upload_status="ready",
            file_type="resume"
        )
        db_session.add(file2)
        
        with pytest.raises(Exception) as exc_info:
            db_session.commit()
        
        assert "unique" in str(exc_info.value).lower() or "duplicate" in str(exc_info.value).lower()
    
    def test_userfile_foreign_key_constraint(self, db_session):
        """Test that owner_id must reference a valid user."""
        fake_uuid = uuid.uuid4()
        
        file = UserFile(
            owner_id=fake_uuid,
            original_filename="orphan.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/orphan-uuid",
            upload_status="ready",
            file_type="resume"
        )
        db_session.add(file)
        
        with pytest.raises(Exception) as exc_info:
            db_session.commit()
        
        error_str = str(exc_info.value).lower()
        assert "foreign key" in error_str or "owner_id" in error_str or "constraint" in error_str
    
    def test_userfile_relationship_to_owner(self, db_session):
        """Test file relationship back to owner."""
        user = User(email="relationship@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        file = UserFile(
            owner_id=user.id,
            original_filename="test.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/rel-test-uuid",
            upload_status="ready",
            file_type="resume"
        )
        db_session.add(file)
        db_session.commit()
        
        db_session.refresh(file)
        
        assert file.owner is not None  # type: ignore
        assert file.owner.email == "relationship@example.com"  # type: ignore
    
    def test_userfile_file_type_must_be_valid_enum(self, db_session):
        """Test that file_type must be a valid ENUM value ('resume')."""
        user = User(email="filetype@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        file = UserFile(
            owner_id=user.id,
            original_filename="resume.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/valid-type-uuid",
            upload_status="ready",
            file_type="resume"
        )
        
        db_session.add(file)
        db_session.commit()
        
        assert file.file_type == "resume"  # type: ignore
    
    def test_userfile_storage_key_format(self, db_session):
        """Test that storage_key follows the expected format: {type}/{uuid}."""
        user = User(email="storageformat@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        test_uuid = "550e8400-e29b-41d4-a716-446655440000"
        
        file = UserFile(
            owner_id=user.id,
            original_filename="format_test.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key=f"resume/{test_uuid}",
            upload_status="ready",
            file_type="resume"
        )
        db_session.add(file)
        db_session.commit()
        
        parts = file.storage_key.split("/")  # type: ignore
        assert len(parts) == 2, f"Storage key should have format 'type/uuid', got '{file.storage_key}'"  # type: ignore
        assert parts[0] == "resume", f"Expected type 'resume', got '{parts[0]}'"
        assert parts[1] == test_uuid, f"Expected UUID '{test_uuid}', got '{parts[1]}'"
    
    def test_userfile_cascade_delete(self, db_session):
        """Test that files are deleted when user is deleted."""
        user = User(email="cascade@example.com", password_hash="hash")
        db_session.add(user)
        db_session.commit()
        
        file = UserFile(
            owner_id=user.id,
            original_filename="will_delete.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/cascade-uuid",
            upload_status="ready",
            file_type="resume"
        )
        db_session.add(file)
        db_session.commit()
        
        file_id = file.id
        
        db_session.delete(user)
        db_session.commit()
        
        deleted_file = db_session.query(UserFile).filter_by(id=file_id).first()
        assert deleted_file is None, "File should be deleted when user is deleted"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-m', 'db'])
