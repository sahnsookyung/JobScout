"""
Unit tests for User, UserAuthIdentity, and UserFile SQLAlchemy models.
"""

import pytest
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database.models import User, UserAuthIdentity, UserFile

TEST_EMAIL = "test@example.com"


@pytest.fixture(scope="function")
def db_session(test_database):
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
    def test_user_creation_basic(self, db_session):
        user = User(email=TEST_EMAIL)
        db_session.add(user)
        db_session.commit()

        assert user.id is not None  # type: ignore
        assert user.email == TEST_EMAIL  # type: ignore
        assert user.is_active is True  # type: ignore
        assert user.created_at is not None  # type: ignore

    def test_user_email_unique_constraint(self, db_session):
        user1 = User(email="unique@example.com")
        db_session.add(user1)
        db_session.commit()

        user2 = User(email="unique@example.com")
        db_session.add(user2)

        with pytest.raises(Exception):
            db_session.commit()

    def test_user_relationships(self, db_session):
        user = User(email="files@example.com")
        db_session.add(user)
        db_session.commit()

        identity = UserAuthIdentity(
            user_id=user.id,
            provider="google",
            provider_subject="google-sub-1",
            email="files@example.com",
            email_normalized="files@example.com",
            email_verified=True,
        )
        file = UserFile(
            owner_id=user.id,
            original_filename="resume.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/test-uuid-1",
            upload_status="ready",
            file_type="resume",
        )
        db_session.add_all([identity, file])
        db_session.commit()
        db_session.refresh(user)

        assert len(user.files) == 1  # type: ignore
        assert len(user.auth_identities) == 1  # type: ignore


@pytest.mark.db
class TestUserAuthIdentityModel:
    def test_provider_subject_is_unique(self, db_session):
        user1 = User(email="identity1@example.com")
        user2 = User(email="identity2@example.com")
        db_session.add_all([user1, user2])
        db_session.commit()

        identity1 = UserAuthIdentity(
            user_id=user1.id,
            provider="google",
            provider_subject="same-subject",
        )
        identity2 = UserAuthIdentity(
            user_id=user2.id,
            provider="google",
            provider_subject="same-subject",
        )
        db_session.add(identity1)
        db_session.commit()
        db_session.add(identity2)

        with pytest.raises(Exception):
            db_session.commit()


@pytest.mark.db
class TestUserFileModel:
    def test_userfile_creation_basic(self, db_session):
        user = User(email="fileowner@example.com")
        db_session.add(user)
        db_session.commit()

        file = UserFile(
            owner_id=user.id,
            original_filename="My Resume.pdf",
            mime_type="application/pdf",
            size_bytes=1024576,
            storage_key="resume/550e8400-e29b-41d4-a716-446655440000",
            upload_status="ready",
            file_type="resume",
        )
        db_session.add(file)
        db_session.commit()

        assert file.owner_id == user.id  # type: ignore
        assert file.upload_status == "ready"  # type: ignore

    def test_userfile_foreign_key_constraint(self, db_session):
        file = UserFile(
            owner_id=uuid.uuid4(),
            original_filename="orphan.pdf",
            mime_type="application/pdf",
            size_bytes=1024,
            storage_key="resume/orphan-uuid",
            upload_status="ready",
            file_type="resume",
        )
        db_session.add(file)

        with pytest.raises(Exception):
            db_session.commit()
