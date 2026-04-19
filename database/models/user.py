import uuid

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Enum,
    ForeignKey,
    Index,
    Text,
    TIMESTAMP,
    UniqueConstraint,
)
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .base import Base

UTC_NOW = sql_text("timezone('UTC', now())")

CASCADE_DELETE_ORPHAN = "all, delete-orphan"


class User(Base):
    """Canonical app user/profile record."""

    __tablename__ = 'users'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(Text, nullable=False, unique=True)
    display_name = Column(Text)
    email_verified_at = Column(TIMESTAMP(timezone=True))
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)

    files = relationship("UserFile", back_populates="owner", cascade=CASCADE_DELETE_ORPHAN)
    auth_identities = relationship(
        "UserAuthIdentity",
        back_populates="user",
        cascade=CASCADE_DELETE_ORPHAN,
    )

    __table_args__ = (
        Index('idx_users_email', 'email'),
    )


class UserAuthIdentity(Base):
    """External or local auth identity linked to a user profile."""

    __tablename__ = 'user_auth_identity'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    provider = Column(
        Enum('google', 'password', name='auth_provider', create_type=False),
        nullable=False,
    )
    provider_subject = Column(Text, nullable=False)
    email = Column(Text)
    email_normalized = Column(Text)
    email_verified = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default=sql_text("FALSE"),
    )
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)

    user = relationship("User", back_populates="auth_identities")

    __table_args__ = (
        Index('idx_user_auth_identity_email', 'email_normalized'),
        Index('idx_user_auth_identity_user_provider', 'user_id', 'provider'),
        UniqueConstraint(
            'provider',
            'provider_subject',
            name='uq_user_auth_identity_provider_subject',
        ),
    )


class UserFile(Base):
    """Uploaded file metadata with server-generated storage keys."""

    __tablename__ = 'user_files'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    original_filename = Column(Text, nullable=False)
    mime_type = Column(Text, nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    storage_key = Column(Text, nullable=False, unique=True)

    upload_status = Column(
        Enum('pending', 'scanned', 'rejected', 'ready', name='upload_status', create_type=False),
        nullable=False,
        default='pending',
        server_default=sql_text("'pending'::upload_status"),
    )
    file_type = Column(
        Enum('resume', name='file_type', create_type=False),
        nullable=False,
    )
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)

    owner = relationship("User", back_populates="files")

    __table_args__ = (
        Index('idx_user_files_owner_type', 'owner_id', 'file_type'),
        Index('idx_user_files_storage_key', 'storage_key'),
        Index('idx_user_files_created_at', 'created_at'),
    )
