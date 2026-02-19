import uuid

from sqlalchemy import Column, Text, Boolean, Integer, TIMESTAMP, ForeignKey, func, Enum, BigInteger, Index
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .base import Base

class User(Base):
    """
    User account with authentication and audit fields.
    """
    __tablename__ = 'users'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(Text, nullable=False, unique=True)
    password_hash = Column(Text, nullable=False)
    display_name = Column(Text)
    
    # Verification / status
    email_verified_at = Column(TIMESTAMP(timezone=True))
    is_active = Column(Boolean, nullable=False, default=True)
    
    # Brute force protection
    failed_login_attempts = Column(Integer, nullable=False, default=0)
    locked_until = Column(TIMESTAMP(timezone=True))
    
    # Audit
    last_login_at = Column(TIMESTAMP(timezone=True))
    last_login_ip = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"), onupdate=func.now())
    deleted_at = Column(TIMESTAMP(timezone=True))
    
    # Relationships
    files = relationship("UserFile", back_populates="owner", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_users_email', 'email'),
        Index('idx_users_deleted_at', 'deleted_at', postgresql_where=deleted_at.is_(None)),
    )


class UserFile(Base):
    """
    Uploaded file metadata with server-generated storage keys.
    """
    __tablename__ = 'user_files'
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    
    # Display only - never used in paths
    original_filename = Column(Text, nullable=False)
    
    # Server-validated mime type
    mime_type = Column(Text, nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    
    # Server-generated storage key: {file_type}/{uuid}
    storage_key = Column(Text, nullable=False, unique=True)
    
    # Upload status
    upload_status = Column(
        Enum('pending', 'scanned', 'rejected', 'ready', name='upload_status', create_type=False),
        nullable=False,
        default='pending'
    )
    
    # File type
    file_type = Column(
        Enum('resume', name='file_type', create_type=False),
        nullable=False
    )
    
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    
    # Relationships
    owner = relationship("User", back_populates="files")
    
    __table_args__ = (
        Index('idx_user_files_owner_type', 'owner_id', 'file_type'),
        Index('idx_user_files_storage_key', 'storage_key'),
        Index('idx_user_files_created_at', 'created_at'),
    )
