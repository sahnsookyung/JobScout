import uuid
import xxhash
from typing import Any

from sqlalchemy import Column, Text, TIMESTAMP, Integer, Boolean, Numeric, Float, Index, ForeignKey
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from pgvector.sqlalchemy import Vector

from .base import Base

UTC_NOW_SQL = "timezone('UTC', now())"
RESUME_FINGERPRINT_VERSION = 1
SYSTEM_OWNER_ID = "00000000-0000-0000-0000-000000000001"
USERS_ID_FK = "users.id"

RESUME_PROCESSING_EXTRACTING = "extracting"
RESUME_PROCESSING_EXTRACTED = "extracted"
RESUME_PROCESSING_EMBEDDING = "embedding"
RESUME_PROCESSING_READY = "ready"
RESUME_PROCESSING_FAILED = "failed"

RESUME_PROCESSING_STATUSES = {
    RESUME_PROCESSING_EXTRACTING,
    RESUME_PROCESSING_EXTRACTED,
    RESUME_PROCESSING_EMBEDDING,
    RESUME_PROCESSING_READY,
    RESUME_PROCESSING_FAILED,
}

RESUME_UPLOAD_PENDING = "pending"
RESUME_UPLOAD_IN_PROGRESS = "in_progress"
RESUME_UPLOAD_READY = "ready"
RESUME_UPLOAD_FAILED_RETRYABLE = "failed_retryable"
RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED = "failed_reupload_required"

RESUME_UPLOAD_STATUSES = {
    RESUME_UPLOAD_PENDING,
    RESUME_UPLOAD_IN_PROGRESS,
    RESUME_UPLOAD_READY,
    RESUME_UPLOAD_FAILED_RETRYABLE,
    RESUME_UPLOAD_FAILED_REUPLOAD_REQUIRED,
}


class ResumeSectionEmbedding(Base):
    """Stores embeddings for individual resume sections."""

    __tablename__ = 'resume_section_embedding'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey(USERS_ID_FK, ondelete='CASCADE'), nullable=False, index=True)
    fingerprint_version = Column(Integer, nullable=False, default=RESUME_FINGERPRINT_VERSION)
    resume_fingerprint = Column(Text, nullable=False, index=True)
    section_type = Column(Text, nullable=False)
    section_index = Column(Integer, nullable=False)
    source_text = Column(Text, nullable=False)
    source_data = Column(JSONB, nullable=False)
    embedding = Column(Vector(1024), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text(UTC_NOW_SQL))

    __table_args__ = (
        Index('idx_rse_resume', 'resume_fingerprint', 'section_type', 'section_index'),
        Index('idx_rse_owner_resume', 'owner_id', 'resume_fingerprint'),
        Index(
            'idx_rse_embedding_hnsw',
            'embedding',
            postgresql_using='hnsw',
            postgresql_with={'m': 16, 'ef_construction': 64},
            postgresql_ops={'embedding': 'vector_cosine_ops'},
        ),
    )


class ResumeEvidenceUnitEmbedding(Base):
    """Stores embeddings for individual resume evidence units."""

    __tablename__ = 'resume_evidence_unit_embedding'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey(USERS_ID_FK, ondelete='CASCADE'), nullable=False, index=True)
    fingerprint_version = Column(Integer, nullable=False, default=RESUME_FINGERPRINT_VERSION)
    resume_fingerprint = Column(Text, nullable=False, index=True)
    evidence_unit_id = Column(Text, nullable=False)
    source_text = Column(Text, nullable=False)
    source_section = Column(Text)
    tags = Column(JSONB, default={})
    embedding = Column(Vector(1024), nullable=False)
    years_value = Column(Float)
    years_context = Column(Text)
    is_total_years_claim = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text(UTC_NOW_SQL))

    __table_args__ = (
        Index('idx_rfue_fingerprint', 'resume_fingerprint'),
        Index('idx_rfue_owner_resume', 'owner_id', 'resume_fingerprint'),
        Index(
            'idx_rfue_embedding_hnsw',
            'embedding',
            postgresql_using='hnsw',
            postgresql_with={'m': 16, 'ef_construction': 64},
            postgresql_ops={'embedding': 'vector_cosine_ops'},
        ),
    )


class StructuredResume(Base):
    """Stores structured resume extraction results."""

    __tablename__ = 'structured_resume'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey(USERS_ID_FK, ondelete='CASCADE'), nullable=False, index=True)
    fingerprint_version = Column(Integer, nullable=False, default=RESUME_FINGERPRINT_VERSION)
    resume_fingerprint = Column(Text, nullable=False, unique=True, index=True)
    extracted_data = Column(JSONB, nullable=False)
    total_experience_years = Column(Numeric(4, 1))
    extraction_confidence = Column(Numeric(3, 2))
    extraction_warnings = Column(JSONB, default=[])
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text(UTC_NOW_SQL))
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text(UTC_NOW_SQL), onupdate=sql_text(UTC_NOW_SQL))

    __table_args__ = (
        Index('idx_structured_resume_fingerprint', 'resume_fingerprint'),
        Index('idx_structured_resume_owner_resume', 'owner_id', 'resume_fingerprint'),
        Index('idx_structured_resume_years', 'total_experience_years'),
    )


class ResumeProcessingState(Base):
    """Durable fingerprint-scoped processing state for resume ETL readiness."""

    __tablename__ = 'resume_processing_state'

    resume_fingerprint = Column(Text, primary_key=True)
    owner_id = Column(UUID(as_uuid=True), ForeignKey(USERS_ID_FK, ondelete='CASCADE'), nullable=False, index=True)
    fingerprint_version = Column(Integer, nullable=False, default=RESUME_FINGERPRINT_VERSION)
    processing_status = Column(Text, nullable=False)
    last_error = Column(Text, nullable=True)
    failure_stage = Column(Text, nullable=True)
    failure_class = Column(Text, nullable=True)
    retryable = Column(Boolean, nullable=True)
    user_safe_message = Column(Text, nullable=True)
    extraction_completed_at = Column(TIMESTAMP(timezone=True))
    embedding_completed_at = Column(TIMESTAMP(timezone=True))
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sql_text(UTC_NOW_SQL),
    )
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sql_text(UTC_NOW_SQL),
        onupdate=sql_text(UTC_NOW_SQL),
    )

    __table_args__ = (
        Index('idx_resume_processing_state_status', 'processing_status'),
        Index('idx_resume_processing_state_updated_at', 'updated_at'),
        Index('idx_resume_processing_state_owner', 'owner_id', 'updated_at'),
    )


class ResumeUpload(Base):
    """Ordered ledger of upload intents for resume selection."""

    __tablename__ = 'resume_upload'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey(USERS_ID_FK, ondelete='CASCADE'), nullable=False, index=True)
    resume_hash = Column(Text, nullable=False, index=True)
    fingerprint_version = Column(Integer, nullable=False, default=RESUME_FINGERPRINT_VERSION)
    resume_fingerprint = Column(Text, nullable=False, index=True)
    original_filename = Column(Text, nullable=True)
    status = Column(Text, nullable=False, default=RESUME_UPLOAD_PENDING)
    last_error = Column(Text, nullable=True)
    failure_stage = Column(Text, nullable=True)
    failure_class = Column(Text, nullable=True)
    retryable = Column(Boolean, nullable=True)
    user_safe_message = Column(Text, nullable=True)
    failure_debug_context = Column(JSONB, nullable=True)
    processing_task_id = Column(Text, nullable=True)
    retry_of_upload_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sql_text(UTC_NOW_SQL),
    )

    __table_args__ = (
        Index('idx_resume_upload_owner_created', 'owner_id', 'created_at'),
        Index('idx_resume_upload_owner_hash', 'owner_id', 'resume_hash'),
        Index('idx_resume_upload_owner_fingerprint', 'owner_id', 'resume_fingerprint'),
        Index('idx_resume_upload_processing_task_id', 'processing_task_id'),
    )


def generate_file_fingerprint(file_bytes: bytes) -> str:
    """Generate a fingerprint for a resume file based on raw file bytes."""
    return xxhash.xxh64_hexdigest(file_bytes)


def generate_resume_fingerprint(owner_id: Any, resume_hash: str, version: int = RESUME_FINGERPRINT_VERSION) -> str:
    """Derive a versioned per-user canonical fingerprint from a raw resume hash."""
    payload = f"v{version}:{owner_id}:{resume_hash}".encode("utf-8")
    return xxhash.xxh64_hexdigest(payload)
