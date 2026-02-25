import uuid
import hashlib
import json
from typing import Dict, Any

from sqlalchemy import Column, Text, TIMESTAMP, Integer, Boolean, Numeric, Float, Index
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from pgvector.sqlalchemy import Vector

from .base import Base


class ResumeSectionEmbedding(Base):
    """
    Stores embeddings for individual resume sections.
    
    Each resume is broken down into sections (experience, projects, skills, summary)
    and each section gets its own embedding for granular matching against job requirements.
    """
    __tablename__ = 'resume_section_embedding'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resume_fingerprint = Column(Text, nullable=False, index=True)  # Links to structured_resume
    
    # Section identification
    section_type = Column(Text, nullable=False)  # experience|project|skill|summary|education
    section_index = Column(Integer, nullable=False)  # Index within section type (0, 1, 2...)
    
    # Source text that was embedded
    source_text = Column(Text, nullable=False)  # The text that was embedded
    source_data = Column(JSONB, nullable=False)  # Full structured data for this section
    
    # Embedding
    embedding = Column(Vector(1024), nullable=False)
    
    # Metadata
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    
    __table_args__ = (
        # Composite index for retrieving all sections of a resume
        Index('idx_rse_resume', 'resume_fingerprint', 'section_type', 'section_index'),
        # HNSW index for similarity search
        Index('idx_rse_embedding_hnsw', 'embedding', postgresql_using='hnsw', postgresql_with={'m': 16, 'ef_construction': 64}, postgresql_ops={'embedding': 'vector_cosine_ops'}),
    )

class ResumeEvidenceUnitEmbedding(Base):
    """
    Stores embeddings for individual resume evidence units.

    Each evidence unit (description, highlight from resume) gets its own
    embedding for fine-grained matching against job requirements.
    """
    __tablename__ = 'resume_evidence_unit_embedding'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resume_fingerprint = Column(Text, nullable=False, index=True)

    evidence_unit_id = Column(Text, nullable=False)  # Links to ResumeEvidenceUnit.id
    source_text = Column(Text, nullable=False)
    source_section = Column(Text)  # Which resume section (Experience, Skills, etc.)
    tags = Column(JSONB, default={})  # Metadata (company, title, skill, type, etc.)

    embedding = Column(Vector(1024), nullable=False)
    years_value = Column(Float)  # Extracted years of experience
    years_context = Column(Text)  # What the years refer to (e.g., "Python", "total experience")
    is_total_years_claim = Column(Boolean, default=False)  # Whether this is a total years claim

    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))

    __table_args__ = (
        Index('idx_rfue_fingerprint', 'resume_fingerprint'),
        Index('idx_rfue_embedding_hnsw', 'embedding', postgresql_using='hnsw',
              postgresql_with={'m': 16, 'ef_construction': 64},
              postgresql_ops={'embedding': 'vector_cosine_ops'}),
    )


class StructuredResume(Base):
    """
    Stores structured resume extraction results.
    
    Contains AI-extracted structured data from resume with claimed
    years of experience from the summary section.
    """
    __tablename__ = 'structured_resume'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resume_fingerprint = Column(Text, nullable=False, unique=True, index=True)  # Hash of resume content
    
    # Raw extraction result
    extracted_data = Column(JSONB, nullable=False)  # Full structured extraction
    
    # Experience (claimed by candidate from summary section)
    total_experience_years = Column(Numeric(4, 1))  # From profile.summary.total_experience_years
    
    # Extraction metadata
    extraction_confidence = Column(Numeric(3, 2))  # 0.00-1.00
    extraction_warnings = Column(JSONB, default=[])  # List of warning messages
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"), onupdate=sql_text("timezone('UTC', now())"))

    __table_args__ = (
        # Index for finding resumes by fingerprint
        Index('idx_structured_resume_fingerprint', 'resume_fingerprint'),
        # Index for experience queries
        Index('idx_structured_resume_years', 'total_experience_years'),
    )


class FingerprintGenerator:
    """Utility class for generating fingerprints from data using SHA-256."""

    @staticmethod
    def generate(data: Dict[str, Any]) -> str:
        """
        Generate a fingerprint from data using SHA-256 hash of normalized JSON.

        Args:
            data: Dictionary to generate fingerprint for

        Returns:
            First 32 characters of SHA-256 hash
        """
        normalized = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:32]


def generate_resume_fingerprint(resume_data: Dict[str, Any]) -> str:
    """
    Generate a fingerprint for a resume to track changes.

    Uses SHA-256 hash of normalized resume JSON.
    """
    return FingerprintGenerator.generate(resume_data)


def generate_file_fingerprint(file_bytes: bytes) -> str:
    """
    Generate a fingerprint for a resume file based on raw file bytes.

    This is used for deduplication - same file = same hash regardless of parsing.
    The hash is computed from the raw file content (not parsed/normalized data).

    Args:
        file_bytes: Raw bytes of the resume file

    Returns:
        First 32 characters of SHA-256 hash of the file bytes
    """
    return hashlib.sha256(file_bytes).hexdigest()[:32]
