import datetime
import uuid
import hashlib
import json
from typing import List, Dict, Any, Optional

from sqlalchemy import (
    Column, Integer, String, Boolean, Numeric, Text, ForeignKey, TIMESTAMP,
    Date, func, UniqueConstraint, Index, DateTime, Float
)
from sqlalchemy.sql import text as sql_text
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.dialects.postgresql import UUID, JSONB
from pgvector.sqlalchemy import Vector

Base = declarative_base()

class Tenant(Base):
    __tablename__ = 'tenant'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))

class JobPost(Base):
    __tablename__ = 'job_post'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey('tenant.id', ondelete='CASCADE'), nullable=True)

    # Core Identity
    title = Column(Text, nullable=False)
    company = Column(Text, nullable=False)
    location_text = Column(Text)
    is_remote = Column(Boolean)
    
    # Fingerprinting / Tracking
    canonical_fingerprint = Column(Text, nullable=False)
    fingerprint_version = Column(Integer, nullable=False, default=1)
    first_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    last_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    status = Column(Text, nullable=False, default='active') # active|expired|unknown
    
    # State Flags
    is_extracted = Column(Boolean, nullable=False, default=False)
    is_embedded = Column(Boolean, nullable=False, default=False)

    # === Structural Fields (Metadata) ===
    job_type = Column(Text)
    job_level = Column(Text)
    currency = Column(Text)
    salary_min = Column(Numeric)
    salary_max = Column(Numeric)
    salary_interval = Column(Text)
    min_years_experience = Column(Integer)
    requires_degree = Column(Boolean)
    security_clearance = Column(Boolean)

    # === Content Fields (Merged from JobPostContent) ===
    description = Column(Text)
    skills_raw = Column(Text) # CSV or raw string
    raw_payload = Column(JSONB, nullable=False, default={})
    content_hash = Column(Text)  # Hash of description for content change detection
    
    # Extended Company/Job Info
    emails = Column(Text)
    company_industry = Column(Text)
    company_url = Column(Text)
    company_logo = Column(Text)
    company_url_direct = Column(Text)
    company_addresses = Column(Text)
    company_num_employees = Column(Text)
    company_revenue = Column(Text)
    company_description = Column(Text)
    experience_range = Column(Text)
    company_rating = Column(Numeric)
    company_reviews_count = Column(Integer)
    vacancy_count = Column(Integer)
    work_from_home_type = Column(Text)

    # Coarse embedding for the whole job
    summary_embedding = Column(Vector(1024))

    # Relationships
    sources = relationship("JobPostSource", back_populates="job_post", cascade="all, delete-orphan")
    requirements = relationship("JobRequirementUnit", back_populates="job_post", cascade="all, delete-orphan")
    matches = relationship("JobMatch", back_populates="job_post", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('tenant_id', 'fingerprint_version', 'canonical_fingerprint', name='uq_job_post_fingerprint'),
        Index('idx_job_post_last_seen', 'last_seen_at'),
        Index('idx_job_post_company', 'company'),
        Index('idx_job_post_remote', 'is_remote'),
        Index('idx_job_post_tenant', 'tenant_id'),
        Index('idx_job_post_content_hash', 'content_hash'),
        # HNSW index for vector similarity search on summary_embedding (DR-1)
        Index('idx_job_post_summary_embedding_hnsw', 'summary_embedding', postgresql_using='hnsw', postgresql_with={'m': 16, 'ef_construction': 64}, postgresql_ops={'summary_embedding': 'vector_cosine_ops'}),
    )

class JobPostSource(Base):
    __tablename__ = 'job_post_source'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey('job_post.id', ondelete='CASCADE'), nullable=False)
    
    site = Column(Text, nullable=False)
    job_url = Column(Text, nullable=False)
    job_url_direct = Column(Text)
    source_job_id = Column(Text)
    date_posted = Column(Date)
    
    first_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    last_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    is_active = Column(Boolean, nullable=False, default=True)

    job_post = relationship("JobPost", back_populates="sources")

    __table_args__ = (
        UniqueConstraint('site', 'job_url', name='uq_job_post_source_site_url'),
        Index('idx_job_post_source_job', 'job_post_id'),
        Index('idx_job_post_source_seen', 'last_seen_at'),
    )

class JobRequirementUnit(Base):
    __tablename__ = 'job_requirement_unit'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey('job_post.id', ondelete='CASCADE'), nullable=False)
    
    req_type = Column(Text, nullable=False) # required|preferred|responsibility|constraint|benefit
    text = Column(Text, nullable=False)
    tags = Column(JSONB, nullable=False, default={})
    ordinal = Column(Integer)
    
    # Experience requirement (parsed from text like "5+ years Python")
    min_years = Column(Integer)  # Minimum years required
    years_context = Column(Text)  # What the years refer to (e.g., "Python", "total")
    
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))

    job_post = relationship("JobPost", back_populates="requirements")
    embedding_row = relationship("JobRequirementUnitEmbedding", uselist=False, back_populates="unit", cascade="all, delete-orphan")
    match_requirements = relationship("JobMatchRequirement", back_populates="requirement", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_jru_job', 'job_post_id'),
    )

class JobRequirementUnitEmbedding(Base):
    __tablename__ = 'job_requirement_unit_embedding'

    job_requirement_unit_id = Column(UUID(as_uuid=True), ForeignKey('job_requirement_unit.id', ondelete='CASCADE'), primary_key=True)
    embedding = Column(Vector(1024), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))

    unit = relationship("JobRequirementUnit", back_populates="embedding_row")

    __table_args__ = (
        Index('jru_embedding_hnsw', 'embedding', postgresql_using='hnsw', postgresql_with={'m': 16, 'ef_construction': 64}, postgresql_ops={'embedding': 'vector_cosine_ops'}),
    )

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

class JobMatch(Base):
    """
    Stores match results between a resume and a job post.
    
    Tracks:
    - Overall job-level match (JD alignment)
    - Requirement-level matches (skills coverage)
    - Scores with penalties
    - Invalidation tracking via fingerprints
    """
    __tablename__ = 'job_match'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey('job_post.id', ondelete='CASCADE'), nullable=False)
    
    # Resume identification for invalidation
    resume_fingerprint = Column(Text, nullable=False)  # Hash of resume content
    resume_version = Column(Text, nullable=True)  # Optional version identifier
    
    # Job content identification for invalidation
    job_content_hash = Column(Text, nullable=True)  # Hash of job content at match time
    
    # Job-level matching (JD alignment)
    job_similarity = Column(Numeric(3, 2))  # Overall JD similarity score (0.00-1.00)
    
    # Aggregate requirement-level scores
    overall_score = Column(Numeric(5, 2))  # Final weighted score (0.00-100.00)
    base_score = Column(Numeric(5, 2))  # Score before penalties
    penalties = Column(Numeric(5, 2), default=0)  # Total penalty points
    penalty_details = Column(JSONB, default={})  # Detailed penalty breakdown
    
    # Coverage metrics
    required_coverage = Column(Numeric(3, 2))  # Fraction of required requirements covered (0.00-1.00)
    preferred_coverage = Column(Numeric(3, 2))  # Fraction of preferred requirements covered (0.00-1.00)
    total_requirements = Column(Integer, default=0)  # Total requirements matched against
    matched_requirements_count = Column(Integer, default=0)  # Number matched
    
    # Matching configuration used
    match_type = Column(Text, default='requirements_only')  # 'requirements_only' or 'with_preferences'
    preferences_file_hash = Column(Text, nullable=True)  # Hash of preferences file if used
    similarity_threshold = Column(Numeric(3, 2), default=0.50)  # Threshold used for matching
    
    # Status and timestamps
    status = Column(Text, default='active')  # active|stale|invalidated
    invalidated_reason = Column(Text, nullable=True)  # Reason if invalidated
    notified = Column(Boolean, default=False)  # Whether user was notified of this match
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"), onupdate=sql_text("timezone('UTC', now())"))
    calculated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    
    # Relationships
    job_post = relationship("JobPost", back_populates="matches")
    requirement_matches = relationship("JobMatchRequirement", back_populates="job_match", cascade="all, delete-orphan")

    __table_args__ = (
        # Unique constraint: one match per job-resume combination
        UniqueConstraint('job_post_id', 'resume_fingerprint', name='uq_job_match_job_resume'),
        # Indexes for common queries
        Index('idx_job_match_resume', 'resume_fingerprint'),
        Index('idx_job_match_score', 'overall_score'),
        Index('idx_job_match_status', 'status'),
        Index('idx_job_match_notified', 'notified'),
        Index('idx_job_match_calculated', 'calculated_at'),
        Index('idx_job_match_created', 'created_at'),
    )


class JobMatchRequirement(Base):
    """
    Individual requirement-level matches.
    
    Stores the specific evidence that matched each job requirement,
    enabling explainability and debugging.
    """
    __tablename__ = 'job_match_requirement'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_match_id = Column(UUID(as_uuid=True), ForeignKey('job_match.id', ondelete='CASCADE'), nullable=False)
    job_requirement_unit_id = Column(UUID(as_uuid=True), ForeignKey('job_requirement_unit.id', ondelete='CASCADE'), nullable=False)
    
    # Evidence details
    evidence_text = Column(Text, nullable=False)  # The resume evidence text that matched
    evidence_section = Column(Text, nullable=True)  # Section of resume (e.g., "Skills", "Experience")
    evidence_tags = Column(JSONB, default={})  # Tags from the evidence
    
    # Match details
    similarity_score = Column(Numeric(3, 2), nullable=False)  # Cosine similarity (0.00-1.00)
    is_covered = Column(Boolean, default=False)  # Whether this meets threshold
    req_type = Column(Text, nullable=False)  # required|preferred|responsibility|constraint|benefit
    
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))

    # Relationships
    job_match = relationship("JobMatch", back_populates="requirement_matches")
    requirement = relationship("JobRequirementUnit", back_populates="match_requirements")

    __table_args__ = (
        # Index for finding matches by requirement
        Index('idx_jmr_requirement', 'job_requirement_unit_id'),
        Index('idx_jmr_match', 'job_match_id'),
        Index('idx_jmr_similarity', 'similarity_score'),
        Index('idx_jmr_covered', 'is_covered'),
    )


class StructuredResume(Base):
    """
    Stores structured resume extraction results.
    
    Contains AI-extracted structured data from resume with date-based
    experience calculations for accurate years-of-experience validation.
    """
    __tablename__ = 'structured_resume'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resume_fingerprint = Column(Text, nullable=False, unique=True, index=True)  # Hash of resume content
    
    # Raw extraction result
    extracted_data = Column(JSONB, nullable=False)  # Full structured extraction
    
    # Calculated experience metrics
    calculated_total_years = Column(Numeric(4, 1))  # Sum of all experience periods from dates
    claimed_total_years = Column(Numeric(4, 1))  # From summary section if stated
    experience_validated = Column(Boolean, default=False)  # Whether claim matches calculation
    validation_message = Column(Text)  # Details of validation result
    
    # Extraction metadata
    extraction_confidence = Column(Numeric(3, 2))  # 0.00-1.00
    extraction_warnings = Column(JSONB, default=[])  # List of warning messages
    
    # Timestamps
    extracted_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"), onupdate=sql_text("timezone('UTC', now())"))

    __table_args__ = (
        # Index for finding resumes by fingerprint
        Index('idx_structured_resume_fingerprint', 'resume_fingerprint'),
        # Index for experience queries
        Index('idx_structured_resume_years', 'calculated_total_years'),
    )


class NotificationTracker(Base):
    """
    Tracks sent notifications for deduplication.
    
    Prevents notification fatigue by ensuring the same event
    (e.g., job match) is not repeatedly notified to the user.
    """
    __tablename__ = 'notification_tracker'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # What was notified
    user_id = Column(Text, nullable=False, index=True)
    job_match_id = Column(UUID(as_uuid=True), ForeignKey('job_match.id', ondelete='CASCADE'), nullable=True)
    notification_type = Column(Text, nullable=False)  # email, discord, telegram, etc.
    channel_type = Column(Text, nullable=False)  # email, discord, telegram, slack, etc.
    
    # Deduplication key - hash of user + job + event type
    dedup_hash = Column(Text, nullable=False, index=True)
    
    # Notification content hash (to detect content changes)
    content_hash = Column(Text, nullable=True)
    
    # Event that triggered notification
    event_type = Column(Text, nullable=False)  # new_match, score_improved, batch_complete, etc.
    event_data = Column(JSONB, default={})  # Additional event context
    
    # Notification metadata
    recipient = Column(Text, nullable=False)  # email, discord webhook, telegram chat id
    subject = Column(Text)
    sent_successfully = Column(Boolean, default=False)
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    first_sent_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    last_sent_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    send_count = Column(Integer, default=1)  # How many times this was sent (for resends)
    
    # Resend policy
    allow_resend = Column(Boolean, default=False)  # Whether to allow resending
    resend_interval_hours = Column(Integer, default=24)  # Minimum hours between resends
    
    # Relationships
    job_match = relationship("JobMatch", backref="notifications")
    
    __table_args__ = (
        # Unique constraint on dedup hash
        UniqueConstraint('dedup_hash', name='uq_notification_dedup'),
        # Index for querying user's notifications
        Index('idx_notification_user', 'user_id', 'first_sent_at'),
        # Index for checking recent notifications
        Index('idx_notification_recent', 'dedup_hash', 'last_sent_at'),
    )


def generate_resume_fingerprint(resume_data: Dict[str, Any]) -> str:
    """
    Generate a fingerprint for a resume to track changes.
    
    Uses SHA-256 hash of normalized resume JSON.
    """
    # Normalize by sorting keys and converting to canonical JSON
    normalized = json.dumps(resume_data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:32]


def generate_preferences_fingerprint(preferences_data: Dict[str, Any]) -> str:
    """
    Generate a fingerprint for preferences file.
    """
    normalized = json.dumps(preferences_data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:32]
