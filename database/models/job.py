import uuid

from sqlalchemy import Column, Integer, Text, TIMESTAMP, ForeignKey, Boolean, Date, Numeric, Index
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from pgvector.sqlalchemy import Vector

from .base import Base


# ============================================================================
# Constants - avoid duplication across models
# ============================================================================

# SQLAlchemy cascade specification
CASCADE_DELETE_ORPHAN = "all, delete-orphan"

# Server default for UTC timestamps
UTC_NOW = sql_text("timezone('UTC', now())")

# Common foreign key references
JOB_POST_ID_FK = 'job_post.id'
JOB_REQUIREMENT_UNIT_ID_FK = 'job_requirement_unit.id'

# Table name constants
JOB_POST_TABLE = 'job_post'
JOB_REQUIREMENT_UNIT_TABLE = 'job_requirement_unit'
TENANT_TABLE = 'tenant'
RESUME_TABLE = 'resume'
JOB_MATCH_TABLE = 'job_match'


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
    first_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    last_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    status = Column(Text, nullable=False, default='active') # active|expired|unknown
    
    # State Flags
    is_extracted = Column(Boolean, nullable=False, default=False)
    is_embedded = Column(Boolean, nullable=False, default=False)
    extraction_status = Column(Text, nullable=False, default='pending')
    extraction_attempts = Column(Integer, nullable=False, default=0)
    extraction_last_error = Column(Text)
    extraction_last_attempt_at = Column(TIMESTAMP(timezone=True))
    extraction_next_retry_at = Column(TIMESTAMP(timezone=True))
    embedding_status = Column(Text, nullable=False, default='pending')
    embedding_attempts = Column(Integer, nullable=False, default=0)
    embedding_last_error = Column(Text)
    embedding_last_attempt_at = Column(TIMESTAMP(timezone=True))
    embedding_next_retry_at = Column(TIMESTAMP(timezone=True))

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
    canonical_job_summary = Column(Text)
    canonical_job_summary_version = Column(
        Integer,
        nullable=False,
        default=1,
        server_default=sql_text("1"),
    )
    canonical_job_summary_hash = Column(Text)
    
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
    sources = relationship("JobPostSource", back_populates="job_post", cascade=CASCADE_DELETE_ORPHAN)
    requirements = relationship("JobRequirementUnit", back_populates="job_post", cascade=CASCADE_DELETE_ORPHAN)
    benefits = relationship("JobBenefit", back_populates="job_post", cascade=CASCADE_DELETE_ORPHAN)
    matches = relationship("JobMatch", back_populates="job_post", cascade=CASCADE_DELETE_ORPHAN)

    __table_args__ = (
        Index(
            'uq_job_post_tenant_fingerprint',
            'tenant_id',
            'fingerprint_version',
            'canonical_fingerprint',
            unique=True,
            postgresql_where=sql_text('tenant_id IS NOT NULL'),
        ),
        Index(
            'uq_job_post_global_fingerprint',
            'fingerprint_version',
            'canonical_fingerprint',
            unique=True,
            postgresql_where=sql_text('tenant_id IS NULL'),
        ),
        Index('idx_job_post_last_seen', 'last_seen_at'),
        Index('idx_job_post_company', 'company'),
        Index('idx_job_post_remote', 'is_remote'),
        Index('idx_job_post_tenant', 'tenant_id'),
        Index('idx_job_post_content_hash', 'content_hash'),
        Index('idx_job_post_extraction_retry', 'extraction_status', 'extraction_next_retry_at'),
        Index('idx_job_post_embedding_retry', 'embedding_status', 'embedding_next_retry_at'),
        # HNSW index for vector similarity search on summary_embedding
        Index('idx_job_post_summary_embedding_hnsw', 'summary_embedding', postgresql_using='hnsw', postgresql_with={'m': 16, 'ef_construction': 64}, postgresql_ops={'summary_embedding': 'vector_cosine_ops'}),
    )

class JobPostSource(Base):
    __tablename__ = 'job_post_source'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey(JOB_POST_ID_FK, ondelete='CASCADE'), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey(TENANT_TABLE + '.id', ondelete='CASCADE'), nullable=True)
    
    site = Column(Text, nullable=False)
    job_url = Column(Text, nullable=False)
    job_url_direct = Column(Text)
    source_job_id = Column(Text)
    date_posted = Column(Date)
    
    first_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    last_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    is_active = Column(Boolean, nullable=False, default=True)

    job_post = relationship("JobPost", back_populates="sources")

    __table_args__ = (
        Index('idx_job_post_source_job', 'job_post_id'),
        Index('idx_job_post_source_tenant', 'tenant_id'),
        Index('idx_job_post_source_seen', 'last_seen_at'),
        Index(
            'uq_job_post_source_tenant_site_url',
            'tenant_id',
            'site',
            'job_url',
            unique=True,
            postgresql_where=sql_text('tenant_id IS NOT NULL'),
        ),
        Index(
            'uq_job_post_source_global_site_url',
            'site',
            'job_url',
            unique=True,
            postgresql_where=sql_text('tenant_id IS NULL'),
        ),
    )

class JobRequirementUnit(Base):
    __tablename__ = 'job_requirement_unit'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey(JOB_POST_ID_FK, ondelete='CASCADE'), nullable=False)
    
    req_type = Column(Text, nullable=False) # required|preferred|responsibility|constraint|benefit
    text = Column(Text, nullable=False)
    tags = Column(JSONB, nullable=False, default={})
    ordinal = Column(Integer)
    
    # Experience requirement (parsed from text like "5+ years Python")
    min_years = Column(Integer)  # Minimum years required
    years_context = Column(Text)  # What the years refer to (e.g., "Python", "total")
    
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)

    job_post = relationship("JobPost", back_populates="requirements")
    embedding_row = relationship("JobRequirementUnitEmbedding", uselist=False, back_populates="unit", cascade=CASCADE_DELETE_ORPHAN)
    match_requirements = relationship("JobMatchRequirement", back_populates="requirement", cascade=CASCADE_DELETE_ORPHAN)

    __table_args__ = (
        Index('idx_jru_job', 'job_post_id'),
    )

class JobRequirementUnitEmbedding(Base):
    __tablename__ = 'job_requirement_unit_embedding'

    job_requirement_unit_id = Column(UUID(as_uuid=True), ForeignKey(JOB_REQUIREMENT_UNIT_ID_FK, ondelete='CASCADE'), primary_key=True)
    embedding = Column(Vector(1024), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)

    unit = relationship("JobRequirementUnit", back_populates="embedding_row")

    __table_args__ = (
        Index('jru_embedding_hnsw', 'embedding', postgresql_using='hnsw', postgresql_with={'m': 16, 'ef_construction': 64}, postgresql_ops={'embedding': 'vector_cosine_ops'}),
    )

class JobBenefit(Base):
    __tablename__ = 'job_benefit'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey(JOB_POST_ID_FK, ondelete='CASCADE'), nullable=False)

    category = Column(Text, nullable=False)
    text = Column(Text, nullable=False)
    ordinal = Column(Integer)

    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)

    job_post = relationship("JobPost", back_populates="benefits")

    __table_args__ = (
        Index('idx_jb_job', 'job_post_id'),
        Index('idx_jb_category', 'category'),
    )
