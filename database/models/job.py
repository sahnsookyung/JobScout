import uuid

from sqlalchemy import Column, Integer, Text, TIMESTAMP, ForeignKey, Boolean, Date, Numeric, UniqueConstraint, Index
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from pgvector.sqlalchemy import Vector

from .base import Base


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

    # Facet Extraction State
    facet_status = Column(Text, default='pending')  # pending|in_progress|done|failed|quarantined
    facet_claimed_by = Column(Text)
    facet_claimed_at = Column(TIMESTAMP(timezone=True))
    facet_extraction_hash = Column(Text)
    facet_retry_count = Column(Integer, default=0)
    facet_last_error = Column(Text)

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
    benefits = relationship("JobBenefit", back_populates="job_post", cascade="all, delete-orphan")
    matches = relationship("JobMatch", back_populates="job_post", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('tenant_id', 'fingerprint_version', 'canonical_fingerprint', name='uq_job_post_fingerprint'),
        Index('idx_job_post_last_seen', 'last_seen_at'),
        Index('idx_job_post_company', 'company'),
        Index('idx_job_post_remote', 'is_remote'),
        Index('idx_job_post_tenant', 'tenant_id'),
        Index('idx_job_post_content_hash', 'content_hash'),
        # HNSW index for vector similarity search on summary_embedding
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

class JobBenefit(Base):
    __tablename__ = 'job_benefit'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey('job_post.id', ondelete='CASCADE'), nullable=False)

    category = Column(Text, nullable=False)
    text = Column(Text, nullable=False)
    ordinal = Column(Integer)

    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))

    job_post = relationship("JobPost", back_populates="benefits")

    __table_args__ = (
        Index('idx_jb_job', 'job_post_id'),
        Index('idx_jb_category', 'category'),
    )

class JobFacetEmbedding(Base):
    """
    Stores per-facet embeddings for job posts.

    Each job can have multiple facets describing perks/benefits/working conditions:
    - remote_flexibility
    - compensation
    - learning_growth
    - company_culture
    - work_life_balance
    - tech_stack
    - visa_sponsorship
    """
    __tablename__ = 'job_facet_embedding'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey('job_post.id', ondelete='CASCADE'), nullable=False)

    facet_key = Column(Text, nullable=False)
    facet_text = Column(Text, nullable=False)
    embedding = Column(Vector(1024), nullable=False)

    content_hash = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))

    __table_args__ = (
        UniqueConstraint('job_post_id', 'facet_key', name='uq_job_facet_job_key'),
        Index('idx_job_facet_job', 'job_post_id'),
        Index('idx_job_facet_key', 'facet_key'),
        Index('jru_facet_embedding_hnsw', 'embedding', postgresql_using='hnsw', postgresql_with={'m': 16, 'ef_construction': 64}, postgresql_ops={'embedding': 'vector_cosine_ops'}),
    )
