import datetime
import uuid

from sqlalchemy import (
    Column, Integer, String, Boolean, Numeric, Text, ForeignKey, TIMESTAMP,
    Date, func, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.dialects.postgresql import UUID, JSONB
from pgvector.sqlalchemy import Vector

Base = declarative_base()

class Tenant(Base):
    __tablename__ = 'tenant'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())

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
    first_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())
    last_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())
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

    __table_args__ = (
        UniqueConstraint('tenant_id', 'fingerprint_version', 'canonical_fingerprint', name='uq_job_post_fingerprint'),
        Index('idx_job_post_last_seen', 'last_seen_at'),
        Index('idx_job_post_company', 'company'),
        Index('idx_job_post_remote', 'is_remote'),
        Index('idx_job_post_tenant', 'tenant_id'),
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
    
    first_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())
    last_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())
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
    
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())

    job_post = relationship("JobPost", back_populates="requirements")
    embedding_row = relationship("JobRequirementUnitEmbedding", uselist=False, back_populates="unit", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_jru_job', 'job_post_id'),
    )

class JobRequirementUnitEmbedding(Base):
    __tablename__ = 'job_requirement_unit_embedding'

    job_requirement_unit_id = Column(UUID(as_uuid=True), ForeignKey('job_requirement_unit.id', ondelete='CASCADE'), primary_key=True)
    embedding = Column(Vector(1024), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())

    unit = relationship("JobRequirementUnit", back_populates="embedding_row")

    __table_args__ = (
        Index('jru_embedding_hnsw', 'embedding', postgresql_using='hnsw', postgresql_with={'m': 16, 'ef_construction': 64}, postgresql_ops={'embedding': 'vector_cosine_ops'}),
    )
