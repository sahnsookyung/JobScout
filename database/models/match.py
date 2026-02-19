import uuid

from sqlalchemy import Column, Text, TIMESTAMP, ForeignKey, Boolean, Integer, Numeric, UniqueConstraint, Index
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from .base import Base


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

    resume_fingerprint = Column(Text, nullable=False)
    resume_version = Column(Text, nullable=True)

    job_content_hash = Column(Text, nullable=True)

    job_similarity = Column(Numeric(3, 2))

    fit_score = Column(Numeric(5, 2))
    want_score = Column(Numeric(5, 2))
    overall_score = Column(Numeric(5, 2))

    fit_components = Column(JSONB, default={})
    want_components = Column(JSONB, default={})

    fit_weight = Column(Numeric(3, 2))
    want_weight = Column(Numeric(3, 2))

    base_score = Column(Numeric(5, 2))
    penalties = Column(Numeric(5, 2), default=0)
    penalty_details = Column(JSONB, default={})

    required_coverage = Column(Numeric(3, 2))
    preferred_coverage = Column(Numeric(3, 2))
    total_requirements = Column(Integer, default=0)
    matched_requirements_count = Column(Integer, default=0)

    match_type = Column(Text, default='requirements_only')
    similarity_threshold = Column(Numeric(3, 2), default=0.50)

    status = Column(Text, default='active')
    invalidated_reason = Column(Text, nullable=True)
    notified = Column(Boolean, default=False)
    is_hidden = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"), onupdate=sql_text("timezone('UTC', now())"))
    calculated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))

    job_post = relationship("JobPost", back_populates="matches")
    requirement_matches = relationship("JobMatchRequirement", back_populates="job_match", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('job_post_id', 'resume_fingerprint', name='uq_job_match_job_resume'),
        Index('idx_job_match_resume', 'resume_fingerprint'),
        Index('idx_job_match_score', 'overall_score'),
        Index('idx_job_match_fit', 'fit_score'),
        Index('idx_job_match_want', 'want_score'),
        Index('idx_job_match_status', 'status'),
        Index('idx_job_match_notified', 'notified'),
        Index('idx_job_match_hidden', 'is_hidden'),
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
