import uuid

from sqlalchemy import Column, ForeignKey, Index, Text, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import text as sql_text

from .base import Base

UTC_NOW = sql_text("timezone('UTC', now())")


class ResumeVariant(Base):
    """Job-specific generated resume draft, stored as JSON only."""

    __tablename__ = "resume_variant"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenant.id", ondelete="CASCADE"), nullable=True)
    match_id = Column(UUID(as_uuid=True), ForeignKey("job_match.id", ondelete="CASCADE"), nullable=False)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey("job_post.id", ondelete="CASCADE"), nullable=False)
    resume_fingerprint = Column(Text, nullable=False)

    template_key = Column(Text, nullable=False)
    template_version = Column(Text, nullable=False)
    generation_mode = Column(Text, nullable=False)
    tone = Column(Text, nullable=False)
    generator_version = Column(Text, nullable=False)
    renderer_version = Column(Text, nullable=False)
    evidence_policy_version = Column(Text, nullable=False)

    source_match_updated_at = Column(TIMESTAMP(timezone=True), nullable=False)
    source_match_calculated_at = Column(TIMESTAMP(timezone=True), nullable=False)
    source_job_content_hash = Column(Text, nullable=False, default="")
    source_resume_updated_at = Column(TIMESTAMP(timezone=True), nullable=False)
    source_resume_content_hash = Column(Text, nullable=False)

    content_json = Column(JSONB, nullable=False)
    evidence_map = Column(JSONB, nullable=False, default={})
    warnings = Column(JSONB, nullable=False, default=[])
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW, onupdate=UTC_NOW)

    __table_args__ = (
        Index("idx_resume_variant_owner_tenant_match", "owner_id", "tenant_id", "match_id"),
        Index("idx_resume_variant_owner_created", "owner_id", "created_at"),
        Index("idx_resume_variant_match", "match_id"),
        Index(
            "uq_resume_variant_current_tenant",
            "owner_id",
            "tenant_id",
            "match_id",
            "template_key",
            "template_version",
            "generation_mode",
            "tone",
            "generator_version",
            "renderer_version",
            "evidence_policy_version",
            "source_match_updated_at",
            "source_match_calculated_at",
            "source_job_content_hash",
            "source_resume_updated_at",
            "source_resume_content_hash",
            unique=True,
            postgresql_where=sql_text("tenant_id IS NOT NULL"),
        ),
        Index(
            "uq_resume_variant_current_global",
            "owner_id",
            "match_id",
            "template_key",
            "template_version",
            "generation_mode",
            "tone",
            "generator_version",
            "renderer_version",
            "evidence_policy_version",
            "source_match_updated_at",
            "source_match_calculated_at",
            "source_job_content_hash",
            "source_resume_updated_at",
            "source_resume_content_hash",
            unique=True,
            postgresql_where=sql_text("tenant_id IS NULL"),
        ),
    )
