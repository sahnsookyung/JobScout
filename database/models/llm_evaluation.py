import uuid

from sqlalchemy import Boolean, Column, ForeignKey, Index, Numeric, Text, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import text as sql_text

from .base import Base

UTC_NOW = sql_text("timezone('UTC', now())")

LLM_EVALUATION_PENDING = "pending"
LLM_EVALUATION_RUNNING = "running"
LLM_EVALUATION_SUCCEEDED = "succeeded"
LLM_EVALUATION_FAILED = "failed"
LLM_EVALUATION_SKIPPED = "skipped"
LLM_EVALUATION_DELETED = "deleted"


class LlmMatchEvaluation(Base):
    """Per-user cached LLM evaluation of a resume/job match."""

    __tablename__ = "llm_match_evaluation"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenant.id", ondelete="CASCADE"), nullable=True)
    job_post_id = Column(UUID(as_uuid=True), ForeignKey("job_post.id", ondelete="CASCADE"), nullable=False)
    job_match_id = Column(UUID(as_uuid=True), ForeignKey("job_match.id", ondelete="SET NULL"), nullable=True)
    resume_fingerprint = Column(Text, nullable=False)

    provider = Column(Text, nullable=False)
    model = Column(Text, nullable=False)
    prompt_version = Column(Text, nullable=False)
    schema_version = Column(Text, nullable=False)
    judge_config_hash = Column(Text, nullable=False)
    evidence_hash = Column(Text, nullable=False)
    input_hash = Column(Text, nullable=False)

    status = Column(Text, nullable=False, default=LLM_EVALUATION_PENDING, server_default=sql_text("'pending'"))
    llm_score = Column(Numeric(5, 2), nullable=True)
    confidence = Column(Numeric(5, 4), nullable=True)
    verdict = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)
    reason_codes = Column(JSONB, nullable=False, default=list, server_default=sql_text("'[]'"))
    requirement_verdicts = Column(JSONB, nullable=False, default=list, server_default=sql_text("'[]'"))
    analysis = Column(JSONB, nullable=False, default=dict, server_default=sql_text("'{}'"))
    error_code = Column(Text, nullable=True)
    retryable = Column(Boolean, nullable=False, default=False, server_default=sql_text("FALSE"))

    started_at = Column(TIMESTAMP(timezone=True), nullable=True)
    completed_at = Column(TIMESTAMP(timezone=True), nullable=True)
    deleted_at = Column(TIMESTAMP(timezone=True), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW, onupdate=UTC_NOW)

    __table_args__ = (
        Index("idx_llm_eval_owner_status_created", "owner_id", "status", "created_at"),
        Index("idx_llm_eval_owner_resume_created", "owner_id", "resume_fingerprint", "created_at"),
        Index("idx_llm_eval_owner_match_created", "owner_id", "job_match_id", "created_at"),
        Index("idx_llm_eval_job_match", "job_match_id"),
        Index("idx_llm_eval_job_post", "job_post_id"),
        Index(
            "uq_llm_eval_active_tenant_cache",
            "owner_id",
            "tenant_id",
            "resume_fingerprint",
            "job_post_id",
            "judge_config_hash",
            "evidence_hash",
            unique=True,
            postgresql_where=sql_text("deleted_at IS NULL AND tenant_id IS NOT NULL"),
        ),
        Index(
            "uq_llm_eval_active_global_cache",
            "owner_id",
            "resume_fingerprint",
            "job_post_id",
            "judge_config_hash",
            "evidence_hash",
            unique=True,
            postgresql_where=sql_text("deleted_at IS NULL AND tenant_id IS NULL"),
        ),
    )
