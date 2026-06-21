import uuid

from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, Text, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text as sql_text

from .base import Base

UTC_NOW = sql_text("timezone('UTC', now())")

PIPELINE_RUN_PENDING = "pending"
PIPELINE_RUN_RUNNING = "running"
PIPELINE_RUN_COMPLETED = "completed"
PIPELINE_RUN_FAILED = "failed"
PIPELINE_RUN_CANCELLED = "cancelled"
PIPELINE_RUN_TERMINAL_STATUSES = frozenset({
    PIPELINE_RUN_COMPLETED,
    PIPELINE_RUN_FAILED,
    PIPELINE_RUN_CANCELLED,
})
PIPELINE_RUN_CANONICAL_STAGES = frozenset({
    "scrape",
    "extraction",
    "embedding",
    "matching",
    "repair",
    "resume_extraction",
    "resume_embedding",
})


class PipelineRun(Base):
    """Durable source of truth for user-visible pipeline progress."""

    __tablename__ = "pipeline_run"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(Text, nullable=False, unique=True)
    run_type = Column(Text, nullable=False)
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    tenant_id = Column(UUID(as_uuid=True), ForeignKey("tenant.id", ondelete="CASCADE"), nullable=True)
    resume_fingerprint = Column(Text, nullable=True)

    status = Column(Text, nullable=False, default=PIPELINE_RUN_PENDING, server_default=sql_text("'pending'"))
    current_stage = Column(Text, nullable=True)
    queued_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    processed_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    succeeded_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    failed_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    skipped_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    retry_eligible = Column(Boolean, nullable=False, default=False, server_default=sql_text("FALSE"))
    last_error = Column(Text, nullable=True)
    metadata_json = Column("metadata", JSONB, nullable=False, default=dict, server_default=sql_text("'{}'"))

    started_at = Column(TIMESTAMP(timezone=True), nullable=True)
    completed_at = Column(TIMESTAMP(timezone=True), nullable=True)
    heartbeat_at = Column(TIMESTAMP(timezone=True), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW, onupdate=UTC_NOW)

    stages = relationship(
        "PipelineRunStage",
        back_populates="run",
        cascade="all, delete-orphan",
        order_by="PipelineRunStage.created_at",
    )

    __table_args__ = (
        Index("idx_pipeline_run_task_id", "task_id"),
        Index("idx_pipeline_run_status_stage", "status", "current_stage"),
        Index("idx_pipeline_run_owner_tenant", "owner_id", "tenant_id"),
        Index("idx_pipeline_run_created_at", "created_at"),
        Index("idx_pipeline_run_heartbeat", "heartbeat_at"),
    )


class PipelineRunStage(Base):
    """Durable per-stage progress for a PipelineRun."""

    __tablename__ = "pipeline_run_stage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_run.id", ondelete="CASCADE"), nullable=False)
    stage = Column(Text, nullable=False)
    status = Column(Text, nullable=False, default=PIPELINE_RUN_PENDING, server_default=sql_text("'pending'"))
    queued_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    processed_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    succeeded_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    failed_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    skipped_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    retry_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    retry_eligible = Column(Boolean, nullable=False, default=False, server_default=sql_text("FALSE"))
    last_error = Column(Text, nullable=True)
    metadata_json = Column("metadata", JSONB, nullable=False, default=dict, server_default=sql_text("'{}'"))

    started_at = Column(TIMESTAMP(timezone=True), nullable=True)
    completed_at = Column(TIMESTAMP(timezone=True), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW, onupdate=UTC_NOW)

    run = relationship("PipelineRun", back_populates="stages")

    __table_args__ = (
        Index("idx_pipeline_run_stage_run_stage", "run_id", "stage"),
        Index("idx_pipeline_run_stage_status", "status", "stage"),
        Index("idx_pipeline_run_stage_created_at", "created_at"),
    )
