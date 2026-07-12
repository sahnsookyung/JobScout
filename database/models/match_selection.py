import enum
import uuid

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    TIMESTAMP,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text as sql_text

from .base import Base


class SelectionTier(str, enum.Enum):
    PRIMARY = "primary"
    EXCLUDED = "excluded"


class ExcludedReason(str, enum.Enum):
    BELOW_MIN_FIT = "below_min_fit"
    BEYOND_TOP_K = "beyond_top_k"
    BELOW_COVERAGE_FLOOR = "below_coverage_floor"
    TRUNCATED = "truncated"

UTC_NOW = sql_text("timezone('UTC', now())")


class MatchSelectionRun(Base):
    __tablename__ = "match_selection_run"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    resume_fingerprint = Column(Text, nullable=False)
    task_id = Column(Text, nullable=True)
    lifecycle_status = Column(
        Text,
        nullable=False,
        default="pending",
        server_default=sql_text("'pending'"),
    )
    is_current = Column(Boolean, nullable=False, default=False, server_default=sql_text("FALSE"))
    policy_snapshot_json = Column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=sql_text("'{}'"),
    )
    ranking_mode_used = Column(Text, nullable=False)
    ranking_config_version = Column(Text, nullable=False)
    stable_tie_break_key = Column(Text, nullable=False)
    fit_floor_used = Column(Numeric(5, 2), nullable=False, default=0, server_default=sql_text("0"))
    notification_fit_floor_used = Column(
        Numeric(5, 2), nullable=False, default=0, server_default=sql_text("0")
    )
    top_k_used = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    candidate_pool_size = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    selected_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    alert_candidate_count = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    resume_resolution_reason = Column(
        Text,
        nullable=False,
        default="",
        server_default=sql_text("''"),
    )
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW, onupdate=UTC_NOW)

    items = relationship(
        "MatchSelectionItem",
        back_populates="selection_run",
        cascade="all, delete-orphan",
        order_by="MatchSelectionItem.rank_position",
    )

    __table_args__ = (
        Index("idx_match_selection_run_owner", "owner_id", "created_at"),
        Index("idx_match_selection_run_resume", "resume_fingerprint", "created_at"),
        Index(
            "idx_match_selection_run_current",
            "owner_id",
            "resume_fingerprint",
            unique=True,
            postgresql_where=sql_text("is_current AND lifecycle_status = 'committed'"),
        ),
    )


class MatchSelectionItem(Base):
    __tablename__ = "match_selection_item"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    selection_run_id = Column(
        UUID(as_uuid=True),
        ForeignKey("match_selection_run.id", ondelete="CASCADE"),
        nullable=False,
    )
    job_match_id = Column(
        UUID(as_uuid=True),
        ForeignKey("job_match.id", ondelete="CASCADE"),
        nullable=False,
    )
    rank_position = Column(Integer, nullable=False)
    fit_score_at_selection = Column(Numeric(5, 2), nullable=False, default=0, server_default=sql_text("0"))
    preference_score_at_selection = Column(Numeric(5, 2), nullable=True)
    job_similarity_at_selection = Column(Numeric(3, 2), nullable=False, default=0, server_default=sql_text("0"))
    required_coverage_at_selection = Column(
        Numeric(3, 2), nullable=False, default=0, server_default=sql_text("0")
    )
    alert_eligible = Column(Boolean, nullable=False, default=False, server_default=sql_text("FALSE"))
    dominant_reason_code = Column(Text, nullable=True)
    explanation_label = Column(Text, nullable=True)
    ranking_snapshot = Column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=sql_text("'{}'"),
    )
    selection_tier = Column(
        Text,
        nullable=False,
        default=SelectionTier.PRIMARY.value,
        server_default=sql_text("'primary'"),
    )
    excluded_reason = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)

    selection_run = relationship("MatchSelectionRun", back_populates="items")
    job_match = relationship("JobMatch")

    __table_args__ = (
        Index("idx_match_selection_item_run", "selection_run_id", "rank_position", unique=True),
        Index("idx_match_selection_item_match", "job_match_id"),
        Index("idx_msi_run_tier", "selection_run_id", "selection_tier"),
        Index("idx_msi_run_tier_rank_id", "selection_run_id", "selection_tier", "rank_position", "id"),
        CheckConstraint(
            "selection_tier IN ('primary', 'excluded')",
            name="msi_selection_tier_chk",
        ),
        CheckConstraint(
            "(selection_tier = 'primary' AND excluded_reason IS NULL) OR "
            "(selection_tier = 'excluded' AND excluded_reason IS NOT NULL)",
            name="msi_excluded_reason_chk",
        ),
    )
