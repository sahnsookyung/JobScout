import uuid

from sqlalchemy import Boolean, Column, Index, Text, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import text as sql_text

from .base import Base

UTC_NOW = sql_text("timezone('UTC', now())")


class UserFeatureEntitlement(Base):
    """Per-user feature gating and configuration overrides."""

    __tablename__ = "user_feature_entitlement"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    feature_key = Column(Text, nullable=False)
    enabled = Column(Boolean, nullable=False, default=True, server_default=sql_text("true"))
    value_json = Column(JSONB, nullable=True)
    source = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=UTC_NOW,
        server_onupdate=UTC_NOW,
    )

    __table_args__ = (
        Index(
            "idx_user_feature_entitlement_owner_feature",
            "owner_id",
            "feature_key",
            unique=True,
        ),
        Index("idx_user_feature_entitlement_feature", "feature_key"),
    )
