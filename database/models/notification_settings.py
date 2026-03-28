import uuid

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    Text,
    TIMESTAMP,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text as sql_text

from .base import Base

UTC_NOW = sql_text("timezone('UTC', now())")


class UserNotificationSettings(Base):
    """Per-user notification preferences."""

    __tablename__ = "user_notification_settings"

    owner_id = Column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    notifications_enabled = Column(Boolean, nullable=False, default=True, server_default=sql_text("TRUE"))
    min_score_threshold = Column(Integer, nullable=False, default=70, server_default=sql_text("70"))
    notify_on_new_match = Column(Boolean, nullable=False, default=True, server_default=sql_text("TRUE"))
    notify_on_batch_complete = Column(Boolean, nullable=False, default=True, server_default=sql_text("TRUE"))
    revision = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=UTC_NOW,
        onupdate=sql_text("timezone('UTC', now())"),
    )

    channels = relationship(
        "UserNotificationChannel",
        back_populates="settings",
        cascade="all, delete-orphan",
        primaryjoin="UserNotificationSettings.owner_id == foreign(UserNotificationChannel.owner_id)",
    )


class UserNotificationChannel(Base):
    """Per-user notification channel configuration."""

    __tablename__ = "user_notification_channel"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(
        UUID(as_uuid=True),
        ForeignKey("user_notification_settings.owner_id", ondelete="CASCADE"),
        nullable=False,
    )
    channel_type = Column(Text, nullable=False)
    enabled = Column(Boolean, nullable=False, default=False, server_default=sql_text("FALSE"))
    configured = Column(Boolean, nullable=False, default=False, server_default=sql_text("FALSE"))
    masked_recipient = Column(Text, nullable=True)
    secret_ciphertext = Column(Text, nullable=True)
    secret_key_version = Column(Text, nullable=True)
    config_json = Column(JSONB, nullable=False, default=dict, server_default=sql_text("'{}'"))
    last_test_status = Column(Text, nullable=True)
    last_tested_at = Column(TIMESTAMP(timezone=True), nullable=True)
    last_test_error = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=UTC_NOW)
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=UTC_NOW,
        onupdate=sql_text("timezone('UTC', now())"),
    )

    settings = relationship(
        "UserNotificationSettings",
        back_populates="channels",
        primaryjoin="foreign(UserNotificationChannel.owner_id) == UserNotificationSettings.owner_id",
    )

    __table_args__ = (
        UniqueConstraint("owner_id", "channel_type", name="uq_user_notification_channel_owner_type"),
        Index("idx_user_notification_channel_owner", "owner_id"),
        Index("idx_user_notification_channel_last_tested", "last_tested_at"),
    )
