import uuid

from sqlalchemy import Column, Text, TIMESTAMP, ForeignKey, Boolean, Integer, UniqueConstraint, Index
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from .base import Base


class NotificationTracker(Base):
    """Tracks sent notifications for deduplication."""

    __tablename__ = 'notification_tracker'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, index=True)
    job_match_id = Column(UUID(as_uuid=True), ForeignKey('job_match.id', ondelete='CASCADE'), nullable=True, index=True)
    channel_type = Column(Text, nullable=False)
    dedup_hash = Column(Text, nullable=False, index=True)
    content_hash = Column(Text, nullable=True)
    event_type = Column(Text, nullable=False)
    event_data = Column(JSONB, default=dict, server_default=sql_text("'{}'"))
    recipient = Column(Text, nullable=False)
    subject = Column(Text)
    body = Column(Text, nullable=True)
    sent_successfully = Column(Boolean, default=False)
    error_message = Column(Text, nullable=True)
    first_sent_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    last_sent_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    send_count = Column(Integer, default=1)
    allow_resend = Column(Boolean, default=True)
    resend_interval_hours = Column(Integer, default=24)

    job_match = relationship("JobMatch", backref="notifications")

    __table_args__ = (
        UniqueConstraint('dedup_hash', name='uq_notification_dedup'),
        Index('idx_notification_owner', 'owner_id', 'first_sent_at'),
        Index('idx_notification_recent', 'dedup_hash', 'last_sent_at'),
    )

    def __init__(self, **kwargs):
        if "user_id" in kwargs and "owner_id" not in kwargs:
            kwargs["owner_id"] = kwargs.pop("user_id")
        super().__init__(**kwargs)

    @property
    def user_id(self):
        return self.owner_id

    @user_id.setter
    def user_id(self, value):
        self.owner_id = value
