import uuid

from sqlalchemy import Column, Text, TIMESTAMP, ForeignKey, Boolean, Integer, UniqueConstraint, Index
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from .base import Base

class NotificationTracker(Base):
    """
    Tracks sent notifications for deduplication.
    
    Prevents notification fatigue by ensuring the same event
    (e.g., job match) is not repeatedly notified to the user.
    """
    __tablename__ = 'notification_tracker'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # What was notified
    user_id = Column(Text, nullable=False, index=True)
    job_match_id = Column(UUID(as_uuid=True), ForeignKey('job_match.id', ondelete='CASCADE'), nullable=True)
    notification_type = Column(Text, nullable=False)  # email, discord, telegram, etc.
    channel_type = Column(Text, nullable=False)  # email, discord, telegram, slack, etc.
    
    # Deduplication key - hash of user + job + event type
    dedup_hash = Column(Text, nullable=False, index=True, unique=True)
    
    # Notification content hash (to detect content changes)
    content_hash = Column(Text, nullable=True)
    
    # Event that triggered notification
    event_type = Column(Text, nullable=False)  # new_match, score_improved, batch_complete, etc.
    event_data = Column(JSONB, default={})  # Additional event context
    
    # Notification metadata
    recipient = Column(Text, nullable=False)  # email, discord webhook, telegram chat id
    subject = Column(Text)
    sent_successfully = Column(Boolean, default=False)
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    first_sent_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    last_sent_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
    send_count = Column(Integer, default=1)  # How many times this was sent (for resends)
    
    # Resend policy
    allow_resend = Column(Boolean, default=False)  # Whether to allow resending
    resend_interval_hours = Column(Integer, default=24)  # Minimum hours between resends
    
    # Relationships
    job_match = relationship("JobMatch", backref="notifications")
    
    __table_args__ = (
        # Unique constraint on dedup hash
        UniqueConstraint('dedup_hash', name='uq_notification_dedup'),
        # Index for querying user's notifications
        Index('idx_notification_user', 'user_id', 'first_sent_at'),
        # Index for checking recent notifications
        Index('idx_notification_recent', 'dedup_hash', 'last_sent_at'),
    )
