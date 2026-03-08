#!/usr/bin/env python3
"""
Notification Tracker - Deduplication Service
Implements deduplication for notifications to prevent notification fatigue.
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod

from sqlalchemy import select, delete, desc
from database.repository import JobRepository
from database.models import NotificationTracker

# Constants
RESEND_INTERVAL_NEVER = 999999  # Effectively never

logger = logging.getLogger(__name__)


@dataclass
class NotificationEvent:
    """Represents a notification event for tracking."""
    user_id: str
    job_match_id: Optional[str]
    event_type: str  # e.g., "new_match", "score_improved", "batch_complete"
    channel_type: str  # e.g., "email", "discord", "telegram"
    content_hash: Optional[str] = None  # Hash of notification content
    metadata: Optional[Dict[str, Any]] = None


class DeduplicationStrategy(ABC):
    """
    Abstract strategy for deduplication logic.
    
    Allows different deduplication policies to be implemented
    without changing the core tracker code.
    """
    
    @abstractmethod
    def should_allow_notification(
        self,
        existing_notification: Optional[NotificationTracker],
        new_event: NotificationEvent
    ) -> bool:
        """
        Determine if notification should be allowed.
        
        Args:
            existing_notification: Previous notification record (if any)
            new_event: New notification event
        
        Returns:
            True if notification should be sent, False otherwise
        """
        pass
    
    @abstractmethod
    def get_resend_interval(self) -> int:
        """Get minimum hours between resends."""
        pass


class DefaultDeduplicationStrategy(DeduplicationStrategy):
    """
    Default deduplication strategy.
    
    - Never resend the exact same notification
    - Allow resend if content changes significantly
    - Allow resend after 24 hours for certain event types
    """
    
    # Event types that can be resent after interval
    RESENDABLE_EVENTS = {'score_improved', 'status_changed'}
    
    def __init__(self, default_interval_hours: int = 24):
        self.default_interval_hours = default_interval_hours
    
    def should_allow_notification(
        self,
        existing_notification: Optional[NotificationTracker],
        new_event: NotificationEvent
    ) -> bool:
        """Apply deduplication logic."""
        if not existing_notification:
            return True  # Never sent before
        
        # Check if content changed
        if new_event.content_hash and existing_notification.content_hash:
            if new_event.content_hash != existing_notification.content_hash:
                logger.info("Content changed, allowing resend")
                return True
        
        # Check if this event type allows resending
        if new_event.event_type not in self.RESENDABLE_EVENTS:
            logger.info(f"Event type {new_event.event_type} does not allow resends")
            return False
        
        # Check resend interval
        if not existing_notification.allow_resend:
            return False
        
        min_interval = timedelta(hours=existing_notification.resend_interval_hours or self.default_interval_hours)
        time_since_last = datetime.now(timezone.utc) - existing_notification.last_sent_at
        
        if time_since_last < min_interval:
            logger.info(f"Too soon to resend (sent {time_since_last} ago)")
            return False
        
        return True
    
    def get_resend_interval(self) -> int:
        return self.default_interval_hours


class AggressiveDeduplicationStrategy(DeduplicationStrategy):
    """Aggressive deduplication - never resend, only notify once per event."""
    
    def should_allow_notification(
        self,
        existing_notification: Optional[NotificationTracker],
        new_event: NotificationEvent
    ) -> bool:
        return existing_notification is None
    
    def get_resend_interval(self) -> int:
        return RESEND_INTERVAL_NEVER


class NotificationTrackerService:
    """
    Service for tracking and deduplicating notifications.
    
    Implements:
    - Single Responsibility: Only handles notification tracking
    - Dependency Inversion: Depends on DeduplicationStrategy abstraction
    """
    
    def __init__(
        self,
        repo: JobRepository,
        strategy: Optional[DeduplicationStrategy] = None
    ):
        """
        Initialize tracker.
        
        Args:
            repo: Repository for database operations
            strategy: Deduplication strategy (defaults to DefaultDeduplicationStrategy)
        """
        self.repo = repo
        self.strategy = strategy or DefaultDeduplicationStrategy()
    
    def generate_dedup_hash(
        self,
        user_id: str,
        job_match_id: Optional[str],
        event_type: str,
        channel_type: str
    ) -> str:
        """
        Generate deduplication hash for an event.
        
        This hash uniquely identifies a notification event to prevent duplicates.
        """
        key = f"{user_id}:{job_match_id}:{event_type}:{channel_type}"
        return hashlib.sha256(key.encode('utf-8')).hexdigest()[:32]
    
    def generate_content_hash(
        self,
        subject: str,
        body: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate hash of notification content."""
        content = {
            'subject': subject,
            'body': body[:500],  # First 500 chars to avoid huge hashes
            'metadata': json.dumps(metadata, sort_keys=True, default=str) if metadata else None
        }
        normalized = json.dumps(content, sort_keys=True)
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]
    
    def should_send_notification(
        self,
        user_id: str,
        job_match_id: Optional[str],
        event_type: str,
        channel_type: str,
        subject: str = "",
        body: str = "",
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Check if a notification should be sent (not a duplicate).
        
        Args:
            user_id: User to notify
            job_match_id: Related job match (if applicable)
            event_type: Type of event (e.g., "new_match")
            channel_type: Notification channel (e.g., "email")
            subject: Notification subject
            body: Notification body
            metadata: Additional metadata
        
        Returns:
            True if notification should be sent, False if duplicate
        """
        dedup_hash = self.generate_dedup_hash(user_id, job_match_id, event_type, channel_type)
        
        # Check for existing notification
        existing = self._get_existing_notification(dedup_hash)
        
        if not existing:
            logger.info(f"No previous notification found for hash {dedup_hash}")
            return True
        
        # Build event object
        content_hash = self.generate_content_hash(subject, body, metadata)
        event = NotificationEvent(
            user_id=user_id,
            job_match_id=job_match_id,
            event_type=event_type,
            channel_type=channel_type,
            content_hash=content_hash,
            metadata=metadata
        )
        
        # Apply deduplication strategy
        should_send = self.strategy.should_allow_notification(existing, event)
        
        if not should_send:
            logger.info(f"Suppressing duplicate notification: {event_type} for {user_id} via {channel_type}")
        
        return should_send
    
    def _get_existing_notification(
        self,
        dedup_hash: str
    ) -> Optional[NotificationTracker]:
        """Look up existing notification by dedup hash."""
        
        stmt = select(NotificationTracker).where(
            NotificationTracker.dedup_hash == dedup_hash
        ).order_by(NotificationTracker.last_sent_at.desc()).limit(1)
        
        result = self.repo.db.execute(stmt).scalar_one_or_none()
        return result
    
    def record_notification(
        self,
        user_id: str, # User who received notification
        job_match_id: Optional[str], # Related job match (optional)
        event_type: str, # Event that triggered notification
        channel_type: str, # Channel used (email, discord, etc.)
        recipient: str, # Actual recipient address/ID
        subject: str, # Notification subject
        body: str, # Notification body
        success: bool, # Whether sending succeeded
        error_message: Optional[str] = None, # Error if failed
        metadata: Optional[Dict[str, Any]] = None, # Additional context
        allow_resend: bool = True, # Whether to allow future resends
        commit: bool = True
    ) -> NotificationTracker:
        dedup_hash = self.generate_dedup_hash(user_id, job_match_id, event_type, channel_type)
        content_hash = self.generate_content_hash(subject, body, metadata)
        
        # Check if record exists
        existing = self._get_existing_notification(dedup_hash)
        
        if existing:
            # Update existing record
            existing.last_sent_at = datetime.now(timezone.utc)
            existing.send_count += 1
            existing.content_hash = content_hash
            existing.sent_successfully = success
            existing.error_message = error_message
            
            tracker = existing
            logger.info(f"Updated notification record (send count: {tracker.send_count})")
        else:
            # Create new record
            tracker = NotificationTracker(
                user_id=user_id,
                job_match_id=job_match_id,
                channel_type=channel_type,
                dedup_hash=dedup_hash,
                content_hash=content_hash,
                event_type=event_type,
                event_data=metadata or {},
                recipient=recipient,
                subject=subject,
                body=body,
                sent_successfully=success,
                error_message=error_message,
                allow_resend=allow_resend,
                resend_interval_hours=self.strategy.get_resend_interval()
            )
            
            self.repo.db.add(tracker)
            logger.info(f"Created new notification record for {event_type}")
        
        if commit:
            self.repo.db.commit()
        return tracker


# Convenience function for quick dedup check
def should_notify_user(
    repo: JobRepository,
    user_id: str,
    job_match_id: str,
    event_type: str = "new_match",
    channel: str = "email",
    tracker: Optional[NotificationTrackerService] = None
) -> bool:
    """
    Quick check if user should be notified about a job match.
    """
    tracker_service = tracker or NotificationTrackerService(repo)
    return tracker_service.should_send_notification(
        user_id=user_id,
        job_match_id=job_match_id,
        event_type=event_type,
        channel_type=channel
    )
