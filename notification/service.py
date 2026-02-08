#!/usr/bin/env python3
"""
Notification Service with Deduplication - SOLID Implementation

Main service that orchestrates notifications using:
- NotificationChannel implementations (SOLID: Dependency Inversion)
- NotificationTracker for deduplication (SOLID: Single Responsibility)
- Redis Queue for async processing

Usage:
    from notification.service import NotificationService
    
    service = NotificationService()
    
    # This checks deduplication, sends via queue, and tracks
    service.send_notification(
        channel_type="discord",
        recipient="webhook-url",
        subject="New Match!",
        body="Details...",
        user_id="user123",
        job_match_id="match456",
        event_type="new_high_score_match"
    )
"""

import os
import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from database.models import JobPost
from enum import Enum
from urllib.parse import urljoin

# RQ imports
try:
    from redis import Redis
    from rq import Queue, Retry
    from rq.job import Job
    RQ_AVAILABLE = True
except ImportError:
    RQ_AVAILABLE = False

from database.database import db_session_scope
from database.repository import JobRepository
from notification.channels import NotificationChannelFactory
from notification.tracker import NotificationTrackerService, NotificationEvent
from notification.message_builder import NotificationMessageBuilder

logger = logging.getLogger(__name__)


class NotificationPriority(Enum):
    """Priority levels for notifications."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class NotificationService:
    """
    Main notification service with deduplication.
    
    This service coordinates:
    1. Deduplication checking (via NotificationTracker)
    2. Channel selection (via NotificationChannelFactory)
    3. Queueing for async processing (via RQ)
    4. Recording sent notifications
    """
    
    def __init__(
        self,
        repo: JobRepository,
        redis_url: Optional[str] = None,
        skip_dedup: bool = False,
        base_url: Optional[str] = None,
        use_async_queue: bool = True,
        priority_high: int = 80,
        priority_normal: int = 60
    ):
        """
        Initialize notification service.

        Args:
            repo: Repository for database operations
            redis_url: Redis connection URL
            skip_dedup: If True, disable deduplication (for testing)
            base_url: Base URL for links in notifications (injected from config)
            use_async_queue: Whether to use async queue or sync mode
            priority_high: Score threshold for HIGH priority (default: 80)
            priority_normal: Score threshold for NORMAL priority (default: 60)
        """
        self.repo = repo
        self.skip_dedup = skip_dedup
        self._priority_high = priority_high
        self._priority_normal = priority_normal

        # Initialize deduplication tracker
        self.tracker = NotificationTrackerService(repo)

        # Initialize Redis Queue
        self.redis_url = redis_url or os.environ.get(
            'REDIS_URL',
            'redis://localhost:6379/0'
        )

        # Base URL injected from config (no direct config.yaml read)
        # Falls back to environment variable or default
        self.base_url = base_url or os.environ.get('BASE_URL', 'http://localhost:8080')

        # Store the preferred mode from config
        self._use_async_queue = use_async_queue
        
        if not use_async_queue:
            # Explicitly disabled via config - force sync mode
            logger.info("Async queue disabled via config. Using sync mode.")
            self.redis_conn = None
            self.queue = None
            self.async_mode = False
        elif RQ_AVAILABLE:
            try:
                self.redis_conn = Redis.from_url(self.redis_url)
                # Validate connection with ping before using
                self.redis_conn.ping()
                self.queue = Queue('notifications', connection=self.redis_conn)
                self.async_mode = True
                logger.info("Notification service connected to Redis")
            except Exception as e:
                logger.error(f"Redis connection failed: {e}. Falling back to sync mode.")
                self.redis_conn = None
                self.queue = None
                self.async_mode = False
        else:
            logger.warning("RQ not available. Using sync mode.")
            self.async_mode = False
    
    def send_notification(
        self,
        channel_type: str,
        recipient: str,
        subject: str,
        body: str,
        user_id: str,
        job_match_id: Optional[str] = None,
        event_type: str = "general",
        priority: NotificationPriority = NotificationPriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        allow_resend: bool = True
    ) -> Optional[str]:
        """
        Send a notification with deduplication check.
        
        Args:
            channel_type: Type of channel (email, discord, telegram, slack, etc.)
            recipient: Recipient (email, webhook URL, chat ID, etc.)
            subject: Notification subject
            body: Notification body
            user_id: User ID for deduplication
            job_match_id: Job match ID for deduplication (optional)
            event_type: Event type for deduplication
            priority: Priority level
            metadata: Additional metadata
            allow_resend: Whether to allow future resends
        
        Returns:
            Notification ID if sent/queued, None if suppressed as duplicate
        """
        # Check deduplication
        if not self.skip_dedup:
            should_send = self.tracker.should_send_notification(
                user_id=user_id,
                job_match_id=job_match_id,
                event_type=event_type,
                channel_type=channel_type,
                subject=subject,
                body=body,
                metadata=metadata
            )
            
            if not should_send:
                logger.info(f"Suppressing duplicate notification: {event_type} for {user_id}")
                return None
        
        # Build notification data
        notification_data = {
            'channel_type': channel_type,
            'recipient': recipient,
            'subject': subject,
            'body': body,
            'metadata': metadata or {},
            'user_id': user_id,
            'job_match_id': job_match_id,
            'event_type': event_type,
            'priority': priority.value,
            'allow_resend': allow_resend
        }
        
        # Queue or process immediately
        if self.async_mode:
            # Add retry policy for transient failures
            retry_policy = Retry(max=3, interval=[30, 60, 120])  # Retry 3 times with increasing delays
            job = self.queue.enqueue(
                process_notification_task,
                notification_data,
                job_timeout='5m',
                result_ttl=86400,
                retry=retry_policy
            )
            notification_id = job.id
            logger.info(f"Queued notification as job {job.id}")
        else:
            # Process synchronously
            notification_id = process_notification_task(notification_data)
        
        return notification_id
    
    def notify_new_match(
        self,
        user_id: str,
        match_id: str,
        job_title: str,
        company: str,
        score: float,
        location: Optional[str] = None,
        is_remote: bool = False,
        channels: Optional[list] = None,
        job_post: Optional["JobPost"] = None,
        match_data: Optional[Dict[str, Any]] = None,
        apply_url: Optional[str] = None
    ) -> Dict[str, Optional[str]]:
        """
        Send notifications about a new job match to multiple channels.

        Args:
            user_id: User to notify
            match_id: Match ID
            job_title: Job title
            company: Company name
            score: Match score
            location: Job location
            is_remote: Whether remote
            channels: List of channels to notify (default: ['email'])
            job_post: Optional JobPost object with full job details
            match_data: Optional dict with match details (fit_score, want_score, coverage, etc.)
            apply_url: Optional direct apply URL

        Returns:
            Dict mapping channel names to notification IDs
        """
        if channels is None:
            channels = ['email']

        if score >= self._priority_high:
            priority = NotificationPriority.HIGH
        elif score >= self._priority_normal:
            priority = NotificationPriority.NORMAL
        else:
            priority = NotificationPriority.LOW

        subject = f"🎯 {job_title} at {company}"

        # Build job contents for rich notifications
        job_contents = []
        if job_post:
            job_contents = [NotificationMessageBuilder.build_notification_content(
                job_post=job_post,
                overall_score=score,
                fit_score=match_data.get('fit_score', score) if match_data else score,
                want_score=match_data.get('want_score') if match_data else None,
                required_coverage=match_data.get('required_coverage', 0) if match_data else 0,
                apply_url=apply_url
            )]

        results = {}

        for channel in channels:
            try:
                recipient = self._get_recipient_for_channel(user_id, channel)

                metadata = {
                    'job_title': job_title,
                    'company': company,
                    'score': score,
                    'is_remote': is_remote,
                    'location': location,
                    'job_contents': job_contents,
                    'match_id': match_id,
                }

                notification_id = self.send_notification(
                    channel_type=channel,
                    recipient=recipient,
                    subject=subject,
                    body="",  # Rich content is in metadata
                    user_id=user_id,
                    job_match_id=match_id,
                    event_type="new_high_score_match" if score >= self._priority_normal else "new_match",
                    priority=priority,
                    metadata=metadata
                )

                results[channel] = notification_id

            except Exception as e:
                logger.error(f"Failed to send {channel} notification: {e}")
                results[channel] = None

        return results
    
    def notify_batch_complete(
        self,
        user_id: str,
        total_matches: int,
        high_score_matches: int,
        channels: Optional[list] = None
    ) -> Dict[str, Optional[str]]:
        """Send batch completion notification."""
        if channels is None:
            channels = ['email']
        
        subject = f"✅ Job matching complete: {high_score_matches} great matches found"
        body = f"""Your job matching batch is complete!

Results Summary:
- Total matches analyzed: {total_matches}
- High-quality matches (70+ score): {high_score_matches}

View all your matches at: {self.base_url}

---
JobScout
"""
        
        results = {}
        
        for channel in channels:
            try:
                recipient = self._get_recipient_for_channel(user_id, channel)
                
                notification_id = self.send_notification(
                    channel_type=channel,
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    user_id=user_id,
                    job_match_id=None,
                    event_type="batch_complete",
                    priority=NotificationPriority.NORMAL,
                    metadata={
                        'total_matches': total_matches,
                        'high_score_matches': high_score_matches
                    },
                    allow_resend=True  # Allow daily batch notifications
                )
                
                results[channel] = notification_id
                
            except Exception as e:
                logger.error(f"Failed to send {channel} notification: {e}")
                results[channel] = None
        
        return results
    
    def _get_recipient_for_channel(self, user_id: str, channel: str) -> str:
        """
        Get recipient address for a given notification channel.
        
        Args:
            user_id: The user ID to look up preferences for
            channel: The notification channel type (email, discord, telegram)
            
        Returns:
            The recipient address/URL/ID for the specified channel
            
        Raises:
            ValueError: If the channel type is not supported
        """
        if channel == 'email':
            return os.environ.get('NOTIFICATION_EMAIL', 'user@example.com')
        elif channel == 'discord':
            return os.environ.get('DISCORD_WEBHOOK_URL', '')
        elif channel == 'telegram':
            return os.environ.get('TELEGRAM_CHAT_ID', '')
        else:
            raise ValueError(f"Unsupported channel type: {channel}")

    def get_queue_status(self) -> Dict[str, Any]:
        """Get queue status."""
        if not self.async_mode:
            return {'status': 'sync_mode', 'queue_length': 0}
        
        try:
            return {
                'status': 'active',
                'queue_length': len(self.queue),
                'redis_connected': self.redis_conn.ping()
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}


# Worker task - must be at module level for RQ
def process_notification_task(notification_data: Dict[str, Any]) -> str:
    """
    Process a notification (called by RQ worker).

    This function:
    1. Gets the appropriate channel
    2. Sends the notification
    3. Records the result in the tracker
    """
    notification_id = str(uuid.uuid4())
    
    channel_type = notification_data['channel_type']
    recipient = notification_data['recipient']
    subject = notification_data['subject']
    body = notification_data['body']
    metadata = notification_data.get('metadata', {})
    
    user_id = notification_data['user_id']
    job_match_id = notification_data.get('job_match_id')
    event_type = notification_data['event_type']
    allow_resend = notification_data.get('allow_resend', True)
    
    logger.info(f"Processing notification {notification_id} via {channel_type}")
    
    try:
        # Get channel implementation
        channel = NotificationChannelFactory.get_channel(channel_type)
        
        # Send notification
        success = channel.send(recipient, subject, body, metadata)
        
        # Record in database
        with db_session_scope() as session:
            repo = JobRepository(session)
            tracker = NotificationTrackerService(repo)
            
            tracker.record_notification(
                user_id=user_id,
                job_match_id=job_match_id,
                event_type=event_type,
                channel_type=channel_type,
                notification_type=event_type,
                recipient=recipient,
                subject=subject,
                body=body,
                success=success,
                error_message=None if success else "Send failed",
                metadata=metadata,
                allow_resend=allow_resend
            )
        
        if success:
            logger.info(f"Notification {notification_id} sent successfully")
        else:
            logger.error(f"Notification {notification_id} failed to send")
        
        return notification_id

    except Exception as e:
        logger.error(f"Failed to process notification {notification_id}: {e}", exc_info=True)

        # Record failure
        try:
            with db_session_scope() as session:
                repo = JobRepository(session)
                tracker = NotificationTrackerService(repo)

                tracker.record_notification(
                    user_id=user_id,
                    job_match_id=job_match_id,
                    event_type=event_type,
                    channel_type=channel_type,
                    notification_type=event_type,
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    success=False,
                    error_message=str(e),
                    metadata=metadata,
                    allow_resend=allow_resend
                )
        except Exception as record_error:
            logger.error(f"Failed to record notification failure: {record_error}", exc_info=True)

        raise
