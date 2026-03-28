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

import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from enum import Enum
from uuid import UUID

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
from notification.channels import RateLimitException
from notification.exceptions import (
    NotificationConfigurationError,
    TerminalNotificationError,
    TransientNotificationError,
)
from notification.tracker import NotificationTrackerService, NotificationEvent
from notification.message_builder import NotificationMessageBuilder, JobNotificationContent
from notification.user_settings import UserNotificationSettingsService

logger = logging.getLogger(__name__)

REDIS_URL_DEFAULT = 'redis://localhost:6379/0'
TRANSIENT_RETRY_INTERVALS = (30, 60, 120)


def _parse_uuid(value: Any) -> Optional[UUID]:
    """Return a UUID when the value is UUID-like, otherwise None."""
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _mark_settings_test_result(
    notification_data: Dict[str, Any],
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """Persist terminal status for settings test notifications."""
    if notification_data.get('event_type') != 'settings_test':
        return

    owner_id = _parse_uuid(notification_data.get('user_id'))
    channel_type = notification_data.get('channel_type')
    if owner_id is None or not channel_type:
        return

    try:
        with db_session_scope() as session:
            UserNotificationSettingsService(session).mark_test_result(
                owner_id=owner_id,
                channel_type=channel_type,
                status=status,
                error_message=error_message,
            )
    except Exception:
        logger.debug("Failed to persist settings test status", exc_info=True)


class NotificationRateLimiter:
    """
    Rate limiter using Redis to coordinate across workers.
    
    When any worker hits a rate limit, it stores the "wait until" timestamp.
    All other workers check this and wait before attempting to send.
    """
    
    RATE_LIMIT_PREFIX = "notification:rate_limit:"
    
    def __init__(self, redis_url: str = REDIS_URL_DEFAULT, max_wait_seconds: int = 300):
        self.redis_url = redis_url
        self.max_wait_seconds = max_wait_seconds
        self._redis = None
    
    def _get_redis(self) -> Optional[Redis]:
        """Get Redis connection (lazy init)."""
        if self._redis is None:
            try:
                self._redis = Redis.from_url(self.redis_url)
            except Exception:
                pass
        return self._redis
    
    def set_rate_limit(self, channel_type: str, retry_after: int) -> None:
        """Set rate limit wait time for a channel."""
        redis = self._get_redis()
        if redis:
            try:
                key = f"{self.RATE_LIMIT_PREFIX}{channel_type}"
                wait_until = time.time() + retry_after
                redis.setex(key, retry_after + 5, str(wait_until))  # Store +5s buffer
            except Exception:
                logger.debug("Failed to persist notification rate limit", exc_info=True)
    
    def get_wait_time(self, channel_type: str) -> float:
        """Get how long to wait before retrying (0 if no rate limit, capped at max_wait_seconds)."""
        redis = self._get_redis()
        if not redis:
            return 0
        
        key = f"{self.RATE_LIMIT_PREFIX}{channel_type}"
        try:
            wait_until = redis.get(key)
        except Exception:
            logger.debug("Failed reading notification rate limit state", exc_info=True)
            return 0
        
        if wait_until:
            try:
                # Redis returns bytes, convert to float
                wait_time = float(wait_until) - time.time()
                return min(max(0, wait_time), self.max_wait_seconds)
            except (ValueError, TypeError):
                return 0
        return 0
    
    def is_wait_exceeded(self, channel_type: str) -> bool:
        """Check if the required wait time exceeds max_wait_seconds (fail-fast condition)."""
        wait_time = self.get_wait_time(channel_type)
        return wait_time >= self.max_wait_seconds


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
        channel_configs: Optional[Dict[str, Any]] = None,
        priority_high: int = 80,
        priority_normal: int = 60,
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
        self.channel_configs = {
            name.lower(): config
            for name, config in (channel_configs or {}).items()
        }
        self.user_settings = UserNotificationSettingsService(self.repo.db)

        # Initialize deduplication tracker
        self.tracker = NotificationTrackerService(repo)

        # Initialize Redis Queue
        self.redis_url = redis_url or os.environ.get(
            'REDIS_URL',
            REDIS_URL_DEFAULT
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
        recipient: Optional[str],
        subject: str,
        body: str,
        user_id: str,
        job_match_id: Optional[str] = None,
        event_type: str = "general",
        priority: NotificationPriority = NotificationPriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        allow_resend: bool = True,
        skip_dedup: bool = False,
        resolve_user_settings: bool = False,
        require_enabled_delivery: bool = True,
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
        resolved_recipient = None
        if not resolve_user_settings:
            resolved_recipient = self._get_recipient_for_channel(
                channel_type,
                explicit_recipient=recipient,
            )
        metadata_payload = dict(metadata or {})
        metadata_payload.setdefault("user_id", user_id)

        # Check deduplication with fresh session (avoid stale long-lived session issues)
        if not self.skip_dedup and not skip_dedup:
            with db_session_scope() as session:
                fresh_repo = JobRepository(session)
                fresh_tracker = NotificationTrackerService(fresh_repo)
                should_send = fresh_tracker.should_send_notification(
                    user_id=user_id,
                    job_match_id=job_match_id,
                    event_type=event_type,
                    channel_type=channel_type,
                    subject=subject,
                    body=body,
                    metadata=metadata_payload
                )

            if not should_send:
                logger.info(f"Suppressing duplicate notification: {event_type} for {user_id}")
                return None
        
        # Build notification data
        notification_data = {
            'channel_type': channel_type,
            'recipient': resolved_recipient,
            'subject': subject,
            'body': body,
            'metadata': metadata_payload,
            'user_id': user_id,
            'job_match_id': job_match_id,
            'event_type': event_type,
            'priority': priority.value,
            'allow_resend': allow_resend,
            'resolve_user_settings': resolve_user_settings,
            'raise_transient': self.async_mode,
            'require_enabled_delivery': require_enabled_delivery,
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

    def get_user_notification_snapshot(self, user) -> Any:
        """Return the effective per-user notification settings snapshot."""
        return self.user_settings.get_settings_snapshot(user)

    def get_enabled_channels_for_user(self, user) -> list[str]:
        """Return channels that are enabled and deliverable for a user."""
        snapshot = self.user_settings.get_settings_snapshot(user)
        if not snapshot.notifications_enabled:
            return []
        return [
            name
            for name, channel in snapshot.channels.items()
            if channel.enabled and channel.available and channel.configured
        ]

    @staticmethod
    def _should_resolve_user_settings(user_id: str) -> bool:
        """Use per-user settings only for canonical UUID-backed users."""
        return _parse_uuid(user_id) is not None
    
    def notify_new_match(
        self,
        user_id: str,
        match_id: str,
        content: JobNotificationContent,
        channels: Optional[list] = None,
        task_id: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """
        Send notifications about a new job match to multiple channels.

        Args:
            user_id: User to notify
            match_id: Match ID
            content: JobNotificationContent with job details and match scores
            channels: List of channels to notify (default: ['email'])

        Returns:
            Dict mapping channel names to notification IDs
        """
        if channels is None:
            channels = ['email']

        score = content.match.overall_score

        if score >= self._priority_high:
            priority = NotificationPriority.HIGH
        elif score >= self._priority_normal:
            priority = NotificationPriority.NORMAL
        else:
            priority = NotificationPriority.LOW

        subject = f"🎯 {content.job.title} at {content.job.company}"

        results = {}

        for channel in channels:
            try:
                metadata = {
                    'job_title': content.job.title,
                    'company': content.job.company,
                    'score': score,
                    'is_remote': content.job.is_remote,
                    'location': content.job.location,
                    'job_contents': [content.model_dump()],  # Serialize to plain dict for safe JSON/RQ serialization
                    'match_id': match_id,
                    'user_id': user_id,
                }
                if task_id:
                    metadata['task_id'] = task_id

                notification_id = self.send_notification(
                    channel_type=channel,
                    recipient=None,
                    subject=subject,
                    body="",  # Rich content is in metadata
                    user_id=user_id,
                    job_match_id=match_id,
                    event_type="new_high_score_match" if score >= self._priority_normal else "new_match",
                    priority=priority,
                    metadata=metadata,
                    resolve_user_settings=self._should_resolve_user_settings(user_id),
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
        channels: Optional[list] = None,
        task_id: Optional[str] = None,
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
                notification_id = self.send_notification(
                    channel_type=channel,
                    recipient=None,
                    subject=subject,
                    body=body,
                    user_id=user_id,
                    job_match_id=None,
                    event_type="batch_complete",
                    priority=NotificationPriority.NORMAL,
                    metadata={
                        'total_matches': total_matches,
                        'high_score_matches': high_score_matches,
                        'task_id': task_id,
                        'user_id': user_id,
                    },
                    allow_resend=True,  # Allow daily batch notifications
                    resolve_user_settings=self._should_resolve_user_settings(user_id),
                )
                
                results[channel] = notification_id
                
            except Exception as e:
                logger.error(f"Failed to send {channel} notification: {e}")
                results[channel] = None
        
        return results
    
    def _get_recipient_for_channel(
        self,
        channel: str,
        explicit_recipient: Optional[str] = None,
    ) -> str:
        """
        Get recipient address for a given notification channel.
        
        Args:
            channel: The notification channel type (email, discord, telegram)
            
        Returns:
            The recipient address/URL/ID for the specified channel
            
        Raises:
            ValueError: If the channel type is not supported
        """
        if explicit_recipient:
            return explicit_recipient

        configured_recipient = self._configured_recipient_for_channel(channel)
        if configured_recipient:
            return configured_recipient

        env_recipient = self._env_recipient_for_channel(channel)
        if env_recipient:
            return env_recipient

        raise ValueError(f"No recipient configured for channel type: {channel}")

    def _configured_recipient_for_channel(self, channel: str) -> Optional[str]:
        config = self.channel_configs.get(channel.lower())
        if config is None:
            return None
        if isinstance(config, dict):
            return config.get("recipient")
        return getattr(config, "recipient", None)

    @staticmethod
    def _env_recipient_for_channel(channel: str) -> Optional[str]:
        if channel == 'email':
            return os.environ.get('NOTIFICATION_EMAIL') or os.environ.get('EMAIL')
        if channel == 'discord':
            return os.environ.get('DISCORD_WEBHOOK_URL')
        if channel == 'telegram':
            return os.environ.get('TELEGRAM_CHAT_ID')
        if channel == 'webhook':
            return os.environ.get('NOTIFICATION_WEBHOOK_URL')
        if channel == 'in_app':
            return None
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
    1. Checks global rate limiter (coordination across workers)
    2. Gets the appropriate channel
    3. Sends the notification
    4. Records the result in the tracker (only after max retries)
    5. Updates global rate limiter if rate limited
    """
    notification_id = str(uuid.uuid4())
    channel_type = notification_data['channel_type']
    
    # Read Redis URL from environment (not from notification data - keeps job payload clean)
    redis_url = os.environ.get('REDIS_URL', REDIS_URL_DEFAULT)
    max_wait_seconds = int(os.environ.get('NOTIFICATION_RATE_LIMIT_MAX_WAIT', '300'))
    
    logger.info(f"Processing notification {notification_id} via {channel_type}")
    
    # Create rate limiter (each worker gets its own instance, but they share Redis)
    rate_limiter = NotificationRateLimiter(redis_url, max_wait_seconds)
    
    # Max retries for rate limiting
    max_rate_limit_retries = 3
    rate_limit_retries = 0
    transient_retries = 0
    
    raise_transient = bool(notification_data.get('raise_transient', False))

    while True:
        # Check global rate limit before sending
        wait_time = rate_limiter.get_wait_time(channel_type)
        if wait_time > 0:
            logger.info(f"Global rate limit active for {channel_type}. Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
        
        try:
            return _send_and_record_notification(
                notification_id, notification_data
            )
        except RateLimitException as e:
            rate_limit_retries += 1
            retry_after = e.retry_after or 60
            actual_wait = min(retry_after, max_wait_seconds)
            
            # Update global rate limiter so all workers know to wait (capped)
            rate_limiter.set_rate_limit(channel_type, actual_wait)
            
            if rate_limit_retries > max_rate_limit_retries:
                logger.error(f"Max rate limit retries ({max_rate_limit_retries}) exceeded for notification {notification_id}")
                _record_notification_failure(
                    notification_id, notification_data,
                    f"Rate limit exceeded after {max_rate_limit_retries} retries"
                )
                return notification_id
            
            logger.warning(f"Rate limited by {channel_type}. Waiting {actual_wait}s before retry {rate_limit_retries}/{max_rate_limit_retries}")
            time.sleep(actual_wait)
            continue
        
        except Exception as e:
            if isinstance(e, TerminalNotificationError):
                logger.error(f"Failed to process notification {notification_id}: {e}", exc_info=True)
                _record_notification_failure(
                    notification_id,
                    notification_data,
                    str(e),
                    failure_class=e.failure_class,
                )
                return notification_id

            if isinstance(e, TransientNotificationError) and raise_transient:
                if transient_retries >= len(TRANSIENT_RETRY_INTERVALS):
                    logger.error(
                        "Max transient retries (%s) exceeded for notification %s",
                        len(TRANSIENT_RETRY_INTERVALS),
                        notification_id,
                    )
                    _record_notification_failure(
                        notification_id,
                        notification_data,
                        str(e),
                        failure_class=e.failure_class,
                    )
                    return notification_id

                wait_seconds = TRANSIENT_RETRY_INTERVALS[transient_retries]
                transient_retries += 1
                logger.warning(
                    "Transient notification failure for %s via %s: %s. Retrying in %ss (%s/%s)",
                    notification_id,
                    channel_type,
                    e,
                    wait_seconds,
                    transient_retries,
                    len(TRANSIENT_RETRY_INTERVALS),
                )
                time.sleep(wait_seconds)
                continue

            logger.error(f"Failed to process notification {notification_id}: {e}", exc_info=True)
            _record_notification_failure(
                notification_id,
                notification_data,
                str(e),
                failure_class=getattr(e, "failure_class", "unknown"),
            )
            return notification_id


def _send_and_record_notification(
    notification_id: str,
    notification_data: Dict[str, Any],
) -> str:
    """Send notification and record result in database."""
    channel_type = notification_data['channel_type']
    metadata = dict(notification_data.get('metadata', {}))
    recipient = notification_data.get('recipient')

    if notification_data.get('resolve_user_settings'):
        user_id = notification_data.get('user_id')
        if not user_id:
            raise NotificationConfigurationError(
                "Notification user_id is required for per-user delivery",
                failure_class="user_missing",
            )
        with db_session_scope() as session:
            target = UserNotificationSettingsService(session).resolve_delivery_target(
                owner_id=UUID(str(user_id)),
                channel_type=channel_type,
                require_enabled=bool(notification_data.get('require_enabled_delivery', True)),
            )
        recipient = target.recipient
        metadata['settings_revision'] = target.settings_revision
        metadata['resolved_recipient_masked'] = target.masked_recipient

    if not recipient:
        raise NotificationConfigurationError(
            f"No recipient resolved for channel '{channel_type}'",
            failure_class="recipient_missing",
        )

    channel = NotificationChannelFactory.get_channel(channel_type)
    success = channel.send(
        recipient,
        notification_data['subject'],
        notification_data['body'],
        metadata,
    )
    
    # Record in database
    with db_session_scope() as session:
        repo = JobRepository(session)
        tracker = NotificationTrackerService(repo)
        tracker.record_notification(
            user_id=notification_data['user_id'],
            job_match_id=notification_data.get('job_match_id'),
            event_type=notification_data['event_type'],
            channel_type=channel_type,
            recipient=recipient,
            subject=notification_data['subject'],
            body=notification_data['body'],
            success=success,
            error_message=None if success else "Send failed",
            metadata=metadata,
            allow_resend=notification_data.get('allow_resend', True)
        )
    
    if success:
        logger.info(f"Notification {notification_id} sent successfully")
    else:
        logger.error(f"Notification {notification_id} failed to send")

    if notification_data.get('event_type') == 'settings_test':
        _mark_settings_test_result(
            notification_data,
            status='sent' if success else 'failed',
            error_message=None if success else "Send failed",
        )
    
    return notification_id


def _record_notification_failure(
    notification_id: str,
    notification_data: Dict[str, Any],
    error_message: str,
    failure_class: str = "delivery_failed",
    ) -> None:
    """Record notification failure in database."""
    try:
        metadata = dict(notification_data.get('metadata', {}))
        metadata.setdefault("failure_class", failure_class)
        with db_session_scope() as session:
            repo = JobRepository(session)
            tracker = NotificationTrackerService(repo)
            tracker.record_notification(
                user_id=notification_data['user_id'],
                job_match_id=notification_data.get('job_match_id'),
                event_type=notification_data['event_type'],
                channel_type=notification_data['channel_type'],
                recipient=notification_data.get('recipient') or notification_data['channel_type'],
                subject=notification_data['subject'],
                body=notification_data['body'],
                success=False,
                error_message=error_message,
                metadata=metadata,
                allow_resend=notification_data.get('allow_resend', True)
            )
        _mark_settings_test_result(notification_data, status='failed', error_message=error_message)
    except Exception as db_error:
        logger.error(
            f"Failed to record failure for notification {notification_id}: {db_error}"
        )
