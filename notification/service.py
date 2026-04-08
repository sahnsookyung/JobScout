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
        event_type="new_match_alert"
    )
"""

import logging
import math
import time
import uuid
from datetime import timedelta
from typing import Any, Dict, Optional

from enum import Enum
from uuid import UUID

# RQ imports
try:
    from redis import Redis
    from rq import Queue, get_current_job
    from rq.registry import FailedJobRegistry
    RQ_AVAILABLE = True
except ImportError:
    RQ_AVAILABLE = False

from database.database import db_session_scope
from database.models import User
from database.repository import JobRepository
from notification.channels import NotificationChannelFactory
from notification.channels import RateLimitException
from notification.exceptions import (
    NotificationConfigurationError,
    TerminalNotificationError,
    TransientNotificationError,
)
from notification.tracker import NotificationTrackerService
from notification.message_builder import JobNotificationContent
from notification.runtime_config import (
    REDIS_URL_DEFAULT,
    get_notification_runtime_config,
)
from notification.user_settings import UserNotificationSettingsService

logger = logging.getLogger(__name__)

TRANSIENT_RETRY_INTERVALS = (30, 60, 120)
MAX_RATE_LIMIT_RETRIES = 3
# Maximum seconds to block a sync-mode thread waiting out a rate limit.
# Beyond this threshold the notification is recorded as failed immediately.
SYNC_MODE_MAX_WAIT_SECONDS = 30


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


def _reschedule_notification(
    notification_data: Dict[str, Any],
    delay_seconds: int,
    current_job,
    reason: str,
) -> None:
    """Re-enqueue a notification after a delay, releasing the current worker immediately.

    Raises RuntimeError if the enqueue fails so callers can record a failure rather
    than silently losing the notification.
    """
    try:
        queue = Queue(current_job.origin, connection=current_job.connection)
        queue.enqueue_in(
            timedelta(seconds=delay_seconds),
            process_notification_task,
            notification_data,
            job_timeout='5m',
            result_ttl=86400,
            failure_ttl=604800,
        )
    except Exception as exc:
        logger.error(
            "Failed to reschedule notification via %s in %ds (%s): %s",
            current_job.origin, delay_seconds, reason, exc, exc_info=True,
        )
        raise RuntimeError(f"Reschedule failed: {exc}") from exc
    logger.info(
        "Rescheduled notification via %s in %ds (%s); worker released",
        current_job.origin, delay_seconds, reason,
    )


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
                pass  # Redis unavailable; rate limiting will be skipped
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
        runtime_config = get_notification_runtime_config()
        self.channel_configs = {
            name.lower(): config
            for name, config in (channel_configs or runtime_config.channels).items()
        }
        self.user_settings = UserNotificationSettingsService(self.repo.db)

        # Initialize deduplication tracker
        self.tracker = NotificationTrackerService(repo)

        # Initialize Redis Queue
        self.redis_url = redis_url or runtime_config.redis_url
        self.base_url = base_url or runtime_config.base_url

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
            'require_enabled_delivery': require_enabled_delivery,
            'transient_retries': 0,
            'rate_limit_retries': 0,
            # origin_notification_id is absent on first execution; process_notification_task
            # sets it from the generated notification_id and threads it through all reschedules.
        }

        # Queue or process immediately
        if self.async_mode:
            # Transient failures re-enqueue with a delay, releasing the worker immediately.
            # Terminal failures re-raise so RQ marks the job failed (visible in the failed registry).
            try:
                job = self.queue.enqueue(
                    process_notification_task,
                    notification_data,
                    job_timeout='5m',
                    result_ttl=86400,
                )
                notification_id = job.id
                logger.info(f"Queued notification as job {job.id}")
            except Exception as e:
                logger.error(
                    f"Redis enqueue failed ({e}), falling back to synchronous send",
                    exc_info=True,
                )
                notification_id = process_notification_task(notification_data)
        else:
            # Process synchronously
            notification_id = process_notification_task(notification_data)
        
        return notification_id

    def get_user_notification_snapshot(self, user) -> Any:
        """Return the effective per-user notification settings snapshot."""
        with db_session_scope() as session:
            fresh_user = session.get(User, user.id)
            if fresh_user is None:
                raise NotificationConfigurationError(
                    "Notification user does not exist or is inactive",
                    failure_class="user_missing",
                )
            return UserNotificationSettingsService(session).get_settings_snapshot(fresh_user)

    def get_enabled_channels_for_user(self, user) -> list[str]:
        """Return channels that are enabled and deliverable for a user."""
        snapshot = self.get_user_notification_snapshot(user)
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

        fit_score = content.match.fit_score

        if fit_score >= self._priority_high:
            priority = NotificationPriority.HIGH
        elif fit_score >= self._priority_normal:
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
                    'fit_score': fit_score,
                    'preference_score': content.match.preference_score,
                    'ranking_mode_used': content.match.ranking_mode_used,
                    'explanation_label': content.match.explanation_label,
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
                    event_type="new_match_alert",
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
        alert_eligible_matches: int,
        min_fit_for_alerts: int,
        channels: Optional[list] = None,
        task_id: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """Send batch completion notification."""
        if channels is None:
            channels = ['email']

        subject = f"✅ Job matching complete: {alert_eligible_matches} alert-eligible saved matches"
        body = f"""Your job matching batch is complete!

Results Summary:
- Saved top matches: {total_matches}
- Saved matches above your alert fit floor ({min_fit_for_alerts}%): {alert_eligible_matches}

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
                        'alert_eligible_matches': alert_eligible_matches,
                        'min_fit_for_alerts': min_fit_for_alerts,
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

        raise ValueError(f"No recipient configured for channel type: {channel}")

    def _configured_recipient_for_channel(self, channel: str) -> Optional[str]:
        config = self.channel_configs.get(channel.lower())
        if config is None:
            return None
        if isinstance(config, dict):
            return config.get("recipient")
        return getattr(config, "recipient", None)

    def get_queue_status(self) -> Dict[str, Any]:
        """Get queue status including failed job count from the DLQ."""
        if not self.async_mode:
            return {'status': 'sync_mode', 'queue_length': 0}

        try:
            failed_registry = FailedJobRegistry(queue=self.queue)
            return {
                'status': 'active',
                'queue_length': len(self.queue),
                'failed_job_count': len(failed_registry),
                'redis_connected': self.redis_conn.ping(),
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}


def _handle_preflight_rate_limit(
    notification_id: str,
    origin_id: str,
    notification_data: Dict[str, Any],
    channel_type: str,
    wait_time: float,
    current_job,
    rate_limit_retries: int,
) -> Optional[str]:
    """Handle a pre-flight rate-limit signal before attempting to send.

    Returns the notification_id if the caller should return early (reschedule or
    failure recorded), or None if the caller should proceed with the send.
    """
    if current_job:
        if rate_limit_retries < MAX_RATE_LIMIT_RETRIES:
            _reschedule_notification(
                {**notification_data, 'rate_limit_retries': rate_limit_retries + 1,
                 'origin_notification_id': origin_id},
                math.ceil(wait_time), current_job, "rate_limit_active",
            )
            return notification_id
        # Async mode, retries exhausted — record failure rather than blocking the worker.
        logger.error(
            "Pre-flight rate limit: max retries (%d) exceeded for notification %s (origin: %s)",
            MAX_RATE_LIMIT_RETRIES, notification_id, origin_id,
        )
        _record_notification_failure(
            notification_id, notification_data,
            f"Pre-flight rate limit: max retries ({MAX_RATE_LIMIT_RETRIES}) exceeded",
            failure_class="rate_limit_exhausted",
        )
        return notification_id
    # Sync mode: only sleep for short waits; a long sleep would block the calling thread.
    if wait_time > SYNC_MODE_MAX_WAIT_SECONDS:
        logger.warning(
            "Rate limit wait %.1fs exceeds sync-mode cap (%ds) for %s; recording failure",
            wait_time, SYNC_MODE_MAX_WAIT_SECONDS, channel_type,
        )
        _record_notification_failure(
            notification_id, notification_data,
            f"Rate limit wait ({wait_time:.0f}s) exceeds sync-mode cap",
            failure_class="rate_limit_sync_too_long",
        )
        return notification_id
    logger.info("Rate limit active for %s, waiting %.1fs (sync mode)", channel_type, wait_time)
    time.sleep(wait_time)
    return None


def _handle_rate_limit_exception(
    notification_id: str,
    origin_id: str,
    notification_data: Dict[str, Any],
    channel_type: str,
    exc: "RateLimitException",
    current_job,
    rate_limit_retries: int,
    max_wait_seconds: int,
    rate_limiter: "NotificationRateLimiter",
) -> str:
    """Handle a RateLimitException raised during send."""
    retry_after = exc.retry_after or 60
    actual_wait = min(retry_after, max_wait_seconds)
    rate_limiter.set_rate_limit(channel_type, actual_wait)

    if rate_limit_retries >= MAX_RATE_LIMIT_RETRIES:
        logger.error(
            "Max rate limit retries (%d) exceeded for notification %s (origin: %s)",
            MAX_RATE_LIMIT_RETRIES, notification_id, origin_id,
        )
        _record_notification_failure(
            notification_id, notification_data,
            f"Rate limit exceeded after {MAX_RATE_LIMIT_RETRIES} retries",
        )
        return notification_id

    logger.warning(
        "Rate limited by %s. Rescheduling in %ds (attempt %d/%d)",
        channel_type, actual_wait, rate_limit_retries + 1, MAX_RATE_LIMIT_RETRIES,
    )
    if current_job:
        _reschedule_notification(
            {**notification_data, 'rate_limit_retries': rate_limit_retries + 1,
             'origin_notification_id': origin_id},
            actual_wait, current_job, "rate_limited",
        )
    else:
        # Sync fallback: no queue to reschedule into — record failure immediately
        _record_notification_failure(
            notification_id, notification_data,
            f"Rate limited in sync mode after {rate_limit_retries + 1} attempt(s)",
        )
    return notification_id


def _handle_transient_error(
    notification_id: str,
    origin_id: str,
    notification_data: Dict[str, Any],
    channel_type: str,
    exc: "TransientNotificationError",
    current_job,
    transient_retries: int,
) -> str:
    """Handle a TransientNotificationError raised during send."""
    if transient_retries >= len(TRANSIENT_RETRY_INTERVALS):
        logger.error(
            "Max transient retries (%d) exceeded for notification %s (origin: %s)",
            len(TRANSIENT_RETRY_INTERVALS), notification_id, origin_id,
        )
        _record_notification_failure(notification_id, notification_data, str(exc), failure_class=exc.failure_class)
        return notification_id

    delay = TRANSIENT_RETRY_INTERVALS[transient_retries]
    logger.warning(
        "Transient failure for %s via %s (origin: %s): %s. Rescheduling in %ds (%d/%d)",
        notification_id, channel_type, origin_id, exc, delay,
        transient_retries + 1, len(TRANSIENT_RETRY_INTERVALS),
    )
    if current_job:
        _reschedule_notification(
            {**notification_data, 'transient_retries': transient_retries + 1,
             'origin_notification_id': origin_id},
            delay, current_job, "transient_failure",
        )
    else:
        # Sync fallback: no queue to reschedule into — record failure immediately
        _record_notification_failure(notification_id, notification_data, str(exc), failure_class=exc.failure_class)
    return notification_id


# Worker task - must be at module level for RQ
def process_notification_task(notification_data: Dict[str, Any]) -> str:
    """
    Process a notification (called by RQ worker or synchronously as Redis fallback).

    In async mode (running inside an RQ job):
    - Transient failures and rate limits re-enqueue the job with a delay via
      Queue.enqueue_in(), releasing the worker thread immediately.
    - Terminal failures re-raise so RQ moves the job to the failed registry (DLQ).

    In sync mode (Redis unavailable, direct call):
    - Transient/rate-limit failures record immediately and return the id.
    - No reschedule possible without a queue.
    - Short pre-flight rate-limit waits (≤ SYNC_MODE_MAX_WAIT_SECONDS) are honored
      with a brief sleep; longer waits record a failure immediately.
    """
    notification_id = str(uuid.uuid4())
    channel_type = notification_data['channel_type']
    runtime_config = get_notification_runtime_config()
    redis_url = runtime_config.redis_url
    max_wait_seconds = runtime_config.rate_limit_max_wait_seconds

    # origin_id threads through all reschedules for log correlation. Must use `or` rather than
    # dict.get(key, default) because the key may be absent or present with value None.
    origin_id = notification_data.get('origin_notification_id') or notification_id

    logger.info("Processing notification %s (origin: %s) via %s", notification_id, origin_id, channel_type)

    current_job = get_current_job()  # None when called synchronously
    transient_retries = notification_data.get('transient_retries', 0)
    rate_limit_retries = notification_data.get('rate_limit_retries', 0)

    rate_limiter = NotificationRateLimiter(redis_url, max_wait_seconds)

    # If a global rate limit is still active from a previous attempt, reschedule or fail fast.
    wait_time = rate_limiter.get_wait_time(channel_type)
    if wait_time > 0:
        result = _handle_preflight_rate_limit(
            notification_id, origin_id, notification_data, channel_type,
            wait_time, current_job, rate_limit_retries,
        )
        if result is not None:
            return result

    try:
        return _send_and_record_notification(notification_id, notification_data)

    except RateLimitException as e:
        return _handle_rate_limit_exception(
            notification_id, origin_id, notification_data, channel_type,
            e, current_job, rate_limit_retries, max_wait_seconds, rate_limiter,
        )

    except TerminalNotificationError as e:
        logger.error("Terminal failure for %s: %s", notification_id, e, exc_info=True)
        _record_notification_failure(notification_id, notification_data, str(e), failure_class=e.failure_class)
        raise

    except TransientNotificationError as e:
        return _handle_transient_error(
            notification_id, origin_id, notification_data, channel_type,
            e, current_job, transient_retries,
        )

    except Exception as e:
        logger.error("Unexpected failure for %s: %s", notification_id, e, exc_info=True)
        _record_notification_failure(
            notification_id, notification_data, str(e),
            failure_class=getattr(e, 'failure_class', 'unknown'),
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

    if not success:
        # channel.send() returning False is a retryable failure — treat it the same as a
        # raised exception so the job enters the transient retry path rather than silently
        # recording an undelivered notification as a completed job.
        raise TransientNotificationError(
            f"Channel {channel_type} reported send failure (returned False)",
            failure_class="channel_send_failed",
        )

    # Record success in database
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
            success=True,
            error_message=None,
            metadata=metadata,
            allow_resend=notification_data.get('allow_resend', True)
        )

    logger.info(f"Notification {notification_id} sent successfully")

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
                failure_class=failure_class,
                error_message=error_message,
                metadata=metadata,
                allow_resend=True,  # Always allow retry of undelivered notifications
            )
        _mark_settings_test_result(notification_data, status='failed', error_message=error_message)
    except Exception as db_error:
        logger.error(
            f"Failed to record failure for notification {notification_id}: {db_error}"
        )
