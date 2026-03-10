#!/usr/bin/env python3
"""
Tests for the notification system.
Covers: channels, deduplication tracker, service integration,
        rate limiting, and task processing.
"""

import os
import uuid
import time
import json
import pytest
import inspect
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch, MagicMock

from notification import (
    EmailChannel, DiscordChannel, TelegramChannel,
    WebhookChannel, InAppChannel, NotificationChannelFactory, NotificationChannel,
    NotificationTrackerService, DefaultDeduplicationStrategy,
    AggressiveDeduplicationStrategy, NotificationEvent,
    NotificationService, NotificationPriority, RateLimitException,
)
from notification.message_builder import (
    JobNotificationContent, JobInfo, MatchInfo, RequirementsInfo,
)


# ---------------------------------------------------------------------------
# Helpers & fixtures
# ---------------------------------------------------------------------------

def assert_valid_uuid(value: str) -> None:
    uuid.UUID(value)


def make_db_scope_mock(mock_scope):
    mock_session = Mock()
    ctx = MagicMock()
    ctx.__enter__ = Mock(return_value=mock_session)
    ctx.__exit__ = Mock(return_value=False)
    mock_scope.return_value = ctx
    return mock_session


def _make_task_patches(mock_scope, mock_tracker_class, mock_rl_class):
    """Wire up the three shared collaborators for process_notification_task tests."""
    make_db_scope_mock(mock_scope)
    mock_tracker = Mock()
    mock_tracker_class.return_value = mock_tracker
    mock_rl = Mock()
    mock_rl.get_wait_time.return_value = 0
    mock_rl.is_wait_exceeded.return_value = False
    mock_rl_class.return_value = mock_rl
    return mock_tracker, mock_rl


NOTIFICATION_DATA = {
    'channel_type': 'email',
    'recipient': 'test@example.com',
    'subject': 'Test',
    'body': 'Test body',
    'metadata': {},
    'user_id': 'user-123',
    'job_match_id': 'match-456',
    'event_type': 'new_match',
    'priority': 'normal',
    'allow_resend': True,
}

DISCORD_DATA = {**NOTIFICATION_DATA, 'channel_type': 'discord', 'recipient': 'webhook-url'}


@pytest.fixture(autouse=True)
def restore_env():
    original = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(original)


@pytest.fixture
def mock_repo():
    repo = Mock()
    repo.db = Mock()
    return repo


@pytest.fixture
def tracker(mock_repo):
    return NotificationTrackerService(mock_repo)


@pytest.fixture
def job_content():
    return JobNotificationContent(
        job=JobInfo(
            title="Python Developer", company="TechCorp",
            location="San Francisco, CA", is_remote=True,
        ),
        match=MatchInfo(
            overall_score=85.5, fit_score=80.0,
            want_score=75.0, required_coverage=0.85,
        ),
        requirements=RequirementsInfo(total=10, matched=8, key_matches=[]),
        apply_url='https://example.com/apply',
    )


# ---------------------------------------------------------------------------
# Email channel
# ---------------------------------------------------------------------------

class TestEmailChannel:

    def test_validation_fails_without_config(self):
        for key in ['SMTP_SERVER', 'SMTP_PORT', 'SMTP_USERNAME', 'SMTP_PASSWORD']:
            os.environ.pop(key, None)
        assert not EmailChannel().validate_config()

    def test_validation_passes_with_config(self):
        os.environ.update({
            'SMTP_SERVER': 'smtp.gmail.com', 'SMTP_PORT': '587',
            'SMTP_USERNAME': 'test@example.com', 'SMTP_PASSWORD': 'password',
        })
        assert EmailChannel().validate_config()

    @patch('notification.channels.smtplib.SMTP')
    def test_send_success(self, mock_smtp_class):
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__ = Mock(return_value=mock_smtp)
        mock_smtp_class.return_value.__exit__ = Mock(return_value=False)
        os.environ.update({
            'SMTP_SERVER': 'smtp.gmail.com', 'SMTP_PORT': '587',
            'SMTP_USERNAME': 'test@example.com', 'SMTP_PASSWORD': 'password',
            'FROM_EMAIL': 'from@example.com',
        })

        result = EmailChannel().send(
            recipient='to@example.com', subject='Test Subject',
            body='Test Body', metadata={},
        )

        assert result is True
        mock_smtp.starttls.assert_called_once()
        mock_smtp.login.assert_called_once_with('test@example.com', 'password')
        mock_smtp.send_message.assert_called_once()

    def test_send_without_config_returns_false(self):
        for key in ['SMTP_SERVER', 'SMTP_PORT', 'SMTP_USERNAME', 'SMTP_PASSWORD']:
            os.environ.pop(key, None)

        result = EmailChannel().send(
            recipient='to@example.com', subject='Test', body='Body', metadata={}
        )

        assert result is False


# ---------------------------------------------------------------------------
# Discord channel
# ---------------------------------------------------------------------------

class TestDiscordChannel:

    @patch('notification.channels.requests.post')
    def test_send_success(self, mock_post):
        mock_post.return_value.raise_for_status = Mock()
        mock_post.return_value.status_code = 204

        result = DiscordChannel().send(
            recipient='webhook-url',
            subject='Test Notification',
            body='This is a test message',
            metadata={
                'discord_webhook_url': 'https://discord.com/api/webhooks/test',
                'score': 85.5, 'company': 'TechCorp',
            },
        )

        assert result is True
        call_args = mock_post.call_args
        assert call_args[0][0] == 'https://discord.com/api/webhooks/test'
        payload = call_args[1]['json']
        assert 'embeds' in payload
        assert payload['embeds'][0]['title'] == 'Test Notification'

    def test_send_without_webhook_returns_false(self):
        os.environ.pop('DISCORD_WEBHOOK_URL', None)
        result = DiscordChannel().send(
            recipient='', subject='Test', body='Body', metadata={}
        )
        assert result is False

    @patch('notification.channels.requests.post')
    def test_send_network_failure_returns_false(self, mock_post):
        mock_post.side_effect = Exception('Network error')
        result = DiscordChannel().send(
            recipient='', subject='Test', body='Body',
            metadata={'discord_webhook_url': 'https://test.com/webhook'},
        )
        assert result is False

    @patch('notification.channels.requests.post')
    def test_rate_limit_429_raises_with_retry_after(self, mock_post):
        mock_post.return_value = Mock(
            status_code=429, headers={'Retry-After': '30'}
        )
        with pytest.raises(RateLimitException) as exc_info:
            DiscordChannel().send(
                recipient='test', subject='Test', body='Body',
                metadata={'discord_webhook_url': 'https://test.com/webhook'},
            )
        assert exc_info.value.retry_after == 30

    @patch('notification.channels.requests.post')
    def test_rate_limit_429_defaults_to_60_when_no_header(self, mock_post):
        mock_post.return_value = Mock(status_code=429, headers={})
        with pytest.raises(RateLimitException) as exc_info:
            DiscordChannel().send(
                recipient='test', subject='Test', body='Body',
                metadata={'discord_webhook_url': 'https://test.com/webhook'},
            )
        assert exc_info.value.retry_after == 60


# ---------------------------------------------------------------------------
# Telegram channel
# ---------------------------------------------------------------------------

class TestTelegramChannel:

    @patch('notification.channels.requests.post')
    def test_send_success(self, mock_post):
        mock_post.return_value = Mock(status_code=200)
        mock_post.return_value.json.return_value = {'ok': True}
        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token-123'

        result = TelegramChannel().send(
            recipient='@testchannel', subject='Job Alert',
            body='New match found!', metadata={},
        )

        assert result is True
        call_args = mock_post.call_args
        assert 'test-token-123' in call_args[0][0]
        assert call_args[1]['json']['chat_id'] == '@testchannel'
        assert call_args[1]['json']['parse_mode'] == 'HTML'

    def test_send_without_token_returns_false(self):
        os.environ.pop('TELEGRAM_BOT_TOKEN', None)
        result = TelegramChannel().send(
            recipient='@channel', subject='Test', body='Body', metadata={}
        )
        assert result is False

    @patch('notification.channels.requests.post')
    def test_api_error_returns_false(self, mock_post):
        mock_post.return_value = Mock(status_code=400, text='Bad Request')
        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'
        result = TelegramChannel().send(
            recipient='@channel', subject='Test', body='Body', metadata={}
        )
        assert result is False

    @patch('notification.channels.requests.post')
    def test_rate_limit_429_raises(self, mock_post):
        mock_post.return_value = Mock(
            status_code=429,
            headers={},
        )
        mock_post.return_value.json.return_value = {'parameters': {'retry_after': 45}}
        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'

        with pytest.raises(RateLimitException) as exc_info:
            TelegramChannel().send(
                recipient='@testchannel', subject='Test', body='Body', metadata={}
            )
        assert exc_info.value.retry_after == 45


# ---------------------------------------------------------------------------
# Webhook channel
# ---------------------------------------------------------------------------

class TestWebhookChannel:

    @patch('notification.channels.requests.post')
    def test_send_json_body(self, mock_post):
        mock_post.return_value.raise_for_status = Mock()
        result = WebhookChannel().send(
            recipient='https://example.com/webhook', subject='',
            body=json.dumps({'event': 'test', 'data': 'value'}),
            metadata={'custom': 'header'},
        )
        assert result is True
        assert mock_post.call_args[1]['json'] == {'event': 'test', 'data': 'value'}

    @patch('notification.channels.requests.post')
    def test_send_plain_body_wrapped_in_json(self, mock_post):
        mock_post.return_value.raise_for_status = Mock()
        WebhookChannel().send(
            recipient='https://example.com/webhook', subject='Subject',
            body='Plain text message', metadata={},
        )
        payload = mock_post.call_args[1]['json']
        assert payload['subject'] == 'Subject'
        assert payload['body'] == 'Plain text message'


# ---------------------------------------------------------------------------
# Channel factory
# ---------------------------------------------------------------------------

class TestNotificationChannelFactory:

    def test_returns_email_channel(self):
        assert isinstance(NotificationChannelFactory.get_channel('email'), EmailChannel)

    def test_returns_discord_channel(self):
        assert isinstance(NotificationChannelFactory.get_channel('discord'), DiscordChannel)

    def test_returns_telegram_channel(self):
        assert isinstance(NotificationChannelFactory.get_channel('telegram'), TelegramChannel)

    def test_unknown_channel_raises(self):
        with pytest.raises(ValueError, match='Unknown channel type'):
            NotificationChannelFactory.get_channel('unknown')

    def test_list_channels_contains_expected(self):
        channels = NotificationChannelFactory.list_channels()
        assert 'email' in channels
        assert 'discord' in channels
        assert 'telegram' in channels
        assert 'slack' not in channels

    def test_register_non_channel_subclass_raises(self):
        class NotAChannel:
            @property
            def channel_type(self): return 'test'
            def send(self, *a, **kw): return True
            def validate_config(self): return True

        with pytest.raises(ValueError):
            NotificationChannelFactory.register_channel('test', NotAChannel)


# ---------------------------------------------------------------------------
# Notification tracker
# ---------------------------------------------------------------------------

class TestNotificationTracker:

    def test_dedup_hash_is_deterministic(self, tracker):
        h1 = tracker.generate_dedup_hash('user1', 'match1', 'new_match', 'email')
        h2 = tracker.generate_dedup_hash('user1', 'match1', 'new_match', 'email')
        assert h1 == h2
        assert len(h1) == 32

    def test_dedup_hash_differs_on_channel_and_user(self, tracker):
        base = tracker.generate_dedup_hash('user1', 'match1', 'new_match', 'email')
        diff_channel = tracker.generate_dedup_hash('user1', 'match1', 'new_match', 'discord')
        diff_user = tracker.generate_dedup_hash('user2', 'match1', 'new_match', 'email')
        assert base != diff_channel
        assert base != diff_user

    def test_content_hash_is_deterministic(self, tracker):
        h1 = tracker.generate_content_hash('Subject', 'Body', {'key': 'val'})
        h2 = tracker.generate_content_hash('Subject', 'Body', {'key': 'val'})
        h3 = tracker.generate_content_hash('Different', 'Body', {'key': 'val'})
        assert h1 == h2
        assert h1 != h3

    def test_default_strategy_allows_first_notification(self):
        strategy = DefaultDeduplicationStrategy()
        event = NotificationEvent(
            user_id='u1', job_match_id='m1', event_type='new_match',
            channel_type='email', content_hash='h1',
        )
        assert strategy.should_allow_notification(None, event) is True

    def test_default_strategy_blocks_exact_duplicate(self):
        strategy = DefaultDeduplicationStrategy()
        existing = Mock(
            content_hash='h1', last_sent_at=datetime.now(timezone.utc), allow_resend=False
        )
        event = NotificationEvent(
            user_id='u1', job_match_id='m1', event_type='new_match',
            channel_type='email', content_hash='h1',
        )
        assert strategy.should_allow_notification(existing, event) is False

    def test_default_strategy_allows_content_change(self):
        strategy = DefaultDeduplicationStrategy()
        existing = Mock(content_hash='h1', last_sent_at=datetime.now(timezone.utc))
        event = NotificationEvent(
            user_id='u1', job_match_id='m1', event_type='new_match',
            channel_type='email', content_hash='h2',
        )
        assert strategy.should_allow_notification(existing, event) is True

    def test_default_strategy_allows_resend_after_interval(self):
        strategy = DefaultDeduplicationStrategy(default_interval_hours=24)
        existing = Mock(
            content_hash='h1',
            last_sent_at=datetime.now(timezone.utc) - timedelta(hours=25),
            allow_resend=True,
            resend_interval_hours=24,
        )
        event = NotificationEvent(
            user_id='u1', job_match_id='m1', event_type='score_improved',
            channel_type='email', content_hash='h1',
        )
        assert strategy.should_allow_notification(existing, event) is True

    def test_aggressive_strategy_blocks_all_resends(self):
        strategy = AggressiveDeduplicationStrategy()
        existing = Mock(last_sent_at=datetime.now(timezone.utc) - timedelta(days=30))
        event = NotificationEvent(
            user_id='u1', job_match_id='m1', event_type='score_improved',
            channel_type='email',
        )
        assert strategy.should_allow_notification(existing, event) is False
        assert strategy.should_allow_notification(None, event) is True

    @patch('notification.tracker.NotificationTrackerService._get_existing_notification')
    def test_allows_new_event(self, mock_get, tracker):
        mock_get.return_value = None
        assert tracker.should_send_notification(
            user_id='u1', job_match_id='m1', event_type='new_match',
            channel_type='email', subject='Test', body='Body',
        ) is True

    @patch('notification.tracker.NotificationTrackerService._get_existing_notification')
    @patch.object(NotificationTrackerService, 'generate_content_hash')
    def test_blocks_duplicate(self, mock_hash, mock_get, tracker):
        mock_get.return_value = Mock(
            content_hash='existing_hash',
            last_sent_at=datetime.now(timezone.utc),
            allow_resend=False,
        )
        mock_hash.return_value = 'existing_hash'
        assert tracker.should_send_notification(
            user_id='u1', job_match_id='m1', event_type='new_match',
            channel_type='email', subject='Test', body='Body',
        ) is False


# ---------------------------------------------------------------------------
# NotificationService — basic integration
# ---------------------------------------------------------------------------

class TestNotificationService:

    @patch('notification.service.NotificationTrackerService')
    @patch('notification.service.Queue')
    @patch('notification.service.Redis')
    def test_send_queues_notification(
        self, mock_redis, mock_queue_class, mock_tracker_class, mock_repo
    ):
        mock_tracker = Mock()
        mock_tracker.should_send_notification.return_value = True
        mock_tracker_class.return_value = mock_tracker

        mock_job = Mock()
        mock_job.id = 'job-123'
        mock_queue = Mock()
        mock_queue.enqueue.return_value = mock_job
        mock_queue_class.return_value = mock_queue
        mock_redis.from_url.return_value.ping.return_value = True

        service = NotificationService(mock_repo)
        result = service.send_notification(
            channel_type='email', recipient='test@example.com',
            subject='Test', body='Body', user_id='user1',
            job_match_id='match1', event_type='new_match',
        )

        assert result is not None
        mock_queue.enqueue.assert_called_once()

    @patch('notification.service.NotificationTrackerService')
    def test_duplicate_suppressed_returns_none(self, mock_tracker_class, mock_repo):
        mock_tracker = Mock()
        mock_tracker.should_send_notification.return_value = False
        mock_tracker_class.return_value = mock_tracker

        service = NotificationService(mock_repo)
        result = service.send_notification(
            channel_type='email', recipient='test@example.com',
            subject='Test', body='Body', user_id='user1',
            job_match_id='match1', event_type='new_match',
        )

        assert result is None

    @patch('notification.service.NotificationService.send_notification')
    def test_notify_new_match_single_channel(self, mock_send, mock_repo, job_content):
        mock_send.return_value = 'notif-123'
        service = NotificationService(mock_repo)
        service.notify_new_match(
            user_id='user1', match_id='match1',
            content=job_content, channels=['email'],
        )

        mock_send.assert_called_once()
        kwargs = mock_send.call_args[1]
        assert kwargs['channel_type'] == 'email'
        assert kwargs['event_type'] == 'new_high_score_match'
        assert 'Python Developer' in kwargs['subject']

    @patch('notification.service.NotificationService.send_notification')
    def test_notify_new_match_multiple_channels(self, mock_send, mock_repo, job_content):
        mock_send.return_value = 'notif-123'
        os.environ['DISCORD_WEBHOOK_URL'] = 'https://discord.com/webhook'
        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'

        service = NotificationService(mock_repo)
        results = service.notify_new_match(
            user_id='user1', match_id='match1', content=job_content,
            channels=['email', 'discord', 'telegram'],
        )

        assert mock_send.call_count == 3
        assert len(results) == 3

    def test_priority_enum_values(self):
        assert NotificationPriority.LOW.value == 'low'
        assert NotificationPriority.NORMAL.value == 'normal'
        assert NotificationPriority.HIGH.value == 'high'
        assert NotificationPriority.URGENT.value == 'urgent'


# ---------------------------------------------------------------------------
# NotificationService — queue status
# ---------------------------------------------------------------------------

class TestNotificationServiceGetQueueStatus:

    def _make_service(self, mock_redis_class, mock_queue_class, mock_tracker_class,
                      ping_side_effect=None, ping_return=True):
        mock_redis = Mock()
        if ping_side_effect:
            mock_redis.ping.side_effect = ping_side_effect
        else:
            mock_redis.ping.return_value = ping_return
        mock_queue = Mock()
        mock_queue.__len__ = Mock(return_value=5)
        mock_redis_class.from_url.return_value = mock_redis
        mock_queue_class.return_value = mock_queue
        mock_tracker_class.return_value = Mock()
        return NotificationService(repo=Mock(), use_async_queue=True), mock_redis, mock_queue

    @patch('notification.service.NotificationTrackerService')
    def test_sync_mode_returns_correct_status(self, mock_tracker_class):
        service = NotificationService(repo=Mock(), use_async_queue=False)
        result = service.get_queue_status()
        assert result['status'] == 'sync_mode'
        assert result['queue_length'] == 0

    def test_active_queue_returns_length_and_connected(self):
        with patch('notification.service.Redis') as mr, \
             patch('notification.service.Queue') as mq, \
             patch('notification.service.NotificationTrackerService') as mt:
            service, _, _ = self._make_service(mr, mq, mt)
            result = service.get_queue_status()
            assert result['status'] == 'active'
            assert result['queue_length'] == 5
            assert result['redis_connected'] is True

    def test_redis_error_during_status_check_returns_error(self):
        with patch('notification.service.Redis') as mr, \
             patch('notification.service.Queue') as mq, \
             patch('notification.service.NotificationTrackerService') as mt:
            service, _, _ = self._make_service(
                mr, mq, mt,
                ping_side_effect=[True, Exception("Connection refused")],
            )
            result = service.get_queue_status()
            assert result['status'] == 'error'
            assert 'error' in result


# ---------------------------------------------------------------------------
# SOLID principles
# ---------------------------------------------------------------------------

class TestSOLIDPrinciples:

    def test_all_channels_share_interface(self):
        for cls in [EmailChannel, DiscordChannel, TelegramChannel, WebhookChannel, InAppChannel]:
            ch = cls()
            assert hasattr(ch, 'channel_type')
            assert hasattr(ch, 'send')
            assert hasattr(ch, 'validate_config')
            params = list(inspect.signature(ch.send).parameters.keys())
            assert 'recipient' in params
            assert 'subject' in params
            assert 'body' in params

    def test_factory_requires_notification_channel_subclass(self):
        class NotAChannel:
            @property
            def channel_type(self): return 'x'
            def send(self, *a, **kw): return True
            def validate_config(self): return True

        with pytest.raises(ValueError):
            NotificationChannelFactory.register_channel('x', NotAChannel)

    def test_channel_interface_is_minimal(self):
        public_methods = [
            name for name, _ in inspect.getmembers(
                NotificationChannel, predicate=inspect.isfunction
            ) if not name.startswith('_')
        ]
        assert 'send' in public_methods
        assert 'validate_config' in public_methods

    def test_factory_channel_is_usable_as_interface(self):
        ch = NotificationChannelFactory.get_channel('email')
        assert hasattr(ch, 'send')
        assert hasattr(ch, 'channel_type')


# ---------------------------------------------------------------------------
# NotificationRateLimiter
# ---------------------------------------------------------------------------

class TestNotificationRateLimiter:

    def _limiter(self, max_wait=300):
        from notification.service import NotificationRateLimiter
        limiter = NotificationRateLimiter(max_wait_seconds=max_wait)
        return limiter

    def test_set_rate_limit_stores_correct_key_ttl_and_value(self):
        limiter = self._limiter()
        mock_redis = Mock()
        limiter._redis = mock_redis

        before = time.time()
        limiter.set_rate_limit("discord", 60)
        after = time.time()

        mock_redis.setex.assert_called_once()
        key, ttl, value = mock_redis.setex.call_args[0]
        assert key == "notification:rate_limit:discord"
        assert ttl == 65  # retry_after + 5 buffer
        stored_until = float(value)
        assert before + 60 <= stored_until <= after + 60

    def test_get_wait_time_returns_zero_when_no_redis(self):
        limiter = self._limiter()
        with patch.object(limiter, '_get_redis', return_value=None):
            assert limiter.get_wait_time("discord") == 0

    def test_get_wait_time_returns_zero_when_no_key(self):
        limiter = self._limiter()
        limiter._redis = Mock()
        limiter._redis.get.return_value = None
        assert limiter.get_wait_time("discord") == 0

    def test_get_wait_time_returns_remaining_seconds(self):
        limiter = self._limiter()
        limiter._redis = Mock()
        limiter._redis.get.return_value = str(time.time() + 30)
        assert 25 < limiter.get_wait_time("discord") < 35

    def test_get_wait_time_returns_zero_when_expired(self):
        limiter = self._limiter()
        limiter._redis = Mock()
        limiter._redis.get.return_value = str(time.time() - 30)
        assert limiter.get_wait_time("discord") == 0

    def test_get_wait_time_capped_at_max_wait_seconds(self):
        limiter = self._limiter(max_wait=300)
        limiter._redis = Mock()
        limiter._redis.get.return_value = str(time.time() + 600)
        assert limiter.get_wait_time("discord") == 300

    def test_get_wait_time_handles_invalid_redis_value(self):
        limiter = self._limiter()
        limiter._redis = Mock()
        limiter._redis.get.return_value = "not_a_number"
        assert limiter.get_wait_time("discord") == 0

    def test_is_wait_exceeded_true_when_at_max(self):
        limiter = self._limiter(max_wait=300)
        limiter._redis = Mock()
        limiter._redis.get.return_value = str(time.time() + 600)
        assert limiter.is_wait_exceeded("discord") is True

    def test_is_wait_exceeded_false_when_below_max(self):
        limiter = self._limiter(max_wait=300)
        limiter._redis = Mock()
        limiter._redis.get.return_value = str(time.time() + 30)
        assert limiter.is_wait_exceeded("discord") is False

    def test_handles_redis_unavailable_gracefully(self):
        from notification.service import NotificationRateLimiter
        with patch('notification.service.Redis') as mock_redis_class:
            mock_redis_class.from_url.side_effect = Exception("Redis unavailable")
            limiter = NotificationRateLimiter()
            assert limiter.get_wait_time('discord') == 0
            limiter.set_rate_limit('discord', 30)  # must not raise

    def test_retry_after_capped_to_max_in_ttl(self):
        from notification.service import NotificationRateLimiter
        with patch('notification.service.Redis') as mock_redis_class:
            mock_redis = Mock()
            mock_redis_class.from_url.return_value = mock_redis
            limiter = NotificationRateLimiter(max_wait_seconds=300)
            retry_after = 600
            actual_wait = min(retry_after, 300)
            limiter.set_rate_limit('discord', actual_wait)
            expiry = mock_redis.setex.call_args[0][1]
            assert expiry <= 305  # 300 + 5s buffer


# ---------------------------------------------------------------------------
# process_notification_task
# ---------------------------------------------------------------------------

class TestProcessNotificationTask:

    def test_successful_send_returns_valid_uuid(self):
        from notification.service import process_notification_task

        mock_channel = Mock()
        mock_channel.send.return_value = True

        with patch('notification.service.NotificationChannelFactory.get_channel',
                   return_value=mock_channel), \
             patch('notification.service.db_session_scope') as mock_scope, \
             patch('notification.service.NotificationTrackerService') as mock_tracker_class, \
             patch('notification.service.NotificationRateLimiter') as mock_rl_class:

            _make_task_patches(mock_scope, mock_tracker_class, mock_rl_class)
            result = process_notification_task(NOTIFICATION_DATA)

        assert_valid_uuid(result)
        mock_channel.send.assert_called_once()

    def test_rate_limit_retried_then_succeeds(self):
        from notification.service import process_notification_task

        mock_channel = Mock()
        mock_channel.send.side_effect = [
            RateLimitException("Rate limited", retry_after=10),
            RateLimitException("Rate limited", retry_after=10),
            True,
        ]

        with patch('notification.service.NotificationChannelFactory.get_channel',
                   return_value=mock_channel), \
             patch('notification.service.time.sleep'), \
             patch('notification.service.db_session_scope') as mock_scope, \
             patch('notification.service.NotificationTrackerService') as mock_tracker_class, \
             patch('notification.service.NotificationRateLimiter') as mock_rl_class:

            mock_tracker, mock_rl = _make_task_patches(
                mock_scope, mock_tracker_class, mock_rl_class
            )
            result = process_notification_task(DISCORD_DATA)

        assert_valid_uuid(result)
        assert mock_channel.send.call_count == 3
        assert mock_rl.set_rate_limit.call_count == 2

    def test_rate_limit_max_retries_exceeded_records_failure(self):
        from notification.service import process_notification_task

        mock_channel = Mock()
        mock_channel.send.side_effect = RateLimitException("Rate limited", retry_after=10)

        with patch('notification.service.NotificationChannelFactory.get_channel',
                   return_value=mock_channel), \
             patch('notification.service.time.sleep'), \
             patch('notification.service.db_session_scope') as mock_scope, \
             patch('notification.service.NotificationTrackerService') as mock_tracker_class, \
             patch('notification.service.NotificationRateLimiter') as mock_rl_class, \
             patch('notification.service._record_notification_failure') as mock_record_fail:

            _make_task_patches(mock_scope, mock_tracker_class, mock_rl_class)
            result = process_notification_task(DISCORD_DATA)

        assert_valid_uuid(result)
        mock_record_fail.assert_called_once()
        _, _, error_msg = mock_record_fail.call_args[0]
        assert "Rate limit" in error_msg

    def test_general_exception_records_failure_and_returns_uuid(self):
        from notification.service import process_notification_task

        mock_channel = Mock()
        mock_channel.send.side_effect = Exception("Network error")

        with patch('notification.service.NotificationChannelFactory.get_channel',
                   return_value=mock_channel), \
             patch('notification.service.db_session_scope') as mock_scope, \
             patch('notification.service.NotificationTrackerService') as mock_tracker_class, \
             patch('notification.service.NotificationRateLimiter') as mock_rl_class, \
             patch('notification.service._record_notification_failure') as mock_record_fail:

            _make_task_patches(mock_scope, mock_tracker_class, mock_rl_class)
            result = process_notification_task(NOTIFICATION_DATA)

        assert_valid_uuid(result)
        mock_record_fail.assert_called_once()
        _, _, error_msg = mock_record_fail.call_args[0]
        assert "Network error" in error_msg

    def test_active_rate_limit_sleeps_before_send(self):
        from notification.service import process_notification_task

        mock_channel = Mock()
        mock_channel.send.return_value = True

        with patch('notification.service.NotificationChannelFactory.get_channel',
                   return_value=mock_channel), \
             patch('notification.service.time.sleep') as mock_sleep, \
             patch('notification.service.db_session_scope') as mock_scope, \
             patch('notification.service.NotificationTrackerService') as mock_tracker_class, \
             patch('notification.service.NotificationRateLimiter') as mock_rl_class:

            mock_tracker, mock_rl = _make_task_patches(
                mock_scope, mock_tracker_class, mock_rl_class
            )
            mock_rl.get_wait_time.return_value = 15
            result = process_notification_task(NOTIFICATION_DATA)

        assert_valid_uuid(result)
        mock_sleep.assert_called_with(15)


# ---------------------------------------------------------------------------
# _send_and_record_notification
# ---------------------------------------------------------------------------

class TestSendAndRecordNotification:

    def _make_patches(self, mock_scope, mock_tracker_class):
        make_db_scope_mock(mock_scope)
        mock_tracker = Mock()
        mock_tracker_class.return_value = mock_tracker
        return mock_tracker

    def test_successful_send_records_success_true(self):
        from notification.service import _send_and_record_notification

        mock_channel = Mock()
        mock_channel.send.return_value = True

        with patch('notification.service.NotificationChannelFactory.get_channel',
                   return_value=mock_channel), \
             patch('notification.service.db_session_scope') as mock_scope, \
             patch('notification.service.NotificationTrackerService') as mock_tracker_class:

            mock_tracker = self._make_patches(mock_scope, mock_tracker_class)
            result = _send_and_record_notification("notif-123", NOTIFICATION_DATA)

        assert result == "notif-123"
        kwargs = mock_tracker.record_notification.call_args[1]
        assert kwargs['success'] is True
        assert kwargs['error_message'] is None

    def test_failed_send_records_success_false_with_error(self):
        from notification.service import _send_and_record_notification

        mock_channel = Mock()
        mock_channel.send.return_value = False

        with patch('notification.service.NotificationChannelFactory.get_channel',
                   return_value=mock_channel), \
             patch('notification.service.db_session_scope') as mock_scope, \
             patch('notification.service.NotificationTrackerService') as mock_tracker_class:

            mock_tracker = self._make_patches(mock_scope, mock_tracker_class)
            result = _send_and_record_notification("notif-123", NOTIFICATION_DATA)

        assert result == "notif-123"
        kwargs = mock_tracker.record_notification.call_args[1]
        assert kwargs['success'] is False
        assert kwargs['error_message'] is not None


# ---------------------------------------------------------------------------
# _record_notification_failure
# ---------------------------------------------------------------------------

class TestRecordNotificationFailure:

    def test_records_failure_with_correct_error_message(self):
        from notification.service import _record_notification_failure

        with patch('notification.service.db_session_scope') as mock_scope, \
             patch('notification.service.NotificationTrackerService') as mock_tracker_class:

            make_db_scope_mock(mock_scope)
            mock_tracker = Mock()
            mock_tracker_class.return_value = mock_tracker

            _record_notification_failure("notif-123", NOTIFICATION_DATA, "Test error")

        kwargs = mock_tracker.record_notification.call_args[1]
        assert kwargs['success'] is False
        assert kwargs['error_message'] == "Test error"

    def test_db_error_during_failure_recording_does_not_raise(self):
        from notification.service import _record_notification_failure

        with patch('notification.service.db_session_scope') as mock_scope:
            ctx = MagicMock()
            ctx.__enter__ = Mock(side_effect=Exception("DB error"))
            ctx.__exit__ = Mock(return_value=False)
            mock_scope.return_value = ctx

            # Must not raise
            _record_notification_failure("notif-123", NOTIFICATION_DATA, "Original error")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
