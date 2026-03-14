#!/usr/bin/env python3
"""
Extended tests for notification tracker - edge cases and uncovered functionality.

Tests cover:
1. Content hash generation edge cases
2. Deduplication strategy edge cases
3. Resend interval logic
4. Database recording scenarios
5. Metadata handling
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta, timezone
import json
import hashlib

from notification.tracker import (
    NotificationTrackerService,
    NotificationEvent,
    DefaultDeduplicationStrategy,
    AggressiveDeduplicationStrategy,
    DeduplicationStrategy,
    RESEND_INTERVAL_NEVER,
    should_notify_user
)
from database.models import NotificationTracker


class TestNotificationEvent:
    """Test NotificationEvent dataclass."""

    def test_notification_event_creation(self):
        """Test creating NotificationEvent."""
        event = NotificationEvent(
            user_id='user123',
            job_match_id='match456',
            event_type='new_match',
            channel_type='email'
        )

        assert event.user_id == 'user123'
        assert event.job_match_id == 'match456'
        assert event.event_type == 'new_match'
        assert event.channel_type == 'email'
        assert event.content_hash is None
        assert event.metadata is None

    def test_notification_event_with_optional_fields(self):
        """Test NotificationEvent with optional fields."""
        event = NotificationEvent(
            user_id='user123',
            job_match_id='match456',
            event_type='new_match',
            channel_type='email',
            content_hash='abc123',
            metadata={'key': 'value'}
        )

        assert event.content_hash == 'abc123'
        assert event.metadata == {'key': 'value'}

    def test_notification_event_no_job_match_id(self):
        """Test NotificationEvent without job_match_id (for batch notifications)."""
        event = NotificationEvent(
            user_id='user123',
            job_match_id=None,
            event_type='batch_complete',
            channel_type='email'
        )

        assert event.job_match_id is None


class TestDeduplicationStrategies:
    """Test deduplication strategy implementations."""

    def test_default_strategy_resendable_events(self):
        """Test default strategy resendable event types."""
        strategy = DefaultDeduplicationStrategy()

        # These event types should be in RESENDABLE_EVENTS
        assert 'score_improved' in strategy.RESENDABLE_EVENTS
        assert 'status_changed' in strategy.RESENDABLE_EVENTS

        # new_match should NOT be resendable
        assert 'new_match' not in strategy.RESENDABLE_EVENTS

    def test_default_strategy_get_resend_interval(self):
        """Test default strategy resend interval."""
        strategy = DefaultDeduplicationStrategy(default_interval_hours=48)
        assert strategy.get_resend_interval() == 48

        strategy_default = DefaultDeduplicationStrategy()
        assert strategy_default.get_resend_interval() == 24

    def test_aggressive_strategy_get_resend_interval(self):
        """Test aggressive strategy never allows resend."""
        strategy = AggressiveDeduplicationStrategy()
        assert strategy.get_resend_interval() == RESEND_INTERVAL_NEVER
        assert RESEND_INTERVAL_NEVER == 999999

    def test_default_strategy_content_change_detection(self):
        """Test default strategy detects content changes."""
        strategy = DefaultDeduplicationStrategy()

        existing = Mock()
        existing.content_hash = 'old_hash'
        existing.last_sent_at = datetime.now(timezone.utc)
        existing.allow_resend = False

        # Different content hash should allow resend
        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            content_hash='new_hash'  # Different
        )

        assert strategy.should_allow_notification(existing, event) is True

    def test_default_strategy_missing_content_hash(self):
        """Test strategy handles missing content hashes."""
        strategy = DefaultDeduplicationStrategy()

        existing = Mock()
        existing.content_hash = None  # No content hash
        existing.last_sent_at = datetime.now(timezone.utc)
        existing.allow_resend = False

        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            content_hash=None  # Also no content hash
        )

        # Without content hashes, should check time-based rules
        result = strategy.should_allow_notification(existing, event)
        assert result is False  # Blocked by allow_resend=False

    def test_default_strategy_time_based_resend(self):
        """Test strategy time-based resend logic."""
        strategy = DefaultDeduplicationStrategy(default_interval_hours=24)

        # Sent 25 hours ago (over interval)
        existing = Mock()
        existing.content_hash = 'same_hash'
        existing.last_sent_at = datetime.now(timezone.utc) - timedelta(hours=25)
        existing.allow_resend = True
        existing.resend_interval_hours = 24

        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='score_improved',  # Resendable
            channel_type='email',
            content_hash='same_hash'
        )

        assert strategy.should_allow_notification(existing, event) is True

    def test_default_strategy_too_soon_to_resend(self):
        """Test strategy blocks resend if too soon."""
        strategy = DefaultDeduplicationStrategy(default_interval_hours=24)

        # Sent 12 hours ago (under interval)
        existing = Mock()
        existing.content_hash = 'same_hash'
        existing.last_sent_at = datetime.now(timezone.utc) - timedelta(hours=12)
        existing.allow_resend = True
        existing.resend_interval_hours = 24

        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='score_improved',
            channel_type='email',
            content_hash='same_hash'
        )

        assert strategy.should_allow_notification(existing, event) is False

    def test_default_strategy_custom_resend_interval(self):
        """Test strategy with custom resend interval."""
        strategy = DefaultDeduplicationStrategy(default_interval_hours=24)

        # Custom interval of 48 hours, sent 36 hours ago
        existing = Mock()
        existing.content_hash = 'same_hash'
        existing.last_sent_at = datetime.now(timezone.utc) - timedelta(hours=36)
        existing.allow_resend = True
        existing.resend_interval_hours = 48  # Custom interval

        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='score_improved',
            channel_type='email',
            content_hash='same_hash'
        )

        # Should be blocked (36h < 48h interval)
        assert strategy.should_allow_notification(existing, event) is False


class TestNotificationTrackerService:
    """Test NotificationTrackerService methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_repo = Mock()
        self.mock_repo.db = Mock()
        self.tracker = NotificationTrackerService(self.mock_repo)

    def test_generate_dedup_hash_format(self):
        """Test dedup hash format and length."""
        hash_result = self.tracker.generate_dedup_hash(
            'user123', 'match456', 'new_match', 'email'
        )

        assert len(hash_result) == 32  # SHA-256 truncated to 32 chars
        assert all(c in '0123456789abcdef' for c in hash_result)

    def test_generate_dedup_hash_none_job_match_id(self):
        """Test dedup hash with None job_match_id."""
        hash1 = self.tracker.generate_dedup_hash(
            'user123', None, 'batch_complete', 'email'
        )
        hash2 = self.tracker.generate_dedup_hash(
            'user123', None, 'batch_complete', 'email'
        )

        assert hash1 == hash2
        assert len(hash1) == 32

    def test_generate_content_hash_with_metadata(self):
        """Test content hash generation with metadata."""
        hash1 = self.tracker.generate_content_hash(
            'Subject', 'Body', {'key': 'value'}
        )
        hash2 = self.tracker.generate_content_hash(
            'Subject', 'Body', {'key': 'value'}
        )
        hash3 = self.tracker.generate_content_hash(
            'Subject', 'Body', {'key': 'different'}
        )

        assert hash1 == hash2  # Same content
        assert hash1 != hash3  # Different metadata
        assert len(hash1) == 16  # Truncated to 16 chars

    def test_generate_content_hash_truncates_body(self):
        """Test content hash truncates long bodies."""
        long_body = 'x' * 1000

        hash1 = self.tracker.generate_content_hash(
            'Subject', long_body[:500], {}
        )
        hash2 = self.tracker.generate_content_hash(
            'Subject', long_body, {}
        )

        # Should hash the same since body is truncated to 500 chars
        assert hash1 == hash2

    def test_generate_content_hash_none_metadata(self):
        """Test content hash with None metadata."""
        hash1 = self.tracker.generate_content_hash(
            'Subject', 'Body', None
        )
        hash2 = self.tracker.generate_content_hash(
            'Subject', 'Body', None
        )

        assert hash1 == hash2

    def test_generate_content_hash_non_serializable_metadata(self):
        """Test content hash handles non-serializable metadata."""
        # datetime objects need special handling
        metadata = {'timestamp': datetime.now(timezone.utc)}

        hash_result = self.tracker.generate_content_hash(
            'Subject', 'Body', metadata
        )

        assert len(hash_result) == 16

    @patch.object(NotificationTrackerService, '_get_existing_notification')
    def test_should_send_notification_first_time(self, mock_get_existing):
        """Test should_send_notification for first-time notification."""
        mock_get_existing.return_value = None

        result = self.tracker.should_send_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            subject='Test',
            body='Body'
        )

        assert result is True

    @patch.object(NotificationTrackerService, '_get_existing_notification')
    def test_should_send_notification_with_empty_strings(self, mock_get_existing):
        """Test should_send_notification with empty strings."""
        mock_get_existing.return_value = None

        result = self.tracker.should_send_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            subject='',
            body=''
        )

        assert result is True

    @patch.object(NotificationTrackerService, '_get_existing_notification')
    def test_should_send_notification_calls_strategy(self, mock_get_existing):
        """Test should_send_notification uses strategy."""
        existing = Mock()
        existing.content_hash = 'hash1'
        existing.last_sent_at = datetime.now(timezone.utc)
        existing.allow_resend = False
        mock_get_existing.return_value = existing

        with patch.object(self.tracker.strategy, 'should_allow_notification') as mock_strategy:
            mock_strategy.return_value = False

            result = self.tracker.should_send_notification(
                user_id='user1',
                job_match_id='match1',
                event_type='new_match',
                channel_type='email',
                subject='Test',
                body='Body'
            )

            mock_strategy.assert_called_once()
            assert result is False

    def test_get_existing_notification(self):
        """Test _get_existing_notification database query."""
        mock_result = Mock()
        self.mock_repo.db.execute.return_value.scalar_one_or_none.return_value = mock_result

        result = self.tracker._get_existing_notification('dedup_hash_123')

        assert result == mock_result
        self.mock_repo.db.execute.assert_called_once()

    def test_record_notification_new(self):
        """Test record_notification creates new record."""
        self.tracker._get_existing_notification = Mock(return_value=None)

        tracker = self.tracker.record_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            recipient='user@example.com',
            subject='Test',
            body='Body',
            success=True,
            commit=False
        )

        assert isinstance(tracker, NotificationTracker)
        assert tracker.user_id == 'user1'
        # send_count will be set by DB default (1), but not in Python object
        assert tracker.send_count is None or tracker.send_count == 1
        assert tracker.sent_successfully is True
        self.mock_repo.db.add.assert_called_once()

    def test_record_notification_update_existing(self):
        """Test record_notification updates existing record."""
        existing = Mock(spec=NotificationTracker)
        existing.send_count = 3
        self.tracker._get_existing_notification = Mock(return_value=existing)

        tracker = self.tracker.record_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            recipient='user@example.com',
            subject='Test',
            body='Body',
            success=False,
            error_message='Failed to send',
            commit=False
        )

        assert tracker == existing
        assert tracker.send_count == 4  # Incremented
        assert tracker.sent_successfully is False
        assert tracker.error_message == 'Failed to send'
        self.mock_repo.db.add.assert_not_called()  # No new record

    def test_record_notification_with_commit(self):
        """Test record_notification commits transaction."""
        self.tracker._get_existing_notification = Mock(return_value=None)
        self.tracker.record_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            recipient='user@example.com',
            subject='Test',
            body='Body',
            success=True,
            commit=True
        )

        self.mock_repo.db.commit.assert_called_once()

    def test_record_notification_without_commit(self):
        """Test record_notification without commit."""
        self.tracker._get_existing_notification = Mock(return_value=None)
        self.tracker.record_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            recipient='user@example.com',
            subject='Test',
            body='Body',
            success=True,
            commit=False
        )

        self.mock_repo.db.commit.assert_not_called()

    def test_record_notification_all_fields(self):
        """Test record_notification with all fields."""
        self.tracker._get_existing_notification = Mock(return_value=None)

        tracker = self.tracker.record_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='score_improved',
            channel_type='discord',
            recipient='webhook-url',
            subject='Score improved!',
            body='Your match score improved',
            success=True,
            error_message=None,
            metadata={'old_score': 75, 'new_score': 85},
            allow_resend=True,
            commit=False
        )

        assert tracker.event_type == 'score_improved'
        assert tracker.channel_type == 'discord'
        assert tracker.allow_resend is True
        assert tracker.event_data == {'old_score': 75, 'new_score': 85}


class TestShouldNotifyUser:
    """Test should_notify_user convenience function."""

    def test_should_notify_user_basic(self):
        """Test should_notify_user basic usage."""
        mock_repo = Mock()

        with patch('notification.tracker.NotificationTrackerService') as mock_tracker_class:
            mock_tracker = Mock()
            mock_tracker.should_send_notification.return_value = True
            mock_tracker_class.return_value = mock_tracker

            result = should_notify_user(
                repo=mock_repo,
                user_id='user1',
                job_match_id='match1',
                event_type='new_match',
                channel='email'
            )

            assert result is True
            mock_tracker.should_send_notification.assert_called_once()

    def test_should_notify_user_with_custom_tracker(self):
        """Test should_notify_user with provided tracker."""
        mock_repo = Mock()
        custom_tracker = Mock(spec=NotificationTrackerService)
        custom_tracker.should_send_notification.return_value = False

        result = should_notify_user(
            repo=mock_repo,
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel='email',
            tracker=custom_tracker
        )

        assert result is False
        # Should use provided tracker, not create new one

    def test_should_notify_user_default_event_type(self):
        """Test should_notify_user with default event type."""
        mock_repo = Mock()

        with patch('notification.tracker.NotificationTrackerService') as mock_tracker_class:
            mock_tracker = Mock()
            mock_tracker.should_send_notification.return_value = True
            mock_tracker_class.return_value = mock_tracker

            should_notify_user(
                repo=mock_repo,
                user_id='user1',
                job_match_id='match1'
            )

            call_args = mock_tracker.should_send_notification.call_args
            assert call_args[1]['event_type'] == 'new_match'  # Default

    def test_should_notify_user_default_channel(self):
        """Test should_notify_user with default channel."""
        mock_repo = Mock()

        with patch('notification.tracker.NotificationTrackerService') as mock_tracker_class:
            mock_tracker = Mock()
            mock_tracker.should_send_notification.return_value = True
            mock_tracker_class.return_value = mock_tracker

            should_notify_user(
                repo=mock_repo,
                user_id='user1',
                job_match_id='match1'
            )

            call_args = mock_tracker.should_send_notification.call_args
            assert call_args[1]['channel_type'] == 'email'  # Default


class TestEdgeCases:
    """Test edge cases and error handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_repo = Mock()
        self.mock_repo.db = Mock()
        self.tracker = NotificationTrackerService(self.mock_repo)

    def test_dedup_hash_special_characters(self):
        """Test dedup hash with special characters in inputs."""
        hash1 = self.tracker.generate_dedup_hash(
            'user:with:colons',
            'match-with-dashes',
            'event_with_underscores',
            'email'
        )

        assert len(hash1) == 32
        assert isinstance(hash1, str)

    def test_dedup_hash_unicode_characters(self):
        """Test dedup hash with unicode characters."""
        hash1 = self.tracker.generate_dedup_hash(
            '用户',
            'マッチ',
            '이벤트',
            '이메일'
        )

        assert len(hash1) == 32

    def test_content_hash_empty_strings(self):
        """Test content hash with empty strings."""
        hash_result = self.tracker.generate_content_hash('', '', {})

        assert len(hash_result) == 16

    def test_content_hash_very_long_subject(self):
        """Test content hash with very long subject."""
        long_subject = 'x' * 10000
        hash_result = self.tracker.generate_content_hash(long_subject, 'Body', {})

        assert len(hash_result) == 16

    def test_record_notification_unicode_data(self):
        """Test record_notification with unicode data."""
        self.tracker._get_existing_notification = Mock(return_value=None)

        tracker = self.tracker.record_notification(
            user_id='用户',
            job_match_id='マッチ',
            event_type='イベント',
            channel_type='이메일',
            recipient='user@example.com',
            subject='日本語の件名',
            body='これは本文です',
            success=True,
            metadata={'key': '値'},
            commit=False
        )

        assert tracker.user_id == '用户'
        assert tracker.event_type == 'イベント'

    def test_strategy_with_timezone_aware_datetime(self):
        """Test strategy with timezone-aware datetime."""
        strategy = DefaultDeduplicationStrategy()

        existing = Mock()
        existing.content_hash = 'hash1'
        # UTC timezone
        existing.last_sent_at = datetime.now(timezone.utc) - timedelta(hours=25)
        existing.allow_resend = True
        existing.resend_interval_hours = 24

        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='score_improved',
            channel_type='email',
            content_hash='hash1'
        )

        result = strategy.should_allow_notification(existing, event)
        assert result is True

    def test_record_notification_large_metadata(self):
        """Test record_notification with large metadata."""
        self.tracker._get_existing_notification = Mock(return_value=None)

        large_metadata = {'data': 'x' * 10000}

        tracker = self.tracker.record_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            recipient='user@example.com',
            subject='Test',
            body='Body',
            success=True,
            metadata=large_metadata,
            commit=False
        )

        assert tracker.event_data == large_metadata
