#!/usr/bin/env python3
"""
Comprehensive tests for the notification system.

Tests cover:
1. Notification channels (Email, Discord, Telegram, Webhook)
2. Deduplication tracker
3. Notification service integration
4. SOLID principles validation
5. Dynamic channel loading

Usage:
    uv run python -m pytest tests/test_notifications.py -v
    uv run python -m pytest tests/test_notifications.py::TestDynamicChannelLoading -v
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

# Test the notification channels, tracker, and service
from notification import (
    EmailChannel, DiscordChannel, TelegramChannel,
    WebhookChannel, InAppChannel, NotificationChannelFactory, NotificationChannel,
    NotificationTrackerService, DefaultDeduplicationStrategy,
    AggressiveDeduplicationStrategy, NotificationEvent,
    NotificationService, NotificationPriority
)


class TestNotificationChannels(unittest.TestCase):
    """Test individual notification channels."""
    
    def setUp(self):
        """Set up test environment."""
        # Store original env vars
        self.original_env = dict(os.environ)
    
    def tearDown(self):
        """Restore original environment."""
        os.environ.clear()
        os.environ.update(self.original_env)
    
    # ============ Email Channel Tests ============
    
    def test_email_channel_validation_missing_config(self):
        """Test email channel validation fails without config."""
        # Clear SMTP env vars
        for key in ['SMTP_SERVER', 'SMTP_PORT', 'SMTP_USERNAME', 'SMTP_PASSWORD']:
            os.environ.pop(key, None)
        
        channel = EmailChannel()
        self.assertFalse(channel.validate_config())
    
    def test_email_channel_validation_with_config(self):
        """Test email channel validation passes with config."""
        os.environ['SMTP_SERVER'] = 'smtp.gmail.com'
        os.environ['SMTP_PORT'] = '587'
        os.environ['SMTP_USERNAME'] = 'test@example.com'
        os.environ['SMTP_PASSWORD'] = 'password'
        
        channel = EmailChannel()
        self.assertTrue(channel.validate_config())
    
    @patch('notification.channels.smtplib.SMTP')
    def test_email_channel_send_success(self, mock_smtp_class):
        """Test successful email sending."""
        # Setup mocks
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__ = Mock(return_value=mock_smtp)
        mock_smtp_class.return_value.__exit__ = Mock(return_value=False)
        
        os.environ['SMTP_SERVER'] = 'smtp.gmail.com'
        os.environ['SMTP_PORT'] = '587'
        os.environ['SMTP_USERNAME'] = 'test@example.com'
        os.environ['SMTP_PASSWORD'] = 'password'
        os.environ['FROM_EMAIL'] = 'from@example.com'
        
        channel = EmailChannel()
        result = channel.send(
            recipient='to@example.com',
            subject='Test Subject',
            body='Test Body',
            metadata={}
        )
        
        self.assertTrue(result)
        mock_smtp.starttls.assert_called_once()
        mock_smtp.login.assert_called_once_with('test@example.com', 'password')
        mock_smtp.send_message.assert_called_once()
    
    def test_email_channel_send_no_config_returns_failure(self):
        """Test email sending without config returns failure."""
        # Clear config
        for key in ['SMTP_SERVER', 'SMTP_PORT', 'SMTP_USERNAME', 'SMTP_PASSWORD']:
            os.environ.pop(key, None)
        
        channel = EmailChannel()
        result = channel.send(
            recipient='to@example.com',
            subject='Test',
            body='Body',
            metadata={}
        )
        
        # Should return False when not configured
        self.assertFalse(result)
    
    # ============ Discord Channel Tests ============
    
    @patch('notification.channels.requests.post')
    def test_discord_channel_send_success(self, mock_post):
        """Test successful Discord webhook send."""
        mock_post.return_value.raise_for_status = Mock()
        mock_post.return_value.status_code = 204
        
        channel = DiscordChannel()
        result = channel.send(
            recipient='webhook-url',  # Not used, URL comes from metadata
            subject='Test Notification',
            body='This is a test message',
            metadata={
                'discord_webhook_url': 'https://discord.com/api/webhooks/test',
                'score': 85.5,
                'company': 'TechCorp'
            }
        )
        
        self.assertTrue(result)
        mock_post.assert_called_once()
        
        # Verify Discord embed format
        call_args = mock_post.call_args
        self.assertEqual(call_args[0][0], 'https://discord.com/api/webhooks/test')
        payload = call_args[1]['json']
        self.assertIn('embeds', payload)
        self.assertEqual(payload['embeds'][0]['title'], 'Test Notification')
    
    def test_discord_channel_send_no_webhook(self):
        """Test Discord sending without webhook returns failure."""
        os.environ.pop('DISCORD_WEBHOOK_URL', None)
        
        channel = DiscordChannel()
        result = channel.send(
            recipient='',
            subject='Test',
            body='Body',
            metadata={}  # No webhook in metadata either
        )
        
        self.assertFalse(result)  # Returns False when not configured
    
    @patch('notification.channels.requests.post')
    def test_discord_channel_send_failure(self, mock_post):
        """Test Discord send failure handling."""
        mock_post.side_effect = Exception('Network error')
        
        channel = DiscordChannel()
        result = channel.send(
            recipient='',
            subject='Test',
            body='Body',
            metadata={'discord_webhook_url': 'https://test.com/webhook'}
        )
        
        self.assertFalse(result)
    
    # ============ Telegram Channel Tests ============
    
    @patch('notification.channels.requests.post')
    def test_telegram_channel_send_success(self, mock_post):
        """Test successful Telegram message send."""
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'ok': True}
        
        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token-123'
        
        channel = TelegramChannel()
        result = channel.send(
            recipient='@testchannel',
            subject='Job Alert',
            body='New match found!',
            metadata={}
        )
        
        self.assertTrue(result)
        mock_post.assert_called_once()
        
        # Verify Telegram API call
        call_args = mock_post.call_args
        self.assertIn('test-token-123', call_args[0][0])
        payload = call_args[1]['json']
        self.assertEqual(payload['chat_id'], '@testchannel')
        self.assertEqual(payload['parse_mode'], 'HTML')
    
    def test_telegram_channel_send_no_token(self):
        """Test Telegram sending without token returns failure."""
        os.environ.pop('TELEGRAM_BOT_TOKEN', None)
        
        channel = TelegramChannel()
        result = channel.send(
            recipient='@channel',
            subject='Test',
            body='Body',
            metadata={}
        )
        
        self.assertFalse(result)  # Returns False when not configured
    
    @patch('notification.channels.requests.post')
    def test_telegram_channel_api_error(self, mock_post):
        """Test Telegram API error handling."""
        mock_post.return_value.status_code = 400
        mock_post.return_value.text = 'Bad Request'
        
        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'
        
        channel = TelegramChannel()
        result = channel.send(
            recipient='@channel',
            subject='Test',
            body='Body',
            metadata={}
        )
        
        self.assertFalse(result)
    
    # ============ Webhook Channel Tests ============
    
    @patch('notification.channels.requests.post')
    def test_webhook_channel_send_json_body(self, mock_post):
        """Test webhook with JSON body."""
        mock_post.return_value.raise_for_status = Mock()
        
        channel = WebhookChannel()
        result = channel.send(
            recipient='https://example.com/webhook',
            subject='',
            body=json.dumps({'event': 'test', 'data': 'value'}),
            metadata={'custom': 'header'}
        )
        
        self.assertTrue(result)
        mock_post.assert_called_once()
        
        # Verify JSON payload sent
        call_args = mock_post.call_args
        self.assertEqual(call_args[1]['json'], {'event': 'test', 'data': 'value'})
    
    @patch('notification.channels.requests.post')
    def test_webhook_channel_send_plain_body(self, mock_post):
        """Test webhook with plain text body."""
        mock_post.return_value.raise_for_status = Mock()
        
        channel = WebhookChannel()
        result = channel.send(
            recipient='https://example.com/webhook',
            subject='Subject',
            body='Plain text message',
            metadata={}
        )
        
        self.assertTrue(result)
        
        # Verify wrapped in JSON
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        self.assertEqual(payload['subject'], 'Subject')
        self.assertEqual(payload['body'], 'Plain text message')
    
    # ============ Factory Tests ============
    
    def test_factory_get_email_channel(self):
        """Test factory returns correct channel type."""
        channel = NotificationChannelFactory.get_channel('email')
        self.assertIsInstance(channel, EmailChannel)
    
    def test_factory_get_discord_channel(self):
        """Test factory returns Discord channel."""
        channel = NotificationChannelFactory.get_channel('discord')
        self.assertIsInstance(channel, DiscordChannel)
    
    def test_factory_get_telegram_channel(self):
        """Test factory returns Telegram channel."""
        channel = NotificationChannelFactory.get_channel('telegram')
        self.assertIsInstance(channel, TelegramChannel)
    
    def test_factory_unknown_channel_raises(self):
        """Test factory raises error for unknown channel."""
        with self.assertRaises(ValueError) as context:
            NotificationChannelFactory.get_channel('unknown')
        
        self.assertIn('Unknown channel type', str(context.exception))
    
    def test_factory_list_channels(self):
        """Test factory lists available channels."""
        channels = NotificationChannelFactory.list_channels()
        
        self.assertIn('email', channels)
        self.assertIn('discord', channels)
        self.assertIn('telegram', channels)
        # Slack removed - should not be in list
        self.assertNotIn('slack', channels)
    
    def test_factory_register_new_channel(self):
        """Test factory can register new channels."""
        
        class TestChannel:
            @property
            def channel_type(self):
                return 'test'
            def send(self, *args, **kwargs):
                return True
            def validate_config(self):
                return True
        
        # Register (will fail validation but tests the mechanism)
        with self.assertRaises(ValueError):
            # Should fail because TestChannel doesn't extend NotificationChannel
            NotificationChannelFactory.register_channel('test', TestChannel)


class TestDynamicChannelLoading(unittest.TestCase):
    """Test dynamic loading of custom notification channels."""
    
    def setUp(self):
        """Set up test environment."""
        self.original_env = dict(os.environ)
        # Reset factory state
        NotificationChannelFactory._custom_channels_loaded = False
    
    def tearDown(self):
        """Restore original environment."""
        os.environ.clear()
        os.environ.update(self.original_env)
        NotificationChannelFactory._custom_channels_loaded = False
    
    def test_load_channel_from_config(self):
        """Test loading custom channel from configuration."""
        # Create a temporary module file
        custom_channel_code = '''
from notification import NotificationChannel

class TestCustomChannel(NotificationChannel):
    @property
    def channel_type(self):
        return 'test_custom'
    
    def send(self, recipient, subject, body, metadata):
        return True
    
    def validate_config(self):
        return True
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(custom_channel_code)
            temp_file = f.name
        
        try:
            # Load from file
            NotificationChannelFactory._load_channel_from_file(temp_file)
            
            # Verify it was registered
            self.assertIn('test_custom', NotificationChannelFactory.list_channels())
            
            # Verify it works
            channel = NotificationChannelFactory.get_channel('test_custom')
            self.assertEqual(channel.channel_type, 'test_custom')
            
        finally:
            os.unlink(temp_file)
    
    def test_load_channel_from_config_dict(self):
        """Test loading channels from config dictionary."""
        # First create and load a temp module
        custom_channel_code = '''
from notification import NotificationChannel

class ConfigTestChannel(NotificationChannel):
    @property
    def channel_type(self):
        return 'config_test'
    
    def send(self, recipient, subject, body, metadata):
        return True
    
    def validate_config(self):
        return True
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='_channel.py', delete=False) as f:
            f.write(custom_channel_code)
            temp_file = f.name
        
        try:
            # Add temp directory to path
            import sys
            temp_dir = os.path.dirname(temp_file)
            module_name = os.path.basename(temp_file)[:-3]  # Remove .py
            
            if temp_dir not in sys.path:
                sys.path.insert(0, temp_dir)
            
            # Load via config
            custom_channels = [
                {
                    'name': 'my_custom',
                    'module': module_name,
                    'class': 'ConfigTestChannel'
                }
            ]
            
            NotificationChannelFactory.load_channels_from_config(custom_channels)
            
            # Verify it was registered with the specified name
            self.assertIn('my_custom', NotificationChannelFactory.list_channels())
            
        finally:
            if temp_dir in sys.path:
                sys.path.remove(temp_dir)
            os.unlink(temp_file)
    
    @patch('notification.channels._validate_channel_file_path')
    def test_load_channel_from_environment_variable(self, mock_validate):
        """Test loading channel from NOTIFICATION_CHANNEL_PATH env var."""
        # Mock validation to always pass for test files
        mock_validate.return_value = True
        
        custom_channel_code = '''
from notification import NotificationChannel

class EnvTestChannel(NotificationChannel):
    @property
    def channel_type(self):
        return 'env_test'
    
    def send(self, recipient, subject, body, metadata):
        return True
    
    def validate_config(self):
        return True
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(custom_channel_code)
            temp_file = f.name
        
        try:
            # Set environment variable
            os.environ['NOTIFICATION_CHANNEL_PATH'] = temp_file
            NotificationChannelFactory._custom_channels_loaded = False
            
            # Force reload by calling list_channels
            channels = NotificationChannelFactory.list_channels()
            
            # Verify custom channel was loaded
            self.assertIn('env_test', channels)
            
        finally:
            os.unlink(temp_file)


class TestNotificationTracker(unittest.TestCase):
    """Test notification deduplication tracker."""
    
    def setUp(self):
        """Set up mock repository."""
        self.mock_repo = Mock()
        self.mock_repo.db = Mock()
        self.tracker = NotificationTrackerService(self.mock_repo)
    
    def test_generate_dedup_hash_consistency(self):
        """Test dedup hash is consistent for same inputs."""
        hash1 = self.tracker.generate_dedup_hash('user1', 'match1', 'new_match', 'email')
        hash2 = self.tracker.generate_dedup_hash('user1', 'match1', 'new_match', 'email')
        
        self.assertEqual(hash1, hash2)
        self.assertEqual(len(hash1), 32)  # SHA-256 truncated
    
    def test_generate_dedup_hash_different_inputs(self):
        """Test different inputs produce different hashes."""
        hash1 = self.tracker.generate_dedup_hash('user1', 'match1', 'new_match', 'email')
        hash2 = self.tracker.generate_dedup_hash('user1', 'match1', 'new_match', 'discord')
        hash3 = self.tracker.generate_dedup_hash('user2', 'match1', 'new_match', 'email')
        
        self.assertNotEqual(hash1, hash2)  # Different channel
        self.assertNotEqual(hash1, hash3)  # Different user
    
    def test_generate_content_hash(self):
        """Test content hash generation."""
        hash1 = self.tracker.generate_content_hash('Subject', 'Body text', {'key': 'value'})
        hash2 = self.tracker.generate_content_hash('Subject', 'Body text', {'key': 'value'})
        hash3 = self.tracker.generate_content_hash('Different', 'Body text', {'key': 'value'})
        
        self.assertEqual(hash1, hash2)  # Same content = same hash
        self.assertNotEqual(hash1, hash3)  # Different subject = different hash
    
    def test_default_strategy_allows_first_notification(self):
        """Test strategy allows first notification."""
        strategy = DefaultDeduplicationStrategy()
        
        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            content_hash='hash1'
        )
        
        result = strategy.should_allow_notification(None, event)
        self.assertTrue(result)
    
    def test_default_strategy_blocks_exact_duplicate(self):
        """Test strategy blocks exact duplicate."""
        strategy = DefaultDeduplicationStrategy()
        
        existing = Mock()
        existing.content_hash = 'hash1'
        existing.last_sent_at = datetime.now(timezone.utc)
        existing.allow_resend = False
        
        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            content_hash='hash1'
        )
        
        result = strategy.should_allow_notification(existing, event)
        self.assertFalse(result)
    
    def test_default_strategy_allows_content_change(self):
        """Test strategy allows resend if content changed."""
        strategy = DefaultDeduplicationStrategy()
        
        existing = Mock()
        existing.content_hash = 'hash1'
        existing.last_sent_at = datetime.now(timezone.utc)
        
        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            content_hash='hash2'  # Different content
        )
        
        result = strategy.should_allow_notification(existing, event)
        self.assertTrue(result)
    
    def test_default_strategy_allows_resendable_events_after_interval(self):
        """Test strategy allows resend for resendable event types after interval."""
        strategy = DefaultDeduplicationStrategy(default_interval_hours=24)
        
        existing = Mock()
        existing.content_hash = 'hash1'
        existing.last_sent_at = datetime.now(timezone.utc) - timedelta(hours=25)  # Over 24h ago
        existing.allow_resend = True
        existing.resend_interval_hours = 24
        
        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='score_improved',  # Resendable event
            channel_type='email',
            content_hash='hash1'
        )
        
        result = strategy.should_allow_notification(existing, event)
        self.assertTrue(result)
    
    def test_aggressive_strategy_blocks_all_resends(self):
        """Test aggressive strategy never allows resend."""
        strategy = AggressiveDeduplicationStrategy()
        
        existing = Mock()
        existing.last_sent_at = datetime.now(timezone.utc) - timedelta(days=30)
        
        event = NotificationEvent(
            user_id='user1',
            job_match_id='match1',
            event_type='score_improved',
            channel_type='email'
        )
        
        # Should block even with old notification
        result = strategy.should_allow_notification(existing, event)
        self.assertFalse(result)
        
        # Should allow if no existing
        result = strategy.should_allow_notification(None, event)
        self.assertTrue(result)
    
    @patch('notification.tracker.NotificationTrackerService._get_existing_notification')
    def test_should_send_notification_new_event(self, mock_get_existing):
        """Test allows new notifications."""
        mock_get_existing.return_value = None
        
        result = self.tracker.should_send_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            subject='Test',
            body='Body'
        )
        
        self.assertTrue(result)
    
    @patch('notification.tracker.NotificationTrackerService._get_existing_notification')
    @patch.object(NotificationTrackerService, 'generate_content_hash')
    def test_should_send_notification_duplicate_blocked(self, mock_gen_hash, mock_get_existing):
        """Test blocks duplicate notifications."""
        existing = Mock()
        existing.content_hash = 'existing_hash'
        existing.last_sent_at = datetime.now(timezone.utc)
        existing.allow_resend = False
        mock_get_existing.return_value = existing
        
        # Ensure content hash matches existing
        mock_gen_hash.return_value = 'existing_hash'
        
        result = self.tracker.should_send_notification(
            user_id='user1',
            job_match_id='match1',
            event_type='new_match',
            channel_type='email',
            subject='Test',
            body='Body'
        )
        
        self.assertFalse(result)


class TestNotificationService(unittest.TestCase):
    """Test notification service integration."""

    def setUp(self):
        """Set up mock repository."""
        self.original_env = dict(os.environ)
        self.mock_repo = Mock()
        self.mock_repo.db = Mock()

    def tearDown(self):
        """Restore original environment."""
        os.environ.clear()
        os.environ.update(self.original_env)
    
    @patch('notification.service.NotificationTrackerService')
    @patch('notification.service.Queue')
    @patch('notification.service.Redis')
    def test_send_notification_new_event(self, mock_redis, mock_queue_class, mock_tracker_class):
        """Test sending notification for new event."""
        # Setup mocks
        mock_tracker = Mock()
        mock_tracker.should_send_notification.return_value = True
        mock_tracker_class.return_value = mock_tracker
        
        mock_queue = Mock()
        mock_job = Mock()
        mock_job.id = 'job-123'
        mock_queue.enqueue.return_value = mock_job
        mock_queue_class.return_value = mock_queue
        
        mock_redis.from_url.return_value.ping.return_value = True
        
        service = NotificationService(self.mock_repo)
        
        result = service.send_notification(
            channel_type='email',
            recipient='test@example.com',
            subject='Test',
            body='Body',
            user_id='user1',
            job_match_id='match1',
            event_type='new_match'
        )
        
        self.assertIsNotNone(result)
        mock_queue.enqueue.assert_called_once()
    
    @patch('notification.service.NotificationTrackerService')
    def test_send_notification_duplicate_suppressed(self, mock_tracker_class):
        """Test duplicate notification is suppressed."""
        mock_tracker = Mock()
        mock_tracker.should_send_notification.return_value = False  # Duplicate
        mock_tracker_class.return_value = mock_tracker
        
        service = NotificationService(self.mock_repo)
        
        result = service.send_notification(
            channel_type='email',
            recipient='test@example.com',
            subject='Test',
            body='Body',
            user_id='user1',
            job_match_id='match1',
            event_type='new_match'
        )
        
        self.assertIsNone(result)  # Suppressed
    
    @patch('notification.service.NotificationService.send_notification')
    def test_notify_new_match_single_channel(self, mock_send):
        """Test notifying about new match."""
        mock_send.return_value = 'notif-123'
        
        service = NotificationService(self.mock_repo)
        service.notify_new_match(
            user_id='user1',
            match_id='match1',
            job_title='Python Developer',
            company='TechCorp',
            score=85.5,
            channels=['email']
        )
        
        mock_send.assert_called_once()
        call_args = mock_send.call_args[1]
        self.assertEqual(call_args['channel_type'], 'email')
        self.assertEqual(call_args['event_type'], 'new_high_score_match')
        self.assertIn('Python Developer', call_args['subject'])
    
    @patch('notification.service.NotificationService.send_notification')
    def test_notify_new_match_multiple_channels(self, mock_send):
        """Test notifying on multiple channels."""
        mock_send.return_value = 'notif-123'
        
        os.environ['DISCORD_WEBHOOK_URL'] = 'https://discord.com/webhook'
        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'
        
        service = NotificationService(self.mock_repo)
        results = service.notify_new_match(
            user_id='user1',
            match_id='match1',
            job_title='Python Developer',
            company='TechCorp',
            score=85.5,
            channels=['email', 'discord', 'telegram']
        )
        
        self.assertEqual(mock_send.call_count, 3)
        self.assertEqual(len(results), 3)
    
    def test_priority_levels(self):
        """Test priority enum values."""
        self.assertEqual(NotificationPriority.LOW.value, 'low')
        self.assertEqual(NotificationPriority.NORMAL.value, 'normal')
        self.assertEqual(NotificationPriority.HIGH.value, 'high')
        self.assertEqual(NotificationPriority.URGENT.value, 'urgent')


class TestSOLIDPrinciples(unittest.TestCase):
    """Validate SOLID principles in notification system."""
    
    def test_single_responsibility_channels(self):
        """Test each channel has single responsibility."""
        # Each channel should only implement send() and validate_config()
        # No other business logic should be in channels
        for channel_class in [EmailChannel, DiscordChannel, TelegramChannel]:
            channel = channel_class()
            # Should have channel_type property
            self.assertTrue(hasattr(channel, 'channel_type'))
            # Should have send method
            self.assertTrue(hasattr(channel, 'send'))
            # Should have validate_config method
            self.assertTrue(hasattr(channel, 'validate_config'))
    
    def test_open_closed_principle(self):
        """Test new channels can be added without modifying existing code."""
        # Define a new channel class
        class NewChannel:
            @property
            def channel_type(self):
                return 'new_channel'
            
            def send(self, recipient, subject, body, metadata):
                return True
            
            def validate_config(self):
                return True
        
        # Should be able to register (will fail validation but tests the mechanism)
        with self.assertRaises(ValueError):
            # Fails because NewChannel doesn't extend NotificationChannel
            NotificationChannelFactory.register_channel('new_channel', NewChannel)
    
    def test_liskov_substitution_channels(self):
        """Test all channels are interchangeable."""
        channels = [
            EmailChannel(),
            DiscordChannel(),
            TelegramChannel(),
            WebhookChannel(),
            InAppChannel()
        ]
        
        # All should have same interface
        for channel in channels:
            # All should have channel_type
            self.assertTrue(hasattr(channel, 'channel_type'))
            self.assertIsInstance(channel.channel_type, str)
            
            # All should be callable with same signature
            # (We won't actually call to avoid network requests)
            import inspect
            send_signature = inspect.signature(channel.send)
            params = list(send_signature.parameters.keys())
            self.assertIn('recipient', params)
            self.assertIn('subject', params)
            self.assertIn('body', params)
    
    def test_interface_segregation(self):
        """Test interfaces are focused and minimal."""
        # NotificationChannel should be minimal
        import inspect
        channel_methods = [name for name, _ in inspect.getmembers(
            NotificationChannel, predicate=inspect.isfunction
        ) if not name.startswith('_')]
        
        # Should only have send and validate_config
        self.assertIn('send', channel_methods)
        self.assertIn('validate_config', channel_methods)
        self.assertIn('channel_type', [name for name, _ in inspect.getmembers(
            NotificationChannel, predicate=inspect.isdatadescriptor
        )])
    
    def test_dependency_inversion(self):
        """Test high-level modules depend on abstractions."""
        # NotificationService should work with any channel
        # (We can't easily test this without real setup, but we verify the design)
        
        # Factory returns NotificationChannel interface
        channel = NotificationChannelFactory.get_channel('email')
        # Should be usable as NotificationChannel
        self.assertTrue(hasattr(channel, 'send'))
        self.assertTrue(hasattr(channel, 'channel_type'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
