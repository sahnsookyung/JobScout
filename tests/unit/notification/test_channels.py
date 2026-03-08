#!/usr/bin/env python3
"""
Tests for notification channels - focusing on uncovered functionality.

Tests cover:
1. Rate limit parsing (Discord, Telegram)
2. URL validation and SSRF protection
3. HTML sanitization functions
4. Rich content builders (HTML email, Telegram messages, Discord embeds)
5. Dry-run mode testing
6. Email masking for logs
7. NotificationRateLimiter class
"""

import os
import time
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
import pytest

from notification.channels import (
    DiscordChannel, TelegramChannel, EmailChannel, WebhookChannel,
    RateLimitException,
    _validate_webhook_url, _sanitize_url, _escape_html, _is_dry_run_mode,
    _mask_email, _validate_channel_file_path,
    NotificationChannelFactory, InAppChannel
)
from notification.service import NotificationRateLimiter
from notification.message_builder import JobNotificationContent, JobInfo, MatchInfo, RequirementsInfo


class TestRateLimitParsing:
    """Test rate limit response parsing for various APIs."""

    def test_discord_rate_limit_json_body(self):
        """Test parsing rate limit from Discord JSON body."""
        channel = DiscordChannel()

        # Mock response with retry_after in body
        mock_response = Mock()
        mock_response.json.return_value = {'retry_after': 30}
        mock_response.headers = {}

        retry_after = channel._parse_rate_limit_response(mock_response)
        assert retry_after == 30

    def test_discord_rate_limit_fractional_seconds(self):
        """Test parsing rate limit with fractional seconds."""
        channel = DiscordChannel()

        mock_response = Mock()
        mock_response.json.side_effect = Exception("No JSON")
        mock_response.headers = {'X-RateLimit-Reset-After': '2.5'}

        retry_after = channel._parse_rate_limit_response(mock_response)
        assert retry_after == 2  # Should floor to 2 seconds

    def test_discord_rate_limit_retry_after_header(self):
        """Test parsing rate limit from Retry-After header."""
        channel = DiscordChannel()

        mock_response = Mock()
        mock_response.json.side_effect = Exception("No JSON")
        mock_response.headers = {
            'X-RateLimit-Reset-After': None,
            'Retry-After': '45'
        }

        retry_after = channel._parse_rate_limit_response(mock_response)
        assert retry_after == 45

    def test_discord_rate_limit_default_fallback(self, caplog):
        """Test default fallback when no rate limit info available."""
        channel = DiscordChannel()

        mock_response = Mock()
        mock_response.json.side_effect = Exception("No JSON")
        mock_response.headers = {}

        retry_after = channel._parse_rate_limit_response(mock_response)
        assert retry_after == 60  # Default fallback
        assert "Could not parse Discord rate limit" in caplog.text

    def test_telegram_rate_limit_json_body(self):
        """Test parsing rate limit from Telegram JSON body."""
        channel = TelegramChannel()

        # Mock response with retry_after in parameters
        mock_response = Mock()
        mock_response.json.return_value = {
            'ok': False,
            'parameters': {'retry_after': 120}
        }

        retry_after = channel._parse_rate_limit_response(mock_response)
        assert retry_after == 120

    def test_telegram_rate_limit_description_fallback(self):
        """Test parsing rate limit from description field fallback."""
        channel = TelegramChannel()

        mock_response = Mock()
        mock_response.json.return_value = {
            'ok': False,
            'retry_after': 90
        }

        retry_after = channel._parse_rate_limit_response(mock_response)
        assert retry_after == 90

    def test_telegram_rate_limit_default_fallback(self, caplog):
        """Test default fallback for Telegram."""
        channel = TelegramChannel()

        mock_response = Mock()
        mock_response.json.side_effect = Exception("No JSON")

        retry_after = channel._parse_rate_limit_response(mock_response)
        assert retry_after == 60  # Default fallback
        assert "Could not parse Telegram rate limit" in caplog.text


class TestURLValidation:
    """Test webhook URL validation and SSRF protection."""

    def test_validate_webhook_url_valid_https(self):
        """Test valid HTTPS URL passes validation."""
        assert _validate_webhook_url('https://example.com/webhook') is True

    def test_validate_webhook_url_valid_http(self):
        """Test valid HTTP URL passes validation."""
        assert _validate_webhook_url('http://example.com/webhook') is True

    def test_validate_webhook_url_invalid_scheme(self):
        """Test invalid schemes are rejected."""
        assert _validate_webhook_url('ftp://example.com/file') is False
        assert _validate_webhook_url('file:///etc/passwd') is False
        assert _validate_webhook_url('javascript:alert(1)') is False

    def test_validate_webhook_url_missing_hostname(self):
        """Test URLs without hostname are rejected."""
        assert _validate_webhook_url('https:///path') is False

    def test_validate_webhook_url_private_ip_loopback(self):
        """Test private/loopback IPs are rejected (SSRF protection)."""
        assert _validate_webhook_url('http://127.0.0.1/webhook') is False
        assert _validate_webhook_url('http://localhost/webhook') is False

    def test_validate_webhook_url_private_ip_range(self):
        """Test private IP ranges are rejected."""
        assert _validate_webhook_url('http://192.168.1.1/webhook') is False
        assert _validate_webhook_url('http://10.0.0.1/webhook') is False
        assert _validate_webhook_url('http://172.16.0.1/webhook') is False

    def test_validate_webhook_url_invalid_hostname(self, caplog):
        """Test unresolvable hostnames are rejected."""
        result = _validate_webhook_url('https://invalid-hostname-that-does-not-exist.com/webhook')
        assert result is False
        assert "Could not resolve hostname" in caplog.text


class TestHTMLSanitization:
    """Test HTML sanitization and escaping functions."""

    def test_sanitize_url_valid(self):
        """Test valid URL is sanitized correctly."""
        result = _sanitize_url('https://example.com/path?query=value')
        assert result is not None
        assert 'https://example.com' in result

    def test_sanitize_url_invalid_scheme(self):
        """Test invalid scheme returns None."""
        result = _sanitize_url('javascript:alert(1)')
        assert result is None

    def test_sanitize_url_malformed(self):
        """Test malformed URL returns None."""
        result = _sanitize_url('not-a-url')
        assert result is None

    def test_escape_html_basic(self):
        """Test basic HTML escaping."""
        assert _escape_html('<script>alert(1)</script>') == '&lt;script&gt;alert(1)&lt;/script&gt;'

    def test_escape_html_quotes(self):
        """Test quote escaping."""
        assert _escape_html('"quotes"') == '&quot;quotes&quot;'
        assert _escape_html("'single'") == '&#x27;single&#x27;'

    def test_escape_html_ampersand(self):
        """Test ampersand escaping."""
        assert _escape_html('A & B') == 'A &amp; B'

    def test_escape_html_combined(self):
        """Test combined special characters."""
        input_text = '<div onclick="alert(\'XSS\')">Hello & World</div>'
        expected = '&lt;div onclick=&quot;alert(&#x27;XSS&#x27;)&quot;&gt;Hello &amp; World&lt;/div&gt;'
        assert _escape_html(input_text) == expected


class TestDryRunMode:
    """Test dry-run mode functionality."""

    def test_is_dry_run_mode_disabled(self):
        """Test dry-run mode is disabled by default."""
        # Ensure env var is not set
        os.environ.pop('NOTIFICATION_DRY_RUN', None)
        assert _is_dry_run_mode() is False

    def test_is_dry_run_mode_enabled_true(self):
        """Test dry-run mode enabled with 'true'."""
        os.environ['NOTIFICATION_DRY_RUN'] = 'true'
        assert _is_dry_run_mode() is True

    def test_is_dry_run_mode_enabled_1(self):
        """Test dry-run mode enabled with '1'."""
        os.environ['NOTIFICATION_DRY_RUN'] = '1'
        assert _is_dry_run_mode() is True

    def test_is_dry_run_mode_enabled_yes(self):
        """Test dry-run mode enabled with 'yes'."""
        os.environ['NOTIFICATION_DRY_RUN'] = 'yes'
        assert _is_dry_run_mode() is True

    def test_is_dry_run_mode_case_insensitive(self):
        """Test dry-run mode is case insensitive."""
        os.environ['NOTIFICATION_DRY_RUN'] = 'TRUE'
        assert _is_dry_run_mode() is True

        os.environ['NOTIFICATION_DRY_RUN'] = 'Yes'
        assert _is_dry_run_mode() is True


class TestEmailMasking:
    """Test email masking for PII-safe logging."""

    def test_mask_email_basic(self):
        """Test basic email masking."""
        assert _mask_email('user@example.com') == '***@example.com'

    def test_mask_email_subaddress(self):
        """Test email with subaddress masking."""
        assert _mask_email('user+tag@example.com') == '***@example.com'

    def test_mask_email_invalid_format(self):
        """Test invalid email format masking."""
        assert _mask_email('not-an-email') == '***'

    def test_mask_email_empty(self):
        """Test empty string masking."""
        assert _mask_email('') == '***'


class TestChannelFilePathValidation:
    """Test custom channel file path validation."""

    def test_validate_path_within_allowed_dir(self):
        """Test valid path within allowed directory."""
        # Test with a path that would be valid if it existed
        with patch('os.path.abspath') as mock_abspath, \
             patch('os.path.isfile') as mock_isfile:
            mock_abspath.side_effect = lambda x: x
            mock_isfile.return_value = True

            result = _validate_channel_file_path('/app/channels/custom.py')
            assert result is True

    def test_validate_path_outside_allowed_dirs(self, caplog):
        """Test path outside allowed directories is rejected."""
        with patch('os.path.abspath') as mock_abspath, \
             patch('os.path.isfile') as mock_isfile:
            mock_abspath.side_effect = lambda x: x
            mock_isfile.return_value = True

            result = _validate_channel_file_path('/tmp/malicious.py')
            assert result is False
            assert "not in allowed directories" in caplog.text

    def test_validate_path_does_not_exist(self, caplog):
        """Test non-existent file is rejected."""
        with patch('os.path.isfile') as mock_isfile:
            mock_isfile.return_value = False

            result = _validate_channel_file_path('/app/channels/nonexistent.py')
            assert result is False
            assert "does not exist" in caplog.text


class TestNotificationRateLimiter:
    """Test rate limiter with Redis coordination."""

    def test_rate_limiter_init(self):
        """Test rate limiter initialization."""
        limiter = NotificationRateLimiter(
            redis_url='redis://localhost:6379/0',
            max_wait_seconds=300
        )
        assert limiter.redis_url == 'redis://localhost:6379/0'
        assert limiter.max_wait_seconds == 300
        assert limiter._redis is None  # Lazy init

    @patch('notification.service.Redis')
    def test_set_rate_limit(self, mock_redis_class):
        """Test setting rate limit."""
        mock_redis = Mock()
        mock_redis_class.from_url.return_value = mock_redis

        limiter = NotificationRateLimiter()
        limiter.set_rate_limit('discord', 60)

        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args[0]
        assert call_args[0] == 'notification:rate_limit:discord'
        assert call_args[1] > 60  # Includes buffer

    @patch('notification.service.Redis')
    def test_get_wait_time_no_limit(self, mock_redis_class):
        """Test get wait time when no limit set."""
        mock_redis = Mock()
        mock_redis.get.return_value = None
        mock_redis_class.from_url.return_value = mock_redis

        limiter = NotificationRateLimiter()
        wait_time = limiter.get_wait_time('discord')

        assert wait_time == 0

    @patch('notification.service.Redis')
    @patch('notification.service.time')
    def test_get_wait_time_active_limit(self, mock_time, mock_redis_class):
        """Test get wait time with active limit."""
        mock_redis = Mock()
        mock_redis.get.return_value = b'1735689600.0'  # Future timestamp
        mock_redis_class.from_url.return_value = mock_redis
        mock_time.time.return_value = 1735689500.0  # Current time (100s before)

        limiter = NotificationRateLimiter()
        wait_time = limiter.get_wait_time('discord')

        assert wait_time > 0

    @patch('notification.service.Redis')
    def test_get_wait_time_redis_unavailable(self, mock_redis_class):
        """Test get wait time when Redis is unavailable."""
        mock_redis_class.from_url.return_value = None

        limiter = NotificationRateLimiter()
        wait_time = limiter.get_wait_time('discord')

        assert wait_time == 0

    @patch('notification.service.Redis')
    @patch('notification.service.time')
    def test_is_wait_exceeded(self, mock_time, mock_redis_class):
        """Test is_wait_exceeded check."""
        mock_redis = Mock()
        # Set wait time to 350 seconds (exceeds default max_wait_seconds=300)
        mock_redis.get.return_value = b'1735690000.0'
        mock_redis_class.from_url.return_value = mock_redis
        mock_time.time.return_value = 1735689500.0

        limiter = NotificationRateLimiter(max_wait_seconds=300)
        assert limiter.is_wait_exceeded('discord') is True


class TestInAppChannel:
    """Test in-app notification channel."""

    def test_in_app_channel_type(self):
        """Test in-app channel type identifier."""
        channel = InAppChannel()
        assert channel.channel_type == 'in_app'

    @patch('notification.channels.logger')
    def test_in_app_channel_send(self, mock_logger):
        """Test in-app channel send logs notification."""
        channel = InAppChannel()
        result = channel.send(
            recipient='user123',
            subject='Test Subject',
            body='Test Body',
            metadata={}
        )

        assert result is True
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert '[IN_APP]' in call_args
        assert 'Test Subject' in call_args


class TestWebhookChannelRichContent:
    """Test webhook channel with rich job notification content."""

    @patch('notification.channels.requests.post')
    def test_webhook_send_with_job_contents(self, mock_post):
        """Test webhook sends rich payload with job contents."""
        mock_post.return_value.raise_for_status = Mock()

        channel = WebhookChannel()
        result = channel.send(
            recipient='https://example.com/webhook',
            subject='Job Alert',
            body='',
            metadata={
                'job_contents': [
                    {
                        'job': {'title': 'Developer', 'company': 'TechCorp'},
                        'match': {'overall_score': 85},
                        'requirements': {'total': 10, 'matched': 8}
                    }
                ],
                'user_id': 'user123'
            }
        )

        assert result is True
        call_args = mock_post.call_args[1]['json']
        assert call_args['type'] == 'job_notifications'
        assert len(call_args['jobs']) == 1
        assert call_args['jobs'][0]['job']['title'] == 'Developer'

    @patch('notification.channels.requests.post')
    def test_webhook_send_with_invalid_url(self, mock_post, caplog):
        """Test webhook send with invalid URL is rejected."""
        # Mock URL validation to fail
        with patch('notification.channels._validate_webhook_url', return_value=False):
            channel = WebhookChannel()
            result = channel.send(
                recipient='http://127.0.0.1/invalid',
                subject='Test',
                body='Body',
                metadata={}
            )

            assert result is False
            assert "Invalid or unsafe webhook URL" in caplog.text


class TestDiscordChannelRichContent:
    """Test Discord channel with rich job notification content."""

    @patch('notification.channels.requests.post')
    def test_discord_send_with_job_contents(self, mock_post):
        """Test Discord sends rich embeds with job contents."""
        mock_response = Mock()
        mock_response.status_code = 204
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        channel = DiscordChannel()
        result = channel.send(
            recipient='',
            subject='Job Alert',
            body='',
            metadata={
                'discord_webhook_url': 'https://discord.com/api/webhooks/test/test',
                'job_contents': [
                    {
                        'job': {
                            'title': 'Senior Developer',
                            'company': 'TechCorp',
                            'location': 'San Francisco, CA',
                            'is_remote': True,
                            'salary': '$150k - $200k',
                            'job_type': 'Full-time',
                            'description': 'We are hiring...'
                        },
                        'match': {
                            'overall_score': 92,
                            'fit_score': 88,
                            'want_score': 85,
                            'required_coverage': 0.9
                        },
                        'requirements': {
                            'total': 10,
                            'matched': 9,
                            'key_matches': ['Python', 'React']
                        },
                        'apply_url': 'https://example.com/apply'
                    }
                ],
                'match_id': 'match-123',
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        )

        assert result is True
        call_args = mock_post.call_args[1]['json']
        assert 'embeds' in call_args
        assert len(call_args['embeds']) >= 1

        embed = call_args['embeds'][0]
        assert 'Senior Developer' in embed['title']
        assert embed['color'] == 0x28A745  # Green for high score

    @patch('notification.channels.requests.post')
    def test_discord_send_rate_limit_exception(self, mock_post):
        """Test Discord rate limit raises RateLimitException."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.json.return_value = {'retry_after': 30}
        mock_post.return_value = mock_response

        channel = DiscordChannel()

        with pytest.raises(RateLimitException) as exc_info:
            channel.send(
                recipient='',
                subject='Test',
                body='Body',
                metadata={'discord_webhook_url': 'https://discord.com/webhook'}
            )

        assert 'rate limited' in str(exc_info.value).lower()
        assert exc_info.value.retry_after == 30


class TestTelegramChannelRichContent:
    """Test Telegram channel with rich job notification content."""

    @patch('notification.channels.requests.post')
    def test_telegram_send_with_job_contents(self, mock_post):
        """Test Telegram sends rich HTML message with job contents."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'ok': True}
        mock_post.return_value = mock_response

        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'

        channel = TelegramChannel()
        result = channel.send(
            recipient='@channel',
            subject='Job Alert',
            body='',
            metadata={
                'job_contents': [
                    {
                        'job': {
                            'title': 'Python Developer',
                            'company': 'TechCorp',
                            'location': 'Remote',
                            'is_remote': True,
                            'salary': '$120k+'
                        },
                        'match': {
                            'overall_score': 88,
                            'fit_score': 85,
                            'want_score': 80,
                            'required_coverage': 0.85
                        },
                        'requirements': {
                            'total': 8,
                            'matched': 7
                        },
                        'apply_url': 'https://example.com/apply'
                    }
                ],
                'match_id': 'match-456'
            }
        )

        assert result is True
        call_args = mock_post.call_args[1]['json']
        assert 'parse_mode' in call_args
        assert call_args['parse_mode'] == 'HTML'
        assert '<b>' in call_args['text']  # HTML formatting

    @patch('notification.channels.requests.post')
    def test_telegram_send_message_truncation(self, mock_post):
        """Test Telegram message is truncated if too long."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'

        # Create very long message
        long_body = 'x' * 5000

        channel = TelegramChannel()
        channel.send(
            recipient='@channel',
            subject='Test',
            body=long_body,
            metadata={}
        )

        call_args = mock_post.call_args[1]['json']
        assert len(call_args['text']) <= 4096

    @patch('notification.channels.requests.post')
    def test_telegram_send_rate_limit_exception(self, mock_post):
        """Test Telegram rate limit raises RateLimitException."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.json.return_value = {
            'ok': False,
            'parameters': {'retry_after': 60}
        }
        mock_post.return_value = mock_response

        os.environ['TELEGRAM_BOT_TOKEN'] = 'test-token'

        channel = TelegramChannel()

        with pytest.raises(RateLimitException) as exc_info:
            channel.send(
                recipient='@channel',
                subject='Test',
                body='Body',
                metadata={}
            )

        assert 'rate limited' in str(exc_info.value).lower()
        assert exc_info.value.retry_after == 60


class TestEmailChannelRichContent:
    """Test email channel with rich HTML job notifications."""

    @patch('notification.channels.smtplib.SMTP')
    def test_email_send_with_job_contents(self, mock_smtp_class):
        """Test email sends rich HTML with job contents."""
        mock_smtp = Mock()
        mock_smtp_class.return_value.__enter__ = Mock(return_value=mock_smtp)
        mock_smtp_class.return_value.__exit__ = Mock(return_value=False)

        os.environ.update({
            'SMTP_SERVER': 'smtp.gmail.com',
            'SMTP_PORT': '587',
            'SMTP_USERNAME': 'test@example.com',
            'SMTP_PASSWORD': 'password',
            'FROM_EMAIL': 'noreply@jobscout.app'
        })

        channel = EmailChannel()
        result = channel.send(
            recipient='user@example.com',
            subject='Job Alert',
            body='',
            metadata={
                'job_contents': [
                    {
                        'job': {
                            'title': 'Senior Engineer',
                            'company': 'TechCorp',
                            'location': 'San Francisco, CA',
                            'is_remote': True,
                            'salary': '$180k - $220k',
                            'job_type': 'Full-time',
                            'job_level': 'Senior'
                        },
                        'match': {
                            'overall_score': 90,
                            'fit_score': 88,
                            'want_score': 85,
                            'required_coverage': 0.9
                        },
                        'requirements': {
                            'total': 10,
                            'matched': 9
                        },
                        'apply_url': 'https://example.com/apply'
                    }
                ],
                'match_id': 'match-789'
            }
        )

        assert result is True
        mock_smtp.send_message.assert_called_once()

        # Verify HTML content was sent
        msg = mock_smtp.send_message.call_args[0][0]
        assert msg.is_multipart()
        # Get HTML part
        html_content = None
        for part in msg.walk():
            if part.get_content_type() == 'text/html':
                html_content = part.get_payload()
                break
        
        assert html_content is not None
        # Content may be base64 encoded, so check for key terms
        import base64
        try:
            # Try to decode if base64
            decoded = base64.b64decode(html_content).decode('utf-8')
            assert '<html>' in decoded
            assert 'Senior Engineer' in decoded
        except Exception:
            # If not base64, check directly
            assert '<html>' in html_content
            assert 'Senior Engineer' in html_content


class TestNotificationChannelFactory:
    """Test notification channel factory methods."""

    def test_list_channels(self):
        """Test listing available channel types."""
        channels = NotificationChannelFactory.list_channels()
        assert 'email' in channels
        assert 'discord' in channels
        assert 'telegram' in channels
        assert 'webhook' in channels
        assert 'in_app' in channels
        assert 'slack' not in channels  # Removed

    def test_get_channel_email(self):
        """Test getting email channel."""
        channel = NotificationChannelFactory.get_channel('email')
        assert isinstance(channel, EmailChannel)

    def test_get_channel_discord(self):
        """Test getting discord channel."""
        channel = NotificationChannelFactory.get_channel('discord')
        assert isinstance(channel, DiscordChannel)

    def test_get_channel_telegram(self):
        """Test getting telegram channel."""
        channel = NotificationChannelFactory.get_channel('telegram')
        assert isinstance(channel, TelegramChannel)

    def test_get_channel_webhook(self):
        """Test getting webhook channel."""
        channel = NotificationChannelFactory.get_channel('webhook')
        assert isinstance(channel, WebhookChannel)

    def test_get_channel_in_app(self):
        """Test getting in-app channel."""
        channel = NotificationChannelFactory.get_channel('in_app')
        assert isinstance(channel, InAppChannel)

    def test_get_channel_unknown(self):
        """Test getting unknown channel raises error."""
        with pytest.raises(ValueError, match='Unknown channel type'):
            NotificationChannelFactory.get_channel('unknown_channel')
