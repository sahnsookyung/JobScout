#!/usr/bin/env python3
"""
Notification Channels - SOLID Implementation

Provides extensible notification channel implementations following SOLID principles:
- Single Responsibility: Each channel handles one type of notification
- Open/Closed: New channels can be added without modifying existing code
- Liskov Substitution: All channels implement the same interface
- Interface Segregation: Clean, focused interfaces
- Dependency Inversion: High-level modules depend on abstractions

Usage:
    from notification.channels import NotificationChannelFactory
    
    # Get channel by type
    channel = NotificationChannelFactory.get_channel('discord')
    channel.send(recipient, subject, body, metadata)
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import logging
import json
import os
import html

import requests
import smtplib
from email.mime.text import MIMEText


class RateLimitException(Exception):
    """Exception raised when a rate limit is encountered (e.g., HTTP 429)."""
    
    def __init__(self, message: str, retry_after: Optional[int] = None):
        super().__init__(message)
        self.retry_after = retry_after  # Seconds to wait before retrying
from email.mime.multipart import MIMEMultipart
import urllib.parse
import ipaddress
import socket

from core.auth import _current_environment
from notification.exceptions import NotificationConfigurationError, TerminalNotificationError, TransientNotificationError
from notification.message_builder import NotificationMessageBuilder, JobNotificationContent
from notification.runtime_config import get_notification_runtime_config

logger = logging.getLogger(__name__)

# Allowed directories for custom notification channels (security)
ALLOWED_CHANNEL_DIRS = [
    '/app/channels',
    '/etc/jobscout/channels',
    os.path.expanduser('~/.jobscout/channels'),
]


def _validate_channel_file_path(file_path: str) -> bool:
    """
    Validate that a channel file path is within allowed directories.
    
    Prevents loading arbitrary Python files from untrusted locations.
    """
    try:
        # Resolve to absolute path
        abs_path = os.path.abspath(os.path.expanduser(file_path))
        
        # Check if path exists and is a file
        if not os.path.isfile(abs_path):
            logger.error(f"Channel file does not exist: {file_path}")
            return False
        
        # Check if path is within allowed directories
        for allowed_dir in ALLOWED_CHANNEL_DIRS:
            allowed_abs = os.path.abspath(os.path.expanduser(allowed_dir))
            if abs_path.startswith(allowed_abs + os.sep) or abs_path == allowed_abs:
                return True
        
        logger.error(f"Channel file path not in allowed directories: {file_path}")
        logger.error(f"Allowed directories: {ALLOWED_CHANNEL_DIRS}")
        return False
    except Exception as e:
        logger.error(f"Path validation error: {e}")
        return False


def _validate_webhook_url(url: str) -> bool:
    """
    Validate webhook URL to prevent SSRF attacks.
    
    Checks:
    - Scheme is http or https
    - Hostname resolves to public IP (not private/loopback)
    """
    try:
        parsed = urllib.parse.urlparse(url)
        
        # Check scheme
        if parsed.scheme not in ('http', 'https'):
            logger.error(f"Invalid URL scheme: {parsed.scheme}")
            return False
        
        # Check hostname
        if not parsed.hostname:
            logger.error("URL missing hostname")
            return False
        
        environment = _current_environment()
        allow_private_hosts = environment in {"development", "dev", "test"}

        # Resolve hostname to IP
        try:
            addrinfo = socket.getaddrinfo(parsed.hostname, None)
            for _, _, _, _, sockaddr in addrinfo:
                ip = ipaddress.ip_address(sockaddr[0])
                
                # Check for private/reserved IPs
                if (
                    not allow_private_hosts
                    and (ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local)
                ):
                    logger.error(f"URL resolves to private/reserved IP: {ip}")
                    return False
        except socket.gaierror:
            logger.error(f"Could not resolve hostname: {parsed.hostname}")
            return False
        
        return True
    except Exception as e:
        logger.error(f"URL validation error: {e}")
        return False


def _sanitize_url(url: str) -> Optional[str]:
    """Sanitize and validate URL, returning None if invalid."""
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ('http', 'https'):
            return None
        escaped = html.escape(url, quote=True)
        return escaped
    except Exception:
        return None


def _escape_html(text: str) -> str:
    """Escape HTML special characters to prevent injection."""
    return (text
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#x27;')
    )


def _is_dry_run_mode() -> bool:
    """Check if notification channels should run in dry-run (log-only) mode."""
    return get_notification_runtime_config().dry_run


def _configured_channel_recipient(channel_type: str) -> str:
    """Resolve a configured channel recipient from the shared runtime config."""
    config = get_notification_runtime_config().channels.get(channel_type, {})
    if isinstance(config, dict):
        return str(config.get("recipient") or "")
    return str(getattr(config, "recipient", "") or "")


def _mask_email(email: str) -> str:
    """
    Mask email address for safe logging (PII protection).
    
    Shows only domain, e.g., "***@example.com"
    """
    if '@' not in email:
        return "***"
    _, domain = email.rsplit('@', 1)
    return f"***@{domain}"


def _notification_job_parts(job: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Return the job, match, and requirement payloads for a notification item."""
    return (
        job.get('job', {}),
        job.get('match', {}),
        job.get('requirements', {}),
    )


def _escaped_job_detail(job_info: Dict[str, Any], key: str) -> str:
    """Return an escaped optional job detail."""
    return html.escape(job_info.get(key, '') or '')


def _build_job_type_details(job_info: Dict[str, Any]) -> List[str]:
    """Return the escaped job type details to display."""
    return [
        detail
        for detail in (
            _escaped_job_detail(job_info, 'job_type'),
            _escaped_job_detail(job_info, 'job_level'),
        )
        if detail
    ]


def _build_email_optional_job_details(job_info: Dict[str, Any]) -> List[str]:
    """Build optional email detail rows for location, salary, and role metadata."""
    details: List[str] = []

    location = _escaped_job_detail(job_info, 'location')
    if location:
        details.append(f'            <div class="job-detail">📍 {location}</div>\n')

    salary = _escaped_job_detail(job_info, 'salary')
    if salary:
        details.append(f'            <div class="job-detail">💰 {salary}</div>\n')

    job_type_details = _build_job_type_details(job_info)
    if job_type_details:
        details.append(
            f'            <div class="job-detail">📋 {" | ".join(job_type_details)}</div>\n'
        )

    return details


def _build_email_job_links(job: Dict[str, Any], metadata: Dict[str, Any]) -> List[str]:
    """Build optional apply/details links for an email job card."""
    links: List[str] = []

    apply_url = job.get('apply_url')
    if apply_url:
        safe_url = _sanitize_url(apply_url)
        if safe_url:
            links.append(
                f'            <div class="job-detail"><strong>🔗 <a href="{safe_url}">Apply Here</a></strong></div>\n'
            )

    match_id = metadata.get('match_id')
    if match_id:
        safe_match_id = html.escape(str(match_id), quote=True)
        links.append(
            f'            <div class="job-detail"><strong>🔍 <a href="/api/matches/{safe_match_id}">View Details</a></strong></div>\n'
        )

    return links


def _build_email_job_card(job: Dict[str, Any], metadata: Dict[str, Any]) -> str:
    """Build a single HTML email job card."""
    job_info, match_info, req_info = _notification_job_parts(job)
    title = html.escape(job_info.get('title', 'Unknown Position'))
    company = html.escape(job_info.get('company', 'Unknown'))
    overall_score = match_info.get('overall_score', 0)
    fit_score = match_info.get('fit_score', 0)
    want_score = match_info.get('want_score')
    total = req_info.get('total', 0)
    matched = req_info.get('matched', 0)

    html_parts = [
        f"""        <div class="job-card">
            <div class="job-title">{title}</div>
            <div class="job-detail"><strong>🏢 Company:</strong> {company}</div>
""",
        *_build_email_optional_job_details(job_info),
        f"""
            <div class="job-detail"><strong>📊 Match:</strong> {overall_score:.0f}%</div>
            <div class="job-detail"><strong>🎯 Fit:</strong> {fit_score:.0f}%</div>
""",
    ]

    if want_score:
        html_parts.append(
            f'            <div class="job-detail"><strong>💡 Want:</strong> {want_score:.0f}%</div>\n'
        )

    html_parts.append(
        f'            <div class="job-detail"><strong>✅ Requirements:</strong> {matched}/{total} matched</div>\n'
    )
    html_parts.extend(_build_email_job_links(job, metadata))
    html_parts.append("        </div>\n")
    return "".join(html_parts)


def _build_telegram_optional_lines(job_info: Dict[str, Any]) -> List[str]:
    """Build optional Telegram lines for a job payload."""
    lines: List[str] = []

    location = job_info.get('location')
    if location:
        lines.append(f"📍 {_escape_html(location)}")

    salary = job_info.get('salary')
    if salary:
        lines.append(f"💰 {_escape_html(salary)}")

    job_type_details = [
        _escape_html(detail)
        for detail in (job_info.get('job_type'), job_info.get('job_level'))
        if detail
    ]
    if job_type_details:
        lines.append(f"📋 {' | '.join(job_type_details)}")

    return lines


def _build_telegram_job_links(job: Dict[str, Any], metadata: Dict[str, Any]) -> List[str]:
    """Build optional apply/details links for a Telegram message."""
    links: List[str] = []

    apply_url = job.get('apply_url')
    if apply_url:
        safe_url = _sanitize_url(apply_url)
        if safe_url:
            links.append(f"🔗 <a href=\"{safe_url}\">Apply Here</a>")

    match_id = metadata.get('match_id')
    if match_id:
        safe_match_id = _escape_html(str(match_id))
        links.append(f"🔍 <a href=\"/api/matches/{safe_match_id}\">View Details</a>")

    return links


def _build_telegram_job_lines(job: Dict[str, Any], metadata: Dict[str, Any]) -> List[str]:
    """Build the Telegram lines for a single job notification."""
    job_info, match_info, req_info = _notification_job_parts(job)
    total = req_info.get('total', 0)
    matched = req_info.get('matched', 0)
    want_score = match_info.get('want_score')

    lines = [
        f"🎯 <b>{_escape_html(job_info.get('title', 'Unknown Position'))}</b>",
        f"🏢 {_escape_html(job_info.get('company', 'Unknown'))}",
        *_build_telegram_optional_lines(job_info),
        "",
        f"📊 <b>{match_info.get('overall_score', 0):.0f}%</b> Match",
        f"   Fit: {match_info.get('fit_score', 0):.0f}%",
    ]

    if want_score:
        lines.append(f"   Want: {want_score:.0f}%")

    lines.append(f"✅ {matched}/{total} requirements matched")
    lines.extend(_build_telegram_job_links(job, metadata))
    return lines


class NotificationChannel(ABC):
    """
    Abstract base class for all notification channels.
    
    All notification channels must implement this interface.
    This ensures Liskov Substitution - any channel can be used interchangeably.
    """
    
    @property
    @abstractmethod
    def channel_type(self) -> str:
        """Return the channel type identifier."""
        pass
    
    @abstractmethod
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        """
        Send a notification through this channel.
        
        Args:
            recipient: Target recipient (format depends on channel)
            subject: Notification subject/title
            body: Notification body
            metadata: Additional channel-specific metadata
        
        Returns:
            True if sent successfully, False otherwise
        """
        pass
    
    def validate_config(self) -> bool:
        """
        Validate that the channel is properly configured.
        
        Returns:
            True if configured correctly, False otherwise
        """
        return True


class EmailChannel(NotificationChannel):
    """Email notification channel via SMTP."""
    
    @property
    def channel_type(self) -> str:
        return 'email'
    
    def validate_config(self) -> bool:
        smtp_config = get_notification_runtime_config().smtp
        if not smtp_config.server or not smtp_config.port:
            return False

        username = smtp_config.username
        password = smtp_config.password
        return bool(username) == bool(password)
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        if not self.validate_config():
            raise NotificationConfigurationError(
                "Email not configured — SMTP runtime settings not set"
            )
        
        try:
            smtp_config = get_notification_runtime_config().smtp
            smtp_server = smtp_config.server or 'smtp.gmail.com'
            smtp_port = smtp_config.port
            username = smtp_config.username
            password = smtp_config.password
            use_tls = smtp_config.use_tls
            from_email = smtp_config.from_email or username or 'noreply@jobscout.app'

            if bool(username) != bool(password):
                raise NotificationConfigurationError(
                    "Email not configured — SMTP_USERNAME and SMTP_PASSWORD must both be set"
                )
            
            # Check for rich job notification content
            job_contents = metadata.get('job_contents', [])
            
            if job_contents:
                # Use HTML format for rich notifications
                html_body = self._build_html_body(subject, job_contents, metadata)
                msg = MIMEMultipart()
                msg['From'] = from_email
                msg['To'] = recipient
                msg['Subject'] = subject
                msg.attach(MIMEText(html_body, 'html', 'utf-8'))
            else:
                # Fallback to plain text
                msg = MIMEMultipart()
                msg['From'] = from_email
                msg['To'] = recipient
                msg['Subject'] = subject
                msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                if use_tls:
                    server.starttls()
                if username and password:
                    server.login(username, password)
                server.send_message(msg)
            
            logger.info(f"Email sent to {_mask_email(recipient)}")
            return True

        except (TerminalNotificationError, TransientNotificationError):
            raise

        except (smtplib.SMTPException, OSError) as e:
            raise TransientNotificationError(
                f"Failed to send email to {_mask_email(recipient)}: {e}",
                failure_class="email_transport",
            ) from e
        except Exception as e:
            raise TransientNotificationError(
                f"Failed to send email to {_mask_email(recipient)}: {e}",
                failure_class="email_unknown",
            ) from e
    
    def _build_html_body(self, subject: str, job_contents: List[Dict], metadata: Dict) -> str:
        """Build HTML email body for job notifications."""
        safe_subject = html.escape(subject)
        html_body = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px 8px 0 0; }}
        .content {{ padding: 20px; background: #f9f9f9; }}
        .job-card {{ background: white; padding: 15px; margin: 10px 0; border-radius: 8px; border-left: 4px solid #667eea; }}
        .job-title {{ font-size: 18px; font-weight: bold; color: #667eea; margin-bottom: 10px; }}
        .job-detail {{ margin: 5px 0; font-size: 14px; }}
        .separator {{ border-top: 2px dashed #ddd; margin: 20px 0; }}
        .footer {{ text-align: center; padding: 15px; color: #666; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{safe_subject}</h1>
    </div>
    <div class="content">
"""
        for i, job in enumerate(job_contents):
            if i > 0:
                html_body += '<div class="separator"></div>\n'
            html_body += _build_email_job_card(job, metadata)
        
        html_body += """    </div>
    <div class="footer">
        <p>JobScout - AI-Powered Job Matching</p>
    </div>
</body>
</html>"""
        return html_body


class DiscordChannel(NotificationChannel):
    """Discord notification channel via webhook."""
    
    @property
    def channel_type(self) -> str:
        return 'discord'
    
    def validate_config(self) -> bool:
        return True
    
    def _parse_rate_limit_response(self, response: requests.Response) -> int:
        """
        Parse rate limit info from Discord 429 response.
        
        Discord provides multiple sources:
        1. JSON body: retry_after field
        2. Headers: X-RateLimit-Reset-After (fractional seconds)
        3. Headers: Retry-After
        
        Returns:
            Seconds to wait before retrying.
        """
        try:
            data = response.json()
            retry_after = data.get('retry_after')
            if retry_after is not None:
                return int(float(retry_after))
        except Exception:
            pass
        
        # Check headers (X-RateLimit-Reset-After may have fractional seconds)
        reset_after = response.headers.get('X-RateLimit-Reset-After')
        if reset_after:
            try:
                return int(float(reset_after))
            except (ValueError, TypeError):
                pass
        
        retry_after_header = response.headers.get('Retry-After')
        if retry_after_header:
            try:
                return int(retry_after_header)
            except (ValueError, TypeError):
                pass
        
        # Default fallback
        logger.warning("Could not parse Discord rate limit info, using default 60s")
        return 60
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        webhook_url = (
            recipient
            or metadata.get('discord_webhook_url')
            or _configured_channel_recipient('discord')
        )
        
        if not webhook_url:
            raise NotificationConfigurationError(
                "Discord not configured — DISCORD_WEBHOOK_URL not set"
            )
        
        try:
            embeds: List[Dict[str, Any]] = []
            
            # Check for rich job notification content in metadata
            job_contents = metadata.get('job_contents', [])
            
            if job_contents:
                # Use rich embed format for job notifications
                # Reconstruct Pydantic models from dicts (serialized for safe JSON/RQ transport)
                contents = [JobNotificationContent.model_validate(jc) for jc in job_contents]
                embeds = NotificationMessageBuilder.build_batch_embeds(contents)
            else:
                # Fallback to simple embed
                embed = {
                    'title': subject,
                    'description': body[:2000],
                    'color': 0x0099ff,
                    'footer': {'text': 'JobScout Notifications'},
                    'timestamp': metadata.get('created_at') or datetime.now(timezone.utc).isoformat(),
                }
                embeds = [embed]
            
            payload = {
                'username': 'JobScout',
                'avatar_url': 'https://cdn-icons-png.flaticon.com/512/3135/3135715.png',
                'embeds': embeds
            }
            
            response = requests.post(webhook_url, json=payload, timeout=30)
            
            # Handle rate limiting (HTTP 429)
            if response.status_code == 429:
                retry_after = self._parse_rate_limit_response(response)
                raise RateLimitException(
                    f"Discord rate limited. Retry after {retry_after} seconds.",
                    retry_after=retry_after
                )
            
            response.raise_for_status()
            
            logger.info(f"Discord message sent ({len(embeds)} embed(s))")
            return True
        
        except RateLimitException:
            # Re-raise rate limit exceptions to be handled by the worker
            raise
        
        except requests.RequestException as e:
            raise TransientNotificationError(
                f"Failed to send Discord message: {e}",
                failure_class="discord_transport",
            ) from e
        except Exception as e:
            raise TransientNotificationError(
                f"Failed to send Discord message: {e}",
                failure_class="discord_unknown",
            ) from e


class TelegramChannel(NotificationChannel):
    """Telegram notification channel via Bot API."""
    
    @property
    def channel_type(self) -> str:
        return 'telegram'
    
    def validate_config(self) -> bool:
        return bool(get_notification_runtime_config().telegram_bot_token)
    
    def _parse_rate_limit_response(self, response: requests.Response) -> int:
        """
        Parse rate limit info from Telegram 429 response.
        
        Telegram typically provides retry info in JSON body:
        - parameters.retry_after
        
        Returns:
            Seconds to wait before retrying.
        """
        try:
            data = response.json()
            parameters = data.get('parameters', {})
            retry_after = parameters.get('retry_after')
            if retry_after is not None:
                return int(retry_after)
        except Exception:
            pass
        
        # Fallback: try description field
        try:
            data = response.json()
            retry_after = data.get('retry_after')
            if retry_after is not None:
                return int(retry_after)
        except Exception:
            pass
        
        # Default fallback
        logger.warning("Could not parse Telegram rate limit info, using default 60s")
        return 60
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        bot_token = get_notification_runtime_config().telegram_bot_token
        
        if not bot_token:
            raise NotificationConfigurationError(
                "Telegram not configured — TELEGRAM_BOT_TOKEN not set"
            )
        
        try:
            api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            
            # Check for rich job notification content
            job_contents = metadata.get('job_contents', [])
            
            if job_contents:
                # Build rich HTML message for multiple jobs
                message = self._build_rich_message(subject, job_contents, metadata)
            else:
                # Fallback to simple message
                safe_subject = _escape_html(subject)
                safe_body = _escape_html(body)
                message = f"<b>{safe_subject}</b>\n\n{safe_body}"
            
            if len(message) > 4096:
                # Truncate but keep structure
                message = message[:4093] + "..."
            
            payload = {
                'chat_id': recipient,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': False
            }
            
            response = requests.post(api_url, json=payload, timeout=30)
            
            # Handle rate limiting (HTTP 429)
            if response.status_code == 429:
                retry_after = self._parse_rate_limit_response(response)
                raise RateLimitException(
                    f"Telegram rate limited. Retry after {retry_after} seconds.",
                    retry_after=retry_after
                )
            
            if response.status_code == 200:
                logger.info(f"Telegram message sent to {recipient}")
                return True
            elif response.status_code >= 500:
                # 5xx — server-side error, worth retrying
                raise TransientNotificationError(
                    f"Telegram API error: {response.status_code} — {response.text}"
                )
            else:
                # 4xx (bad token, bad chat_id, etc.) — won't succeed on retry
                raise NotificationConfigurationError(
                    f"Telegram API rejected message ({response.status_code}): {response.text}"
                )
        
        except RateLimitException:
            raise

        except (TerminalNotificationError, TransientNotificationError):
            raise

        except requests.RequestException as e:
            raise TransientNotificationError(
                f"Failed to send Telegram message: {e}",
                failure_class="telegram_transport",
            ) from e
        except Exception as e:
            raise TransientNotificationError(
                f"Failed to send Telegram message: {e}",
                failure_class="telegram_unknown",
            ) from e
    
    def _build_rich_message(self, subject: str, job_contents: List[Dict], metadata: Dict) -> str:
        """Build rich HTML message for Telegram."""
        lines = [f"<b>{_escape_html(subject)}</b>\n"]
        
        for i, job in enumerate(job_contents):
            if i > 0:
                lines.append("\n" + "─" * 30 + "\n")
            lines.extend(_build_telegram_job_lines(job, metadata))
        
        return "\n".join(lines)


class WebhookChannel(NotificationChannel):
    """Generic webhook notification channel."""
    
    @property
    def channel_type(self) -> str:
        return 'webhook'
    
    def validate_config(self) -> bool:
        return True
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        """Send webhook POST request."""
        try:
            webhook_url = recipient
            
            if not _validate_webhook_url(webhook_url):
                raise NotificationConfigurationError(
                    f"Invalid or unsafe webhook URL: {webhook_url}"
                )
            
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'JobScout-Notification-Service/1.0'
            }
            
            # Check for rich job notification content
            job_contents = metadata.get('job_contents', [])
            
            if job_contents:
                # Rich payload with full job details
                payload = {
                    'type': 'job_notifications',
                    'subject': subject,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'jobs': [
                        {
                            'job': job.get('job', {}),
                            'match': job.get('match', {}),
                            'requirements': job.get('requirements', {}),
                            'apply_url': job.get('apply_url'),
                        }
                        for job in job_contents
                    ],
                    'metadata': {
                        'user_id': metadata.get('user_id'),
                    }
                }
            else:
                # Fallback to simple payload
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError:
                    payload = {
                        'subject': subject,
                        'body': body,
                        'metadata': metadata
                    }
            
            response = requests.post(
                webhook_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            parsed = urllib.parse.urlparse(webhook_url)
            safe_url = f"{parsed.scheme}://{parsed.hostname}{parsed.path}"
            logger.info(f"Webhook sent to {safe_url}")
            return True

        except (TerminalNotificationError, TransientNotificationError):
            raise

        except requests.RequestException as e:
            raise TransientNotificationError(
                f"Failed to send webhook: {e}",
                failure_class="webhook_transport",
            ) from e
        except Exception as e:
            raise TransientNotificationError(
                f"Failed to send webhook: {e}",
                failure_class="webhook_unknown",
            ) from e


class InAppChannel(NotificationChannel):
    """In-app notification channel (stores in database)."""
    
    @property
    def channel_type(self) -> str:
        return 'in_app'
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        """Store notification in database for in-app display."""
        # In production, store in notification table
        logger.info(f"[IN_APP] User: {recipient}, Title: {subject}")
        return True


class NotificationChannelFactory:
    """
    Factory for creating notification channels.
    
    Implements Factory pattern for creating channel instances.
    This makes it easy to add new channels without modifying existing code (Open/Closed).
    
    Supports dynamic loading of custom channels from:
    1. Configuration (module_path + class_name)
    2. Environment variables (NOTIFICATION_CHANNEL_PATH)
    3. Direct registration in code
    """
    
    # Registry of available channels
    _channels: Dict[str, type] = {
        'email': EmailChannel,
        'discord': DiscordChannel,
        'telegram': TelegramChannel,
        'webhook': WebhookChannel,
        'in_app': InAppChannel,
    }
    
    _custom_channels_loaded = False
    
    @classmethod
    def _load_custom_channels(cls):
        """Load custom channels from environment and file system."""
        if cls._custom_channels_loaded:
            return
        
        # 1. Load from environment variable (path to Python file)
        channel_path = os.environ.get('NOTIFICATION_CHANNEL_PATH', '')
        if channel_path:
            # Validate path for security before loading
            if _validate_channel_file_path(channel_path):
                cls._load_channel_from_file(channel_path)
            else:
                logger.warning(f"Skipping custom channel from {channel_path} - path not allowed")
        
        # 2. Load from environment variable (comma-separated list of module paths)
        channel_modules = os.environ.get('NOTIFICATION_CHANNEL_MODULES', '')
        if channel_modules:
            for module_path in channel_modules.split(','):
                module_path = module_path.strip()
                if module_path:
                    cls._load_channel_from_module(module_path)
        
        cls._custom_channels_loaded = True
    
    @classmethod
    def _load_channel_from_file(cls, file_path: str):
        """Dynamically load a channel class from a Python file."""
        try:
            import importlib.util
            import inspect
            
            # Load the module
            spec = importlib.util.spec_from_file_location("custom_channel", file_path)
            if not spec or not spec.loader:
                logger.warning(f"Could not load custom channel from {file_path}")
                return
            
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Find channel classes in the module
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (issubclass(obj, NotificationChannel) and 
                    obj is not NotificationChannel and
                    hasattr(obj, 'channel_type')):
                    
                    # Get channel_type by instantiating (needed for properties)
                    try:
                        instance = obj()
                        channel_type = instance.channel_type
                        if isinstance(channel_type, str):
                            cls._channels[channel_type] = obj
                            logger.info(f"Loaded custom channel '{channel_type}' from {file_path}")
                    except Exception as e:
                        logger.warning(f"Could not instantiate channel class {name}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to load custom channel from {file_path}: {e}")
    
    @classmethod
    def _load_channel_from_module(cls, module_path: str):
        """Load a channel class from an installed module."""
        try:
            import importlib
            import inspect
            
            # Parse module path (e.g., "my_package.channels:CustomChannel")
            if ':' in module_path:
                module_name, class_name = module_path.split(':', 1)
            else:
                # Try to find any NotificationChannel subclass
                module_name = module_path
                class_name = None
            
            # Validate module name: only allow dotted Python identifiers (no shell chars)
            import re as _re
            if not _re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.]*', module_name):
                raise ValueError(f"Invalid module name: {module_name!r}")

            # Import the module
            # nosemgrep
            module = importlib.import_module(module_name)
            
            if class_name:
                # Get specific class
                channel_class = getattr(module, class_name)
                if (issubclass(channel_class, NotificationChannel) and 
                    channel_class is not NotificationChannel):
                    channel_type = channel_class().channel_type
                    cls._channels[channel_type] = channel_class
                    logger.info(f"Loaded custom channel '{channel_type}' from {module_path}")
            else:
                # Find all channel classes in module
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if (issubclass(obj, NotificationChannel) and 
                        obj is not NotificationChannel and
                        hasattr(obj, 'channel_type')):
                        
                        channel_type = obj().channel_type
                        cls._channels[channel_type] = obj
                        logger.info(f"Loaded custom channel '{channel_type}' from {module_name}")
                        
        except Exception as e:
            logger.error(f"Failed to load custom channel from module {module_path}: {e}")
    
    @classmethod
    def get_channel(cls, channel_type: str) -> NotificationChannel:
        """
        Get a notification channel instance by type.
        
        Args:
            channel_type: Type of channel (email, discord, telegram, etc.)
        
        Returns:
            NotificationChannel instance
        
        Raises:
            ValueError: If channel type is not registered
        """
        # Ensure custom channels are loaded
        cls._load_custom_channels()
        
        channel_class = cls._channels.get(channel_type.lower())
        if not channel_class:
            raise ValueError(f"Unknown channel type: {channel_type}. "
                           f"Available: {', '.join(cls._channels.keys())}")
        
        return channel_class()
    
    @classmethod
    def register_channel(cls, channel_type: str, channel_class: type):
        """
        Register a new notification channel.
        
        This allows extending the system with custom channels without
        modifying the factory code.
        
        Args:
            channel_type: Type identifier for the channel
            channel_class: Class implementing NotificationChannel
        """
        if not issubclass(channel_class, NotificationChannel):
            raise ValueError("Channel class must extend NotificationChannel")
        
        cls._channels[channel_type.lower()] = channel_class
        logger.info(f"Registered new channel type: {channel_type}")
    
    @classmethod
    def list_channels(cls) -> list:
        """List all available channel types."""
        cls._load_custom_channels()
        return list(cls._channels.keys())
