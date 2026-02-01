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
    from core.notification_channels import NotificationChannelFactory
    
    # Get channel by type
    channel = NotificationChannelFactory.get_channel('discord')
    channel.send(recipient, subject, body, metadata)
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
import json
import os

# HTTP requests
import requests

# Email
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Security utilities
import urllib.parse
import ipaddress
import socket

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
        
        # Resolve hostname to IP
        try:
            addrinfo = socket.getaddrinfo(parsed.hostname, None)
            for _, _, _, _, sockaddr in addrinfo:
                ip = ipaddress.ip_address(sockaddr[0])
                
                # Check for private/reserved IPs
                if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local:
                    logger.error(f"URL resolves to private/reserved IP: {ip}")
                    return False
        except socket.gaierror:
            logger.error(f"Could not resolve hostname: {parsed.hostname}")
            return False
        
        return True
    except Exception as e:
        logger.error(f"URL validation error: {e}")
        return False


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
    return os.environ.get('NOTIFICATION_DRY_RUN', '').lower() in ('true', '1', 'yes')


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
        required_vars = ['SMTP_SERVER', 'SMTP_PORT', 'SMTP_USERNAME', 'SMTP_PASSWORD']
        return all(os.environ.get(var) for var in required_vars)
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        if not self.validate_config():
            if _is_dry_run_mode():
                logger.warning("Email not configured, logging only (dry-run mode)")
                logger.info(f"[EMAIL] To: {recipient}, Subject: {subject}")
                return True
            else:
                logger.error("Email not configured - set SMTP environment variables or enable dry-run mode")
                return False
        
        try:
            smtp_server = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
            smtp_port = int(os.environ.get('SMTP_PORT', '587'))
            username = os.environ.get('SMTP_USERNAME', '')
            password = os.environ.get('SMTP_PASSWORD', '')
            from_email = os.environ.get('FROM_EMAIL', 'noreply@jobscout.app')
            
            msg = MIMEMultipart()
            msg['From'] = from_email
            msg['To'] = recipient
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)
            
            logger.info(f"Email sent to {recipient}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False


class DiscordChannel(NotificationChannel):
    """Discord notification channel via webhook."""
    
    @property
    def channel_type(self) -> str:
        return 'discord'
    
    def validate_config(self) -> bool:
        # Can use global webhook or per-notification webhook in metadata
        return True  # Always valid - webhook can be passed in metadata
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        # Get webhook URL from metadata or environment
        webhook_url = metadata.get('discord_webhook_url') or os.environ.get('DISCORD_WEBHOOK_URL', '')
        
        if not webhook_url:
            if _is_dry_run_mode():
                logger.warning("Discord webhook not configured, logging only (dry-run mode)")
                logger.info(f"[DISCORD] To: {recipient}, Message: {body}")
                return True
            else:
                logger.error("Discord webhook not configured - set DISCORD_WEBHOOK_URL or enable dry-run mode")
                return False
        
        try:
            # Discord webhook format
            embed = {
                'title': subject,
                'description': body[:2000],  # Discord limit
                'color': 0x00ff00 if 'match' in body.lower() else 0x0099ff,
                'timestamp': metadata.get('created_at'),
                'footer': {
                    'text': 'JobScout Notification Service'
                }
            }
            
            # Add fields if available
            fields = []
            if 'score' in metadata:
                fields.append({
                    'name': 'Match Score',
                    'value': f"{metadata['score']}/100",
                    'inline': True
                })
            if metadata.get('company'):
                fields.append({
                    'name': 'Company',
                    'value': metadata['company'],
                    'inline': True
                })
            if fields:
                embed['fields'] = fields
            
            payload = {
                'username': 'JobScout',
                'avatar_url': 'https://cdn-icons-png.flaticon.com/512/3135/3135715.png',
                'embeds': [embed]
            }
            
            response = requests.post(webhook_url, json=payload, timeout=30)
            response.raise_for_status()
            
            logger.info(f"Discord message sent to webhook")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Discord message: {e}")
            return False


class TelegramChannel(NotificationChannel):
    """Telegram notification channel via Bot API."""
    
    @property
    def channel_type(self) -> str:
        return 'telegram'
    
    def validate_config(self) -> bool:
        return bool(os.environ.get('TELEGRAM_BOT_TOKEN'))
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        """
        Send Telegram message.
        
        Recipient should be a chat ID (e.g., @username or numeric ID).
        """
        bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        
        if not bot_token:
            logger.warning("Telegram bot token not configured, logging only")
            logger.info(f"[TELEGRAM] To: {recipient}, Message: {body}")
            return True
        
        try:
            # Telegram Bot API endpoint
            api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            
            # Format message with HTML escaping to prevent injection
            # Escape subject and body to ensure only our <b> tag is rendered as HTML
            safe_subject = _escape_html(subject)
            safe_body = _escape_html(body)
            message = f"<b>{safe_subject}</b>\n\n{safe_body}"
            if len(message) > 4096:
                message = message[:4093] + "..."
            
            payload = {
                'chat_id': recipient,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': False
            }
            
            response = requests.post(api_url, json=payload, timeout=30)
            
            if response.status_code == 200:
                logger.info(f"Telegram message sent to {recipient}")
                return True
            else:
                logger.error(f"Telegram API error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False


class WebhookChannel(NotificationChannel):
    """Generic webhook notification channel."""
    
    @property
    def channel_type(self) -> str:
        return 'webhook'
    
    def validate_config(self) -> bool:
        return True  # URL is passed per-notification
    
    def send(self, recipient: str, subject: str, body: str, metadata: Dict[str, Any]) -> bool:
        """
        Send webhook POST request.
        
        Recipient is the webhook URL.
        Body should be valid JSON string.
        """
        try:
            webhook_url = recipient
            
            # Validate URL to prevent SSRF
            if not _validate_webhook_url(webhook_url):
                logger.error(f"Invalid or unsafe webhook URL: {webhook_url}")
                return False
            
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'JobScout-Notification-Service/1.0'
            }
            
            # Parse body as JSON if possible
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                # If not valid JSON, wrap it
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
            
            # Log without exposing full URL (might contain tokens)
            parsed = urllib.parse.urlparse(webhook_url)
            safe_url = f"{parsed.scheme}://{parsed.hostname}{parsed.path}"
            logger.info(f"Webhook sent to {safe_url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send webhook: {e}")
            return False


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
                    
                    # Get channel_type as class attribute (don't instantiate)
                    channel_type = getattr(obj, 'channel_type', None)
                    if isinstance(channel_type, str):
                        cls._channels[channel_type] = obj
                        logger.info(f"Loaded custom channel '{channel_type}' from {file_path}")
                    
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
            
            # Import the module
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
    def load_channels_from_config(cls, custom_channels: list):
        """
        Load custom channels from configuration.
        
        Args:
            custom_channels: List of dicts with 'name', 'module', 'class' keys
        """
        for channel_config in custom_channels:
            try:
                name = channel_config.get('name')
                module_path = channel_config.get('module')
                class_name = channel_config.get('class')
                
                if not all([name, module_path, class_name]):
                    logger.warning(f"Invalid custom channel config: {channel_config}")
                    continue
                
                # Import the module
                import importlib
                module = importlib.import_module(module_path)
                
                # Get the class
                channel_class = getattr(module, class_name)
                
                # Validate it's a proper channel
                if not issubclass(channel_class, NotificationChannel):
                    logger.warning(f"Class {class_name} does not extend NotificationChannel")
                    continue
                
                # Register with the specified name
                cls._channels[name.lower()] = channel_class
                logger.info(f"Loaded custom channel '{name}' from {module_path}.{class_name}")
                
            except Exception as e:
                logger.error(f"Failed to load custom channel from config: {e}")
    
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
    
    @classmethod
    def get_configured_channels(cls) -> list:
        """List channels that are properly configured."""
        cls._load_custom_channels()
        configured = []
        for channel_type in cls._channels.keys():
            try:
                channel = cls.get_channel(channel_type)
                if channel.validate_config():
                    configured.append(channel_type)
            except Exception as e:
                logger.debug(f"Channel {channel_type} not properly configured: {e}")
        return configured
