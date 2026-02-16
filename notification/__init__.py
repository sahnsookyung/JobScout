"""
Notification Module

A comprehensive notification system supporting multiple channels,
deduplication, and async processing.

Usage:
    from notification import NotificationService, NotificationChannelFactory
    
    # Send a notification
    service = NotificationService(repo)
    service.send_notification(
        channel_type='discord',
        recipient='webhook-url',
        subject='New Match!',
        body='Details...',
        user_id='user123'
    )
    
    # Get a channel
    channel = NotificationChannelFactory.get_channel('email')
    channel.send('user@example.com', 'Subject', 'Body', {})
"""

from notification.channels import (
    NotificationChannel,
    EmailChannel,
    DiscordChannel,
    TelegramChannel,
    WebhookChannel,
    InAppChannel,
    NotificationChannelFactory,
    RateLimitException,
)

from notification.tracker import (
    NotificationTrackerService,
    NotificationEvent,
    DefaultDeduplicationStrategy,
    AggressiveDeduplicationStrategy,
    should_notify_user,
)

from notification.service import (
    NotificationService,
    NotificationPriority,
    process_notification_task,
)

__all__ = [
    # Channels
    'NotificationChannel',
    'EmailChannel',
    'DiscordChannel',
    'TelegramChannel',
    'WebhookChannel',
    'InAppChannel',
    'NotificationChannelFactory',
    'RateLimitException',
    # Tracker
    'NotificationTrackerService',
    'NotificationEvent',
    'DefaultDeduplicationStrategy',
    'AggressiveDeduplicationStrategy',
    'should_notify_user',
    # Service
    'NotificationService',
    'NotificationPriority',
    'process_notification_task',
]
