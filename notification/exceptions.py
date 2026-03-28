"""
Typed notification exceptions for failure classification.

Terminal failures (bad config, invalid recipient) should not be retried.
Transient failures (network errors, provider errors) are candidates for retry.
"""


class NotificationError(Exception):
    """Base class for all notification delivery errors."""
    failure_class: str = "unknown"


class TerminalNotificationError(NotificationError):
    """
    Terminal failure — the notification cannot succeed without operator action.

    Examples: missing SMTP config, invalid chat ID, blocked bot token,
    invalid webhook URL.  These should land in the DLQ (RQ failed queue)
    so an operator can see and fix them.  Do not retry.
    """
    failure_class = "terminal"


class TransientNotificationError(NotificationError):
    """
    Transient failure — a retry may succeed.

    Examples: provider 5xx, unexpected non-200 API responses, brief
    network hiccups that are not rate-limit related.  RQ's Retry policy
    will re-enqueue these automatically.
    """
    failure_class = "transient"
