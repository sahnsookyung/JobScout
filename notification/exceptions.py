"""Notification-specific delivery and configuration exceptions."""


class NotificationProcessingError(RuntimeError):
    """Base class for notification processing failures."""

    retryable = False

    def __init__(self, message: str, *, failure_class: str):
        super().__init__(message)
        self.failure_class = failure_class


class TerminalNotificationError(NotificationProcessingError):
    """A non-retryable notification failure."""

    retryable = False

    def __init__(self, message: str, *, failure_class: str = "terminal"):
        super().__init__(message, failure_class=failure_class)


class TransientNotificationError(NotificationProcessingError):
    """A retryable notification failure."""

    retryable = True

    def __init__(self, message: str, *, failure_class: str = "transient"):
        super().__init__(message, failure_class=failure_class)


class NotificationConfigurationError(TerminalNotificationError):
    """A permanent configuration or validation failure."""

    def __init__(self, message: str, *, failure_class: str = "configuration"):
        super().__init__(message, failure_class=failure_class)


# Backward-compatible alias
NotificationError = NotificationProcessingError
