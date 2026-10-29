"""Notification jobs must carry their owner through every database operation."""

from typing import Any, Dict
from unittest.mock import Mock

import pytest

from database.database import current_database_user_id, worker_database_context
from notification import service
from notification.exceptions import NotificationConfigurationError


@pytest.mark.parametrize("failure", [None, RuntimeError("delivery failed"),
                                     NotificationConfigurationError("inactive")])
def test_task_scopes_delivery_and_failure_recording_to_owner(
    monkeypatch: pytest.MonkeyPatch, failure: Exception | None,
) -> None:
    observed = []
    limiter = Mock()
    limiter.get_wait_time.return_value = 0
    monkeypatch.setattr(service, "NotificationRateLimiter", Mock(return_value=limiter))
    monkeypatch.setattr(service, "get_current_job", lambda: None)

    def deliver(notification_id: str, data: Dict[str, Any]) -> str:
        observed.append(("delivery", current_database_user_id()))
        if failure is not None:
            raise failure
        return notification_id

    def record_failure(*args: Any, **kwargs: Any) -> None:
        observed.append(("failure", current_database_user_id()))

    monkeypatch.setattr(service, "_send_and_record_notification", deliver)
    monkeypatch.setattr(service, "_record_notification_failure", record_failure)
    payload = {"channel_type": "discord", "user_id": "notification-owner"}
    with worker_database_context(user_id="outer-owner", tenant_id="outer-tenant"):
        if isinstance(failure, NotificationConfigurationError):
            with pytest.raises(NotificationConfigurationError):
                service.process_notification_task(payload)
        else:
            service.process_notification_task(payload)
        assert current_database_user_id() == "outer-owner"

    assert observed == [("delivery", "notification-owner")] + (
        [("failure", "notification-owner")] if failure is not None else []
    )
    assert current_database_user_id() is None


@pytest.mark.parametrize("metadata", [{"tenant_id": "message-tenant"}, {}, None])
def test_task_uses_message_tenant_and_does_not_inherit_previous_identity(
    monkeypatch: pytest.MonkeyPatch, metadata: Dict[str, Any] | None,
) -> None:
    from database.database import ContextSession, _apply_database_context

    settings = {}
    limiter = Mock()
    limiter.get_wait_time.return_value = 0
    monkeypatch.setattr(service, "NotificationRateLimiter", Mock(return_value=limiter))
    monkeypatch.setattr(service, "get_current_job", lambda: None)

    def capture_context(notification_id: str, data: Dict[str, Any]) -> str:
        connection = Mock()
        connection.execute.side_effect = lambda _sql, params: settings.update(
            {params["name"]: params["value"]}
        )
        with ContextSession() as session:
            _apply_database_context(connection, session)
        return notification_id

    monkeypatch.setattr(service, "_send_and_record_notification", capture_context)
    with worker_database_context(user_id="previous-owner", tenant_id="previous-tenant"):
        service.process_notification_task({
            "channel_type": "discord", "user_id": "notification-owner",
            "metadata": metadata,
        })
    assert settings["jobscout.user_id"] == "notification-owner"
    assert settings["jobscout.tenant_id"] == (metadata or {}).get("tenant_id", "")
