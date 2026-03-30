#!/usr/bin/env python3
"""Unit tests for notification.orchestrator."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

from notification.models import NotificationDeliveryPlan
from notification.orchestrator import (
    _notification_setting_value,
    _high_score_matches_for_plan,
    send_notifications,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _notif_config(**kwargs):
    defaults = dict(
        enabled=True,
        user_id=None,
        channels={},
        min_score_threshold=70.0,
        notify_on_new_match=True,
        notify_on_batch_complete=False,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _ctx(notif_config=None):
    return SimpleNamespace(
        config=SimpleNamespace(notifications=notif_config or _notif_config()),
        notification_service=MagicMock(),
    )


def _dto(score):
    job = SimpleNamespace(id="job-1", title="Eng", company="Acme")
    return SimpleNamespace(
        overall_score=score,
        fit_score=score,
        want_score=0.0,
        jd_required_coverage=1.0,
        job=job,
    )


def _delivery_plan(**kwargs):
    defaults = dict(user_id="user-42", enabled_channels=["email"])
    defaults.update(kwargs)
    return NotificationDeliveryPlan(**defaults)


# ---------------------------------------------------------------------------
# _notification_setting_value
# ---------------------------------------------------------------------------

class TestNotificationSettingValue:
    def test_uses_config_when_no_snapshot(self):
        config = SimpleNamespace(foo="from_config")
        assert _notification_setting_value(config, None, "foo") == "from_config"

    def test_uses_snapshot_when_present(self):
        config = SimpleNamespace(foo="from_config")
        snap = SimpleNamespace(foo="from_snapshot")
        assert _notification_setting_value(config, snap, "foo") == "from_snapshot"

    def test_snapshot_zero_overrides_config(self):
        config = SimpleNamespace(threshold=80.0)
        snap = SimpleNamespace(threshold=0.0)
        assert _notification_setting_value(config, snap, "threshold") == 0.0


# ---------------------------------------------------------------------------
# _high_score_matches_for_plan
# ---------------------------------------------------------------------------

class TestHighScoreMatchesForPlan:
    def test_filters_below_threshold(self):
        config = _notif_config(min_score_threshold=70.0)
        plan = _delivery_plan()
        dtos = [_dto(50.0), _dto(70.0), _dto(90.0)]
        result = _high_score_matches_for_plan(dtos, config, plan)
        assert len(result) == 2
        assert all(d.overall_score >= 70.0 for d in result)

    def test_empty_list(self):
        config = _notif_config(min_score_threshold=70.0)
        plan = _delivery_plan()
        assert _high_score_matches_for_plan([], config, plan) == []

    def test_threshold_from_snapshot(self):
        config = _notif_config(min_score_threshold=70.0)
        snap = SimpleNamespace(min_score_threshold=50.0)
        plan = _delivery_plan(settings_snapshot=snap)
        dtos = [_dto(55.0), _dto(45.0)]
        result = _high_score_matches_for_plan(dtos, config, plan)
        assert len(result) == 1
        assert result[0].overall_score == 55.0

    def test_none_score_excluded(self):
        config = _notif_config(min_score_threshold=0.0)
        plan = _delivery_plan()
        dto = _dto(None)
        result = _high_score_matches_for_plan([dto], config, plan)
        assert result == []


# ---------------------------------------------------------------------------
# send_notifications
# ---------------------------------------------------------------------------

class TestSendNotifications:
    def test_disabled_config_returns_zero(self):
        ctx = _ctx(_notif_config(enabled=False))
        count = send_notifications(ctx, [_dto(90.0)], 1, "fp", threading.Event())
        assert count == 0
        ctx.notification_service.notify_new_match.assert_not_called()

    def test_zero_saved_count_returns_zero(self):
        ctx = _ctx()
        count = send_notifications(ctx, [_dto(90.0)], 0, "fp", threading.Event())
        assert count == 0

    def test_no_delivery_plan_returns_zero(self):
        ctx = _ctx(_notif_config(enabled=True, user_id=None, channels={}))
        count = send_notifications(ctx, [_dto(90.0)], 1, "fp", threading.Event())
        assert count == 0

    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_sends_per_high_score_match(self, mock_plan, mock_send):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_score_threshold=60.0,
            notify_on_new_match=True,
            notify_on_batch_complete=False,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.return_value = True

        dtos = [_dto(80.0), _dto(90.0), _dto(40.0)]
        count = send_notifications(ctx, dtos, 3, "fp-abc", threading.Event())
        assert count == 2
        assert mock_send.call_count == 2

    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_stop_event_halts_loop(self, mock_plan, mock_send):
        ctx = _ctx(_notif_config(enabled=True, min_score_threshold=0.0, notify_on_new_match=True))
        plan = _delivery_plan()
        mock_plan.return_value = plan

        stop = threading.Event()
        stop.set()

        dtos = [_dto(90.0), _dto(80.0)]
        count = send_notifications(ctx, dtos, 2, "fp", stop)
        assert count == 0
        mock_send.assert_not_called()

    @patch("notification.orchestrator._send_batch_complete_notification")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_batch_complete_sent_when_enabled(self, mock_plan, mock_send, mock_batch):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_score_threshold=0.0,
            notify_on_new_match=True,
            notify_on_batch_complete=True,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.return_value = True

        dtos = [_dto(80.0)]
        send_notifications(ctx, dtos, 1, "fp", threading.Event())
        mock_batch.assert_called_once()

    @patch("notification.orchestrator._send_batch_complete_notification")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_batch_complete_skipped_when_disabled(self, mock_plan, mock_send, mock_batch):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_score_threshold=0.0,
            notify_on_new_match=True,
            notify_on_batch_complete=False,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.return_value = True

        dtos = [_dto(80.0)]
        send_notifications(ctx, dtos, 1, "fp", threading.Event())
        mock_batch.assert_not_called()

    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_exception_in_send_is_caught(self, mock_plan, mock_send):
        ctx = _ctx(_notif_config(
            enabled=True, min_score_threshold=0.0, notify_on_new_match=True,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.side_effect = RuntimeError("boom")

        count = send_notifications(ctx, [_dto(90.0)], 1, "fp", threading.Event())
        assert count == 0

    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_notify_on_new_match_false_skips_all(self, mock_plan, mock_send):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_score_threshold=0.0,
            notify_on_new_match=False,
            notify_on_batch_complete=False,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan

        count = send_notifications(ctx, [_dto(90.0)], 1, "fp", threading.Event())
        assert count == 0
        mock_send.assert_not_called()
