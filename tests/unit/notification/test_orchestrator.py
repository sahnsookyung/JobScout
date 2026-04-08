#!/usr/bin/env python3
"""Unit tests for notification.orchestrator."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from notification.models import NotificationDeliveryPlan
from notification.orchestrator import (
    _alert_eligible_matches_for_plan,
    _notification_setting_value,
    _resolve_notification_plan,
    _send_batch_complete_notification,
    _send_match_notification,
    send_notifications,
)


def _notif_config(**kwargs):
    defaults = dict(
        enabled=True,
        user_id=None,
        channels={},
        min_fit_for_alerts=70.0,
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


def _match(score, *, match_id="match-1"):
    return SimpleNamespace(
        id=match_id,
        fit_score=score,
        notified=False,
    )


def _delivery_plan(**kwargs):
    defaults = dict(user_id="user-42", enabled_channels=["email"])
    defaults.update(kwargs)
    return NotificationDeliveryPlan(**defaults)


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


class TestAlertEligibleMatchesForPlan:
    def test_filters_below_threshold(self):
        config = _notif_config(min_fit_for_alerts=70.0)
        plan = _delivery_plan()
        matches = [_match(50.0, match_id="m1"), _match(70.0, match_id="m2"), _match(90.0, match_id="m3")]
        result = _alert_eligible_matches_for_plan(matches, config, plan)
        assert [match.id for match in result] == ["m2", "m3"]

    def test_empty_list(self):
        config = _notif_config(min_fit_for_alerts=70.0)
        plan = _delivery_plan()
        assert _alert_eligible_matches_for_plan([], config, plan) == []

    def test_threshold_from_snapshot(self):
        config = _notif_config(min_fit_for_alerts=70.0)
        snap = SimpleNamespace(min_fit_for_alerts=50.0)
        plan = _delivery_plan(settings_snapshot=snap)
        matches = [_match(55.0, match_id="m1"), _match(45.0, match_id="m2")]
        result = _alert_eligible_matches_for_plan(matches, config, plan)
        assert [match.id for match in result] == ["m1"]

    def test_none_score_excluded(self):
        config = _notif_config(min_fit_for_alerts=0.0)
        plan = _delivery_plan()
        result = _alert_eligible_matches_for_plan([_match(None)], config, plan)
        assert result == []


class TestSendNotifications:
    def test_disabled_config_returns_zero(self):
        ctx = _ctx(_notif_config(enabled=False))
        count = send_notifications(
            ctx,
            saved_count=1,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 0
        ctx.notification_service.notify_new_match.assert_not_called()

    def test_zero_saved_count_returns_zero(self):
        ctx = _ctx()
        count = send_notifications(
            ctx,
            saved_count=0,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 0

    def test_partial_save_failure_suppresses_notifications(self):
        ctx = _ctx()
        count = send_notifications(
            ctx,
            saved_count=2,
            failed_count=1,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 0

    def test_no_delivery_plan_returns_zero(self):
        ctx = _ctx(_notif_config(enabled=True, user_id=None, channels={}))
        count = send_notifications(
            ctx,
            saved_count=1,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 0

    @patch("notification.orchestrator._load_persisted_notification_matches")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_sends_per_alert_eligible_match(self, mock_plan, mock_send, mock_load):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_fit_for_alerts=60.0,
            notify_on_new_match=True,
            notify_on_batch_complete=False,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.return_value = True
        mock_load.return_value = [_match(80.0, match_id="m1"), _match(90.0, match_id="m2"), _match(40.0, match_id="m3")]

        count = send_notifications(
            ctx,
            saved_count=3,
            failed_count=0,
            resume_fingerprint="fp-abc",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 2
        assert mock_send.call_count == 2

    @patch("notification.orchestrator._load_persisted_notification_matches")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_stop_event_halts_loop(self, mock_plan, mock_send, mock_load):
        ctx = _ctx(_notif_config(enabled=True, min_fit_for_alerts=0.0, notify_on_new_match=True))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_load.return_value = [_match(90.0), _match(80.0, match_id="m2")]

        stop = threading.Event()
        stop.set()

        count = send_notifications(
            ctx,
            saved_count=2,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=stop,
            ranking_context=SimpleNamespace(),
        )
        assert count == 0
        mock_send.assert_not_called()

    @patch("notification.orchestrator._load_persisted_notification_matches")
    @patch("notification.orchestrator._send_batch_complete_notification")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_batch_complete_sent_when_enabled(self, mock_plan, mock_send, mock_batch, mock_load):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_fit_for_alerts=0.0,
            notify_on_new_match=True,
            notify_on_batch_complete=True,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.return_value = True
        mock_load.return_value = [_match(80.0)]

        send_notifications(
            ctx,
            saved_count=1,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        mock_batch.assert_called_once()

    @patch("notification.orchestrator._load_persisted_notification_matches")
    @patch("notification.orchestrator._send_batch_complete_notification")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_batch_complete_skipped_when_disabled(self, mock_plan, mock_send, mock_batch, mock_load):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_fit_for_alerts=0.0,
            notify_on_new_match=True,
            notify_on_batch_complete=False,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.return_value = True
        mock_load.return_value = [_match(80.0)]

        send_notifications(
            ctx,
            saved_count=1,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        mock_batch.assert_not_called()

    @patch("notification.orchestrator._load_persisted_notification_matches")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_exception_in_send_is_caught(self, mock_plan, mock_send, mock_load):
        ctx = _ctx(_notif_config(
            enabled=True, min_fit_for_alerts=0.0, notify_on_new_match=True,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.side_effect = RuntimeError("boom")
        mock_load.return_value = [_match(90.0)]

        count = send_notifications(
            ctx,
            saved_count=1,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 0

    @patch("notification.orchestrator._load_persisted_notification_matches")
    @patch("notification.orchestrator._send_batch_complete_notification")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_batch_complete_exception_is_caught(self, mock_plan, mock_send, mock_batch, mock_load):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_fit_for_alerts=0.0,
            notify_on_new_match=True,
            notify_on_batch_complete=True,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.return_value = True
        mock_batch.side_effect = RuntimeError("batch error")
        mock_load.return_value = [_match(80.0)]

        count = send_notifications(
            ctx,
            saved_count=1,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 1
        mock_batch.assert_called_once()

    @patch("notification.orchestrator._resolve_notification_plan")
    def test_top_level_exception_returns_zero(self, mock_plan):
        ctx = _ctx(_notif_config(enabled=True, min_fit_for_alerts=0.0, notify_on_new_match=True))
        mock_plan.side_effect = RuntimeError("unexpected")

        count = send_notifications(
            ctx,
            saved_count=1,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 0

    @patch("notification.orchestrator._load_persisted_notification_matches")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_notify_on_new_match_false_skips_all(self, mock_plan, mock_send, mock_load):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_fit_for_alerts=0.0,
            notify_on_new_match=False,
            notify_on_batch_complete=False,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_load.return_value = [_match(90.0)]

        count = send_notifications(
            ctx,
            saved_count=1,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            ranking_context=SimpleNamespace(),
        )
        assert count == 0
        mock_send.assert_not_called()
