#!/usr/bin/env python3
"""Unit tests for notification.orchestrator."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from notification.models import NotificationDeliveryPlan
from notification.orchestrator import (
    _alert_eligible_matches_for_plan,
    _load_persisted_notification_matches,
    _notification_setting_value,
    _send_match_notification,
    resolve_notification_fit_floor,
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


def _uow(repo):
    manager = MagicMock()
    manager.__enter__.return_value = repo
    manager.__exit__.return_value = False
    return manager


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


class TestResolveNotificationFitFloor:
    def test_no_notification_config_returns_zero(self):
        ctx = SimpleNamespace(config=SimpleNamespace(), notification_service=None)
        assert resolve_notification_fit_floor(ctx) == 0.0

    def test_no_notification_service_uses_static_config_floor(self):
        ctx = _ctx(_notif_config(min_fit_for_alerts=81.0))
        ctx.notification_service = None

        assert resolve_notification_fit_floor(ctx) == 81.0

    @patch("notification.orchestrator._resolve_notification_plan")
    def test_missing_delivery_plan_uses_static_config_floor(self, mock_plan):
        mock_plan.return_value = None
        ctx = _ctx(_notif_config(min_fit_for_alerts=72.0))

        assert resolve_notification_fit_floor(ctx, owner_id="user-1") == 72.0

    @patch("notification.orchestrator._resolve_notification_plan")
    def test_delivery_plan_snapshot_overrides_config_floor(self, mock_plan):
        mock_plan.return_value = _delivery_plan(
            settings_snapshot=SimpleNamespace(min_fit_for_alerts=64.0),
        )
        ctx = _ctx(_notif_config(min_fit_for_alerts=72.0))

        assert resolve_notification_fit_floor(ctx, owner_id="user-1") == 64.0


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
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
        )
        assert count == 0
        ctx.notification_service.notify_new_match.assert_not_called()

    def test_zero_saved_count_returns_zero(self):
        ctx = _ctx()
        count = send_notifications(
            ctx,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
        )
        assert count == 0

    def test_partial_save_failure_suppresses_notifications(self):
        ctx = _ctx()
        count = send_notifications(
            ctx,
            failed_count=1,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
        )
        assert count == 0

    def test_no_delivery_plan_returns_zero(self):
        ctx = _ctx(_notif_config(enabled=True, user_id=None, channels={}))
        count = send_notifications(
            ctx,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
        )
        assert count == 0

    @patch("notification.orchestrator._resolve_notification_plan")
    def test_missing_selection_run_returns_zero_after_delivery_plan_resolves(self, mock_plan):
        mock_plan.return_value = _delivery_plan()
        ctx = _ctx(_notif_config(enabled=True, user_id="user-1", channels={"email": {}}))

        count = send_notifications(
            ctx,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            selection_run_id=None,
        )

        assert count == 0

    @patch("notification.orchestrator._load_persisted_notification_matches")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_empty_persisted_selection_returns_zero(self, mock_plan, mock_load):
        mock_plan.return_value = _delivery_plan()
        mock_load.return_value = []
        ctx = _ctx(_notif_config(enabled=True, user_id="user-1", channels={"email": {}}))

        count = send_notifications(
            ctx,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            selection_run_id="run-1",
        )

        assert count == 0
        mock_load.assert_called_once_with("run-1")

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
            failed_count=0,
            resume_fingerprint="fp-abc",
            stop_event=threading.Event(),
            selection_run_id="run-1",
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
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=stop,
            selection_run_id="run-1",
        )
        assert count == 0
        mock_send.assert_not_called()


class TestPersistedNotificationHelpers:
    @patch("notification.orchestrator.job_uow")
    def test_send_match_notification_returns_false_when_record_missing(self, mock_uow):
        repo = MagicMock()
        repo.match.get_match_by_id.return_value = None
        mock_uow.return_value = _uow(repo)

        assert _send_match_notification(
            _ctx(),
            "missing-match",
            delivery_plan=_delivery_plan(),
            task_id="task-1",
        ) is False

    @patch("notification.orchestrator.job_uow")
    def test_send_match_notification_skips_hidden_or_inactive_match(self, mock_uow):
        repo = MagicMock()
        repo.match.get_match_by_id.return_value = SimpleNamespace(
            id="m1",
            status="active",
            is_hidden=True,
        )
        mock_uow.return_value = _uow(repo)

        assert _send_match_notification(
            _ctx(),
            "m1",
            delivery_plan=_delivery_plan(),
            task_id="task-1",
        ) is False

    @patch("notification.orchestrator.job_uow")
    def test_send_match_notification_skips_already_notified_match(self, mock_uow):
        repo = MagicMock()
        repo.match.get_match_by_id.return_value = SimpleNamespace(
            id="m1",
            status="active",
            is_hidden=False,
            notified=True,
        )
        mock_uow.return_value = _uow(repo)

        assert _send_match_notification(
            _ctx(),
            "m1",
            delivery_plan=_delivery_plan(),
            task_id="task-1",
        ) is False

    @patch("notification.orchestrator.job_uow")
    def test_load_persisted_notification_matches_uses_selection_run_order_without_rerank(
        self,
        mock_uow,
    ):
        repo = MagicMock()
        repo.match_selection.get_items_for_run.return_value = [
            SimpleNamespace(
                job_match_id="m1",
                fit_score_at_selection=80.0,
                preference_score_at_selection=0.9,
                job_similarity_at_selection=0.8,
                alert_eligible=True,
            ),
            SimpleNamespace(
                job_match_id="m2",
                fit_score_at_selection=60.0,
                preference_score_at_selection=0.1,
                job_similarity_at_selection=0.5,
                alert_eligible=False,
            ),
        ]
        mock_uow.return_value = _uow(repo)

        result = _load_persisted_notification_matches("run-1")

        assert [candidate.id for candidate in result] == ["m1", "m2"]
        assert [candidate.alert_eligible for candidate in result] == [True, False]

    @patch("notification.orchestrator.NotificationMessageBuilder.build_from_orm")
    @patch("notification.orchestrator.job_uow")
    def test_send_match_notification_marks_match_notified_after_successful_delivery(
        self,
        mock_uow,
        mock_build,
    ):
        match_record = SimpleNamespace(
            id="m1",
            status="active",
            is_hidden=False,
            notified=False,
            job_post=SimpleNamespace(company_url_direct="https://example.com"),
        )
        repo = MagicMock()
        repo.match.get_match_by_id.return_value = match_record
        mock_uow.return_value = _uow(repo)
        mock_build.return_value = {"subject": "hi"}
        ctx = _ctx()
        ctx.notification_service.notify_new_match.return_value = {"email": True}

        sent = _send_match_notification(
            ctx,
            "m1",
            delivery_plan=_delivery_plan(),
            task_id="task-1",
        )

        assert sent is True
        assert match_record.notified is True

    @patch("notification.orchestrator.NotificationMessageBuilder.build_from_orm")
    @patch("notification.orchestrator.job_uow")
    def test_send_match_notification_keeps_match_retryable_when_no_channel_accepts(
        self,
        mock_uow,
        mock_build,
    ):
        match_record = SimpleNamespace(
            id="m1",
            status="active",
            is_hidden=False,
            notified=False,
            job_post=SimpleNamespace(company_url_direct="https://example.com"),
        )
        repo = MagicMock()
        repo.match.get_match_by_id.return_value = match_record
        mock_uow.return_value = _uow(repo)
        mock_build.return_value = {"subject": "hi"}
        ctx = _ctx()
        ctx.notification_service.notify_new_match.return_value = {"email": False}

        sent = _send_match_notification(
            ctx,
            "m1",
            delivery_plan=_delivery_plan(),
            task_id="task-1",
        )

        assert sent is False
        assert match_record.notified is False

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
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            selection_run_id="run-1",
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
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            selection_run_id="run-1",
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
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            selection_run_id="run-1",
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
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            selection_run_id="run-1",
        )
        assert count == 1
        mock_batch.assert_called_once()

    @patch("notification.orchestrator._resolve_notification_plan")
    def test_top_level_exception_returns_zero(self, mock_plan):
        ctx = _ctx(_notif_config(enabled=True, min_fit_for_alerts=0.0, notify_on_new_match=True))
        mock_plan.side_effect = RuntimeError("unexpected")

        count = send_notifications(
            ctx,
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
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
            failed_count=0,
            resume_fingerprint="fp",
            stop_event=threading.Event(),
            selection_run_id="run-1",
        )
        assert count == 0
        mock_send.assert_not_called()
