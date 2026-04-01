#!/usr/bin/env python3
"""Unit tests for notification.orchestrator."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

from notification.models import NotificationDeliveryPlan
from notification.orchestrator import (
    _notification_setting_value,
    _high_score_matches_for_plan,
    _resolve_notification_plan,
    _send_match_notification,
    _send_batch_complete_notification,
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

    @patch("notification.orchestrator._send_batch_complete_notification")
    @patch("notification.orchestrator._send_match_notification")
    @patch("notification.orchestrator._resolve_notification_plan")
    def test_batch_complete_exception_is_caught(self, mock_plan, mock_send, mock_batch):
        ctx = _ctx(_notif_config(
            enabled=True,
            min_score_threshold=0.0,
            notify_on_new_match=True,
            notify_on_batch_complete=True,
        ))
        plan = _delivery_plan()
        mock_plan.return_value = plan
        mock_send.return_value = True
        mock_batch.side_effect = RuntimeError("batch error")

        # Should not raise; exception is caught and logged
        count = send_notifications(ctx, [_dto(80.0)], 1, "fp", threading.Event())
        assert count == 1  # match notification succeeded; batch error was swallowed
        mock_batch.assert_called_once()

    @patch("notification.orchestrator._resolve_notification_plan")
    def test_top_level_exception_returns_zero(self, mock_plan):
        ctx = _ctx(_notif_config(enabled=True, min_score_threshold=0.0, notify_on_new_match=True))
        mock_plan.side_effect = RuntimeError("unexpected")

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


# ---------------------------------------------------------------------------
# _resolve_notification_plan
# ---------------------------------------------------------------------------

def _uow(repo):
    m = MagicMock()
    m.__enter__.return_value = repo
    m.__exit__.return_value = False
    return m


class TestResolveNotificationPlan:
    def test_no_user_id_returns_none(self):
        ctx = _ctx(_notif_config(user_id=None))
        result = _resolve_notification_plan(ctx, owner_id=None)
        assert result is None

    def test_non_uuid_user_id_no_channels_returns_none(self):
        config = _notif_config(
            user_id="legacy-user",
            channels={"email": SimpleNamespace(enabled=False)},
        )
        ctx = _ctx(config)
        result = _resolve_notification_plan(ctx, owner_id=None)
        assert result is None

    def test_non_uuid_user_id_with_enabled_channel(self):
        config = _notif_config(
            user_id="legacy-user",
            channels={
                "email": SimpleNamespace(enabled=True),
                "slack": SimpleNamespace(enabled=False),
            },
        )
        ctx = _ctx(config)
        result = _resolve_notification_plan(ctx, owner_id=None)
        assert result is not None
        assert result.user_id == "legacy-user"
        assert result.enabled_channels == ["email"]

    def test_owner_id_overrides_config_user_id(self):
        config = _notif_config(
            user_id="config-user",
            channels={"email": SimpleNamespace(enabled=True)},
        )
        ctx = _ctx(config)
        result = _resolve_notification_plan(ctx, owner_id="owner-user")
        assert result is not None
        assert result.user_id == "owner-user"

    @patch("notification.orchestrator.job_uow")
    def test_uuid_user_not_found_returns_none(self, mock_uow):
        repo = MagicMock()
        repo.db.get.return_value = None
        mock_uow.return_value = _uow(repo)

        config = _notif_config(user_id=None)
        ctx = _ctx(config)
        uuid_id = "00000000-0000-0000-0000-000000000001"
        result = _resolve_notification_plan(ctx, owner_id=uuid_id)
        assert result is None

    @patch("notification.orchestrator.job_uow")
    def test_uuid_notifications_disabled_returns_none(self, mock_uow):
        repo = MagicMock()
        user = MagicMock()
        repo.db.get.return_value = user
        snap = SimpleNamespace(notifications_enabled=False)
        ctx = _ctx(_notif_config(user_id=None))
        ctx.notification_service.get_user_notification_snapshot.return_value = snap
        ctx.notification_service.get_enabled_channels_for_user.return_value = ["email"]
        mock_uow.return_value = _uow(repo)

        uuid_id = "00000000-0000-0000-0000-000000000002"
        result = _resolve_notification_plan(ctx, owner_id=uuid_id)
        assert result is None

    @patch("notification.orchestrator.job_uow")
    def test_uuid_no_enabled_channels_returns_none(self, mock_uow):
        repo = MagicMock()
        user = MagicMock()
        repo.db.get.return_value = user
        snap = SimpleNamespace(notifications_enabled=True)
        ctx = _ctx(_notif_config(user_id=None))
        ctx.notification_service.get_user_notification_snapshot.return_value = snap
        ctx.notification_service.get_enabled_channels_for_user.return_value = []
        mock_uow.return_value = _uow(repo)

        uuid_id = "00000000-0000-0000-0000-000000000003"
        result = _resolve_notification_plan(ctx, owner_id=uuid_id)
        assert result is None

    @patch("notification.orchestrator.job_uow")
    def test_uuid_valid_returns_plan(self, mock_uow):
        repo = MagicMock()
        user = MagicMock()
        repo.db.get.return_value = user
        snap = SimpleNamespace(notifications_enabled=True)
        ctx = _ctx(_notif_config(user_id=None))
        ctx.notification_service.get_user_notification_snapshot.return_value = snap
        ctx.notification_service.get_enabled_channels_for_user.return_value = ["email", "discord"]
        mock_uow.return_value = _uow(repo)

        uuid_id = "00000000-0000-0000-0000-000000000004"
        result = _resolve_notification_plan(ctx, owner_id=uuid_id)
        assert result is not None
        assert result.user_id == uuid_id
        assert result.enabled_channels == ["email", "discord"]
        assert result.settings_snapshot is snap


# ---------------------------------------------------------------------------
# _send_match_notification
# ---------------------------------------------------------------------------

class TestSendMatchNotification:
    def _dto_for_job(self, job_id="job-1"):
        return SimpleNamespace(
            job=SimpleNamespace(id=job_id),
            overall_score=85.0,
            fit_score=80.0,
            jd_required_coverage=0.9,
        )

    @patch("notification.orchestrator.job_uow")
    def test_no_match_record_returns_false(self, mock_uow):
        repo = MagicMock()
        repo.get_existing_match.return_value = None
        mock_uow.return_value = _uow(repo)

        ctx = _ctx()
        plan = _delivery_plan()
        result = _send_match_notification(
            ctx, self._dto_for_job(), resume_fingerprint="fp", delivery_plan=plan, task_id=None
        )
        assert result is False

    @patch("notification.orchestrator.job_uow")
    def test_already_notified_returns_false(self, mock_uow):
        repo = MagicMock()
        match = MagicMock()
        match.id = "m-1"
        match.notified = True
        repo.get_existing_match.return_value = match
        mock_uow.return_value = _uow(repo)

        ctx = _ctx()
        plan = _delivery_plan()
        result = _send_match_notification(
            ctx, self._dto_for_job(), resume_fingerprint="fp", delivery_plan=plan, task_id=None
        )
        assert result is False

    @patch("notification.orchestrator.job_uow")
    def test_no_job_post_returns_false(self, mock_uow):
        repo = MagicMock()
        match = MagicMock()
        match.id = "m-1"
        match.notified = False
        match.job_post = None
        repo.get_existing_match.return_value = match
        mock_uow.return_value = _uow(repo)

        ctx = _ctx()
        plan = _delivery_plan()
        result = _send_match_notification(
            ctx, self._dto_for_job(), resume_fingerprint="fp", delivery_plan=plan, task_id=None
        )
        assert result is False

    @patch("notification.orchestrator.NotificationMessageBuilder")
    @patch("notification.orchestrator.job_uow")
    def test_success_returns_true(self, mock_uow, mock_builder):
        repo = MagicMock()
        match = MagicMock()
        match.id = "m-1"
        match.notified = False
        job_post = MagicMock()
        job_post.company_url_direct = "https://example.com/apply"
        match.job_post = job_post
        repo.get_existing_match.return_value = match
        mock_uow.return_value = _uow(repo)
        mock_builder.build_notification_content.return_value = MagicMock()

        ctx = _ctx()
        plan = _delivery_plan()
        result = _send_match_notification(
            ctx, self._dto_for_job(), resume_fingerprint="fp", delivery_plan=plan, task_id="t-1"
        )
        assert result is True
        ctx.notification_service.notify_new_match.assert_called_once()
        assert match.notified is True


# ---------------------------------------------------------------------------
# _send_batch_complete_notification
# ---------------------------------------------------------------------------

class TestSendBatchCompleteNotification:
    def test_calls_notify_batch_complete(self):
        ctx = _ctx()
        plan = _delivery_plan(user_id="u-1", enabled_channels=["email"])
        matches = [_dto(80.0), _dto(90.0)]

        _send_batch_complete_notification(
            ctx,
            delivery_plan=plan,
            saved_count=5,
            high_score_matches=matches,
            task_id="task-abc",
        )

        ctx.notification_service.notify_batch_complete.assert_called_once_with(
            user_id="u-1",
            total_matches=5,
            high_score_matches=2,
            channels=["email"],
            task_id="task-abc",
        )
