#!/usr/bin/env python3
"""Unit tests for notification.models."""

from notification.models import NotificationDeliveryPlan


class TestNotificationDeliveryPlan:
    def test_required_fields(self):
        plan = NotificationDeliveryPlan(user_id="u1", enabled_channels=["email"])
        assert plan.user_id == "u1"
        assert plan.enabled_channels == ["email"]
        assert plan.settings_snapshot is None

    def test_with_settings_snapshot(self):
        snap = object()
        plan = NotificationDeliveryPlan(
            user_id="u1", enabled_channels=["email", "discord"], settings_snapshot=snap
        )
        assert plan.settings_snapshot is snap
        assert plan.enabled_channels == ["email", "discord"]

    def test_multiple_channels(self):
        plan = NotificationDeliveryPlan(user_id="u1", enabled_channels=["a", "b", "c"])
        assert len(plan.enabled_channels) == 3

    def test_empty_channels(self):
        plan = NotificationDeliveryPlan(user_id="u1", enabled_channels=[])
        assert plan.enabled_channels == []
