from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from database.repositories.notification_settings import NotificationSettingsRepository


class TestNotificationSettingsRepository:
    def test_get_settings_returns_scalar_result(self):
        owner_id = uuid4()
        db = SimpleNamespace()
        expected = object()
        result = SimpleNamespace(unique=lambda: SimpleNamespace(scalar_one_or_none=lambda: expected))
        db.execute = lambda stmt: result

        repo = NotificationSettingsRepository(db)

        assert repo.get_settings(owner_id) is expected

    def test_get_or_create_settings_returns_existing_record(self):
        owner_id = uuid4()
        db = SimpleNamespace(add=lambda obj: None, flush=lambda: None)
        repo = NotificationSettingsRepository(db)
        existing = object()
        repo.get_settings = lambda requested_owner_id: existing

        assert repo.get_or_create_settings(owner_id) is existing

    def test_get_or_create_settings_creates_new_record(self):
        owner_id = uuid4()
        added = []
        db = SimpleNamespace(add=lambda obj: added.append(obj), flush=lambda: None)
        repo = NotificationSettingsRepository(db)
        repo.get_settings = lambda requested_owner_id: None

        settings = repo.get_or_create_settings(owner_id)

        assert settings.owner_id == owner_id
        assert added == [settings]

    def test_get_channel_returns_scalar_result(self):
        owner_id = uuid4()
        db = SimpleNamespace()
        expected = object()
        db.execute = lambda stmt: SimpleNamespace(scalar_one_or_none=lambda: expected)

        repo = NotificationSettingsRepository(db)

        assert repo.get_channel(owner_id, "discord") is expected

    def test_get_or_create_channel_returns_existing_record(self):
        owner_id = uuid4()
        db = SimpleNamespace(add=lambda obj: None, flush=lambda: None)
        repo = NotificationSettingsRepository(db)
        existing = object()
        repo.get_channel = lambda requested_owner_id, channel_type: existing

        assert repo.get_or_create_channel(owner_id, "discord") is existing

    def test_get_or_create_channel_creates_new_record(self):
        owner_id = uuid4()
        added = []
        db = SimpleNamespace(add=lambda obj: added.append(obj), flush=lambda: None)
        repo = NotificationSettingsRepository(db)
        repo.get_channel = lambda requested_owner_id, channel_type: None

        channel = repo.get_or_create_channel(owner_id, "discord")

        assert channel.owner_id == owner_id
        assert channel.channel_type == "discord"
        assert added == [channel]
