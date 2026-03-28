from __future__ import annotations

from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from database.models import UserNotificationChannel, UserNotificationSettings
from database.repositories.base import BaseRepository


class NotificationSettingsRepository(BaseRepository):
    """Repository for per-user notification settings."""

    def get_settings(self, owner_id: UUID) -> Optional[UserNotificationSettings]:
        stmt = (
            select(UserNotificationSettings)
            .options(joinedload(UserNotificationSettings.channels))
            .where(UserNotificationSettings.owner_id == owner_id)
        )
        return self.db.execute(stmt).unique().scalar_one_or_none()

    def get_or_create_settings(self, owner_id: UUID) -> UserNotificationSettings:
        settings = self.get_settings(owner_id)
        if settings is not None:
            return settings

        settings = UserNotificationSettings(owner_id=owner_id)
        self.db.add(settings)
        self.db.flush()
        return settings

    def get_channel(
        self,
        owner_id: UUID,
        channel_type: str,
    ) -> Optional[UserNotificationChannel]:
        stmt = select(UserNotificationChannel).where(
            UserNotificationChannel.owner_id == owner_id,
            UserNotificationChannel.channel_type == channel_type,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_or_create_channel(
        self,
        owner_id: UUID,
        channel_type: str,
    ) -> UserNotificationChannel:
        channel = self.get_channel(owner_id, channel_type)
        if channel is not None:
            return channel

        channel = UserNotificationChannel(owner_id=owner_id, channel_type=channel_type)
        self.db.add(channel)
        self.db.flush()
        return channel
