"""Shared notification runtime configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict

from core.config_loader import load_config_data

REDIS_URL_DEFAULT = "redis://localhost:6379/0"
BASE_URL_DEFAULT = "http://localhost:8080"
RATE_LIMIT_MAX_WAIT_SECONDS_DEFAULT = 300


@dataclass(frozen=True)
class NotificationRuntimeConfig:
    redis_url: str
    base_url: str
    rate_limit_max_wait_seconds: int
    channels: Dict[str, Any]


@lru_cache()
def get_notification_runtime_config() -> NotificationRuntimeConfig:
    """Resolve notification runtime settings using shared config precedence."""
    raw_config = load_config_data()
    notification_config = raw_config.get("notifications") or {}

    return NotificationRuntimeConfig(
        redis_url=notification_config.get("redis_url") or REDIS_URL_DEFAULT,
        base_url=notification_config.get("base_url") or BASE_URL_DEFAULT,
        rate_limit_max_wait_seconds=int(
            notification_config.get("rate_limit_max_wait_seconds")
            or RATE_LIMIT_MAX_WAIT_SECONDS_DEFAULT
        ),
        channels=dict(notification_config.get("channels") or {}),
    )
