"""Shared notification runtime configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Optional

from core.config_loader import load_config_data

REDIS_URL_DEFAULT = "redis://localhost:6379/0"
BASE_URL_DEFAULT = "http://localhost:8080"
RATE_LIMIT_MAX_WAIT_SECONDS_DEFAULT = 300
SMTP_PORT_DEFAULT = 587
SMTP_USE_TLS_DEFAULT = True
LEGACY_RECIPIENT_ENV_VARS = {
    "email": ("NOTIFICATION_EMAIL", "EMAIL"),
    "discord": ("DISCORD_WEBHOOK_URL",),
    "telegram": ("TELEGRAM_CHAT_ID",),
    "webhook": ("NOTIFICATION_WEBHOOK_URL",),
}


def _as_bool(value: Any, default: bool) -> bool:
    """Coerce config values that may arrive as strings through env overrides."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class NotificationSmtpRuntimeConfig:
    server: Optional[str]
    port: int
    username: str
    password: str
    use_tls: bool
    from_email: Optional[str]


@dataclass(frozen=True)
class NotificationRuntimeConfig:
    redis_url: str
    base_url: str
    rate_limit_max_wait_seconds: int
    dry_run: bool
    telegram_bot_token: str
    smtp: NotificationSmtpRuntimeConfig
    channels: Dict[str, Any]


def _resolve_legacy_recipient_env(channel_type: str) -> str | None:
    """Resolve env-backed legacy recipients using standard override order."""
    import os

    for env_var in LEGACY_RECIPIENT_ENV_VARS.get(channel_type, ()):
        value = os.environ.get(env_var)
        if value:
            return value
    return None


def _merge_channel_recipient_overrides(raw_channels: Dict[str, Any]) -> Dict[str, Any]:
    """Merge env-backed recipient overrides into the raw channel config."""
    merged_channels = {
        channel_name.lower(): (
            dict(config) if isinstance(config, dict) else config
        )
        for channel_name, config in raw_channels.items()
    }

    for channel_name in LEGACY_RECIPIENT_ENV_VARS:
        env_recipient = _resolve_legacy_recipient_env(channel_name)
        if not env_recipient:
            continue

        existing = merged_channels.get(channel_name)
        if isinstance(existing, dict):
            existing["recipient"] = env_recipient
        else:
            merged_channels[channel_name] = {"recipient": env_recipient}

    return merged_channels


@lru_cache()
def get_notification_runtime_config() -> NotificationRuntimeConfig:
    """Resolve notification runtime settings using shared config precedence."""
    raw_config = load_config_data()
    notification_config = raw_config.get("notifications") or {}
    smtp_config = notification_config.get("smtp") or {}

    return NotificationRuntimeConfig(
        redis_url=notification_config.get("redis_url") or REDIS_URL_DEFAULT,
        base_url=notification_config.get("base_url") or BASE_URL_DEFAULT,
        rate_limit_max_wait_seconds=int(
            notification_config.get("rate_limit_max_wait_seconds")
            or RATE_LIMIT_MAX_WAIT_SECONDS_DEFAULT
        ),
        dry_run=_as_bool(notification_config.get("dry_run"), False),
        telegram_bot_token=str(notification_config.get("telegram_bot_token") or "").strip(),
        smtp=NotificationSmtpRuntimeConfig(
            server=smtp_config.get("server"),
            port=int(smtp_config.get("port") or SMTP_PORT_DEFAULT),
            username=str(smtp_config.get("username") or ""),
            password=str(smtp_config.get("password") or ""),
            use_tls=_as_bool(smtp_config.get("use_tls"), SMTP_USE_TLS_DEFAULT),
            from_email=smtp_config.get("from_email"),
        ),
        channels=_merge_channel_recipient_overrides(
            dict(notification_config.get("channels") or {})
        ),
    )


def clear_notification_runtime_config_cache() -> None:
    """Clear the cached runtime config, primarily for tests."""
    get_notification_runtime_config.cache_clear()
