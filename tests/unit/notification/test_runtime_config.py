"""Tests for notification/runtime_config.py."""

from notification.runtime_config import (
    clear_notification_runtime_config_cache,
    get_notification_runtime_config,
)


def test_runtime_config_uses_env_recipient_when_channel_missing(monkeypatch):
    monkeypatch.setenv("NOTIFICATION_WEBHOOK_URL", "https://env.example/hook")
    clear_notification_runtime_config_cache()

    runtime_config = get_notification_runtime_config()

    assert runtime_config.channels["webhook"]["recipient"] == "https://env.example/hook"


def test_runtime_config_env_recipient_overrides_yaml_channel_recipient(monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://env.example/hook")
    clear_notification_runtime_config_cache()

    runtime_config = get_notification_runtime_config()

    assert runtime_config.channels["discord"]["recipient"] == "https://env.example/hook"


def test_runtime_config_falls_back_to_email_env_alias(monkeypatch):
    monkeypatch.delenv("NOTIFICATION_EMAIL", raising=False)
    monkeypatch.setenv("EMAIL", "alias@example.com")
    clear_notification_runtime_config_cache()

    runtime_config = get_notification_runtime_config()

    assert runtime_config.channels["email"]["recipient"] == "alias@example.com"


def test_runtime_config_honors_base_url_env_override(monkeypatch):
    monkeypatch.setenv("BASE_URL", "https://env.example")
    clear_notification_runtime_config_cache()

    runtime_config = get_notification_runtime_config()

    assert runtime_config.base_url == "https://env.example"


def test_runtime_config_honors_rate_limit_wait_env_override(monkeypatch):
    monkeypatch.setenv("NOTIFICATION_RATE_LIMIT_MAX_WAIT", "17")
    clear_notification_runtime_config_cache()

    runtime_config = get_notification_runtime_config()

    assert runtime_config.rate_limit_max_wait_seconds == 17
