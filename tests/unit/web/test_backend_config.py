from pathlib import Path

import yaml

from web.backend.config import get_config, load_web_config


def _write_config(tmp_path: Path, config: dict) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def test_load_web_config_prefers_env_over_yaml(monkeypatch, tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "database": {"url": "postgresql://yaml-user:yaml-pass@yaml-host:5432/jobscout"},
            "web": {"host": "127.0.0.1", "port": 8080},
            "notifications": {
                "base_url": "http://yaml.example",
                "redis_url": "redis://yaml:6379/0",
            },
        },
    )

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://env-user:env-pass@env-host:5432/jobscout",
    )
    monkeypatch.setenv("WEB_HOST", "0.0.0.0")
    monkeypatch.setenv("WEB_PORT", "9090")
    monkeypatch.setenv("BASE_URL", "https://env.example")
    monkeypatch.setenv("REDIS_URL", "redis://env:6379/1")

    config = load_web_config(config_path)

    assert config.database.url == "postgresql://env-user:env-pass@env-host:5432/jobscout"
    assert config.web.host == "0.0.0.0"
    assert config.web.port == 9090
    assert config.notifications.base_url == "https://env.example"
    assert config.notifications.redis_url == "redis://env:6379/1"


def test_load_web_config_uses_yaml_when_env_missing(monkeypatch, tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        {
            "database": {"url": "postgresql://yaml-user:yaml-pass@yaml-host:5432/jobscout"},
            "web": {"host": "127.0.0.1", "port": 8081},
            "notifications": {"base_url": "http://yaml.example"},
        },
    )

    for env_var in ("DATABASE_URL", "WEB_HOST", "WEB_PORT", "BASE_URL", "REDIS_URL"):
        monkeypatch.delenv(env_var, raising=False)

    config = load_web_config(config_path)

    assert config.database.url == "postgresql://yaml-user:yaml-pass@yaml-host:5432/jobscout"
    assert config.web.host == "127.0.0.1"
    assert config.web.port == 8081
    assert config.notifications.base_url == "http://yaml.example"


def test_get_config_is_cached(monkeypatch, tmp_path: Path) -> None:
    get_config.cache_clear()
    _write_config(
        tmp_path,
        {
            "database": {"url": "postgresql://cached:pass@cached-host:5432/jobscout"},
            "web": {"host": "127.0.0.1", "port": 8082},
        },
    )
    monkeypatch.setattr("web.backend.config.get_project_root", lambda: tmp_path)

    first = get_config()
    second = get_config()

    assert first is second
    assert first.database.url == "postgresql://cached:pass@cached-host:5432/jobscout"

    get_config.cache_clear()
