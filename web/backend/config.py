"""
Configuration management for JobScout web application.
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from core.config_loader import (
    DEFAULT_ENV_MAPPINGS,
    DEFAULT_HEADER_MAPPINGS,
    DatabaseConfig,
    EtlConfig,
    JobSpyConfig,
    MatchingConfig,
    NotificationConfig,
    PreferencesConfig,
    ScraperConfig,
    load_config_data,
)

load_dotenv()

logger = logging.getLogger(__name__)


class WebConfig(BaseModel):
    """Web server configuration."""

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080)


class AppConfig(BaseModel):
    """Main application configuration."""

    database: DatabaseConfig = Field(
        default_factory=lambda: DatabaseConfig(
            url="postgresql://user:password@localhost:5432/jobscout"
        )
    )
    web: WebConfig = Field(default_factory=WebConfig)
    etl: Optional[EtlConfig] = None
    jobspy: Optional[JobSpyConfig] = None
    matching: MatchingConfig = Field(default_factory=MatchingConfig)
    preferences: PreferencesConfig = Field(default_factory=PreferencesConfig)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    scrapers: List[ScraperConfig] = Field(default_factory=list)


WEB_ENV_MAPPINGS: tuple[tuple[Sequence[str], Sequence[str]], ...] = (
    *DEFAULT_ENV_MAPPINGS,
    (["WEB_HOST"], ["web", "host"]),
    (["WEB_PORT"], ["web", "port"]),
    (["BASE_URL"], ["notifications", "base_url"]),
)


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).resolve().parents[2]


def load_web_config(config_path: Optional[Path] = None) -> AppConfig:
    """Load web configuration using the shared YAML/env precedence rules."""
    resolved_config_path = config_path or (get_project_root() / "config.yaml")
    raw_config: Dict[str, Any] = load_config_data(
        resolved_config_path,
        fallback_path=get_project_root() / "config.yaml",
        env_mappings=WEB_ENV_MAPPINGS,
        header_mappings=DEFAULT_HEADER_MAPPINGS,
    )
    return AppConfig(**raw_config)


@lru_cache()
def get_config() -> AppConfig:
    """Get cached web application configuration."""
    return load_web_config()
