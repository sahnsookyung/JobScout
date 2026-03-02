#!/usr/bin/env python3
"""
Configuration management for JobScout web application.
"""

import os
import yaml
import json
import logging
from pathlib import Path
from functools import lru_cache
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """Database configuration."""
    url: str = Field(default="postgresql://user:password@localhost:5432/jobscout")


class WebConfig(BaseModel):
    """Web server configuration."""
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080)


class ScorerConfig(BaseModel):
    """Scoring configuration."""
    fit_weight: float = Field(default=0.70)
    want_weight: float = Field(default=0.30)
    facet_weights: Dict[str, float] = Field(default_factory=lambda: {
        'remote_flexibility': 0.15,
        'compensation': 0.20,
        'learning_growth': 0.15,
        'company_culture': 0.15,
        'work_life_balance': 0.15,
        'tech_stack': 0.10,
        'visa_sponsorship': 0.10
    })


class ResumeConfig(BaseModel):
    """Configuration for resume processing in ETL."""
    resume_file: str = Field(default="resume.json")
    # Force re-extraction of resume instead of using pre-extracted data from storage.
    # When enabled, always runs LLM extraction regardless of fingerprint match.
    # Useful for validating extraction behavior or testing new extraction models.
    force_re_extraction: bool = False


class EtlConfig(BaseModel):
    """ETL configuration."""
    resume: Optional[ResumeConfig] = None


class MatchingConfig(BaseModel):
    """Matching configuration."""
    scorer: ScorerConfig = Field(default_factory=ScorerConfig)


class JobSpyConfig(BaseModel):
    """JobSpy API configuration."""
    url: str = Field(default="http://localhost:8000")


class LlmConfig(BaseModel):
    """LLM configuration for extraction and embeddings."""
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    extraction_headers: Optional[Dict[str, str]] = None
    extraction_model: str = "gpt-4o-mini"
    extraction_url: Optional[str] = None
    extraction_type: str = "openai"
    extraction_labels: Optional[List[str]] = None
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1024
    extraction_temperature: float = 0.0
    embedding_base_url: Optional[str] = None
    embedding_api_key: Optional[str] = None
    embedding_api_secret: Optional[str] = None
    embedding_headers: Optional[Dict[str, str]] = None


class EtlConfigWithLlm(BaseModel):
    """ETL configuration with LLM settings."""
    resume: Optional[ResumeConfig] = None
    llm: LlmConfig = Field(default_factory=LlmConfig)


class NotificationChannelConfig(BaseModel):
    """Configuration for a single notification channel."""
    enabled: bool = True
    recipient: Optional[str] = None


class NotificationConfig(BaseModel):
    """Notification configuration."""
    enabled: bool = False
    user_id: Optional[str] = None
    base_url: str = "http://localhost:8080"
    min_score_threshold: float = 70.0
    notify_on_new_match: bool = True
    notify_on_batch_complete: bool = True
    channels: Dict[str, NotificationChannelConfig] = {}
    deduplication_enabled: bool = True
    resend_interval_hours: int = 24
    use_async_queue: bool = True
    redis_url: Optional[str] = None
    rate_limit_max_wait_seconds: int = 300


class AppConfig(BaseModel):
    """Main application configuration."""
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    etl: Optional[EtlConfigWithLlm] = None
    jobspy: Optional[JobSpyConfig] = None
    matching: MatchingConfig = Field(default_factory=MatchingConfig)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)


def _load_yaml_config() -> Dict[str, Any]:
    """Load configuration from YAML file."""
    project_root = Path(__file__).parent.parent
    config_path = project_root / 'config.yaml'
    
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f) or {}
    return {}


def _set_nested(data: dict, keys: list, value: Any) -> None:
    """Helper to set a value in a nested dictionary, creating intermediate dicts as needed."""
    for key in keys[:-1]:
        data = data.setdefault(key, {})
    data[keys[-1]] = value


def _apply_env_overrides(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Apply environment variable overrides to configuration."""
    env_mappings = [
        (["DATABASE_URL"], ["database", "url"]),
        (["WEB_HOST"], ["web", "host"]),
        (["WEB_PORT"], ["web", "port"]),
        (["JOBSPY_URL"], ["jobspy", "url"]),
        (["ETL_LLM_EXTRACTION_BASE_URL", "ETL_LLM_BASE_URL"], ["etl", "llm", "base_url"]),
        (["ETL_LLM_EXTRACTION_API_KEY", "ETL_LLM_API_KEY"], ["etl", "llm", "api_key"]),
        (["ETL_LLM_EXTRACTION_API_SECRET", "ETL_LLM_API_SECRET"], ["etl", "llm", "api_secret"]),
        (["ETL_EMBEDDING_BASE_URL"], ["etl", "llm", "embedding_base_url"]),
        (["ETL_EMBEDDING_API_KEY"], ["etl", "llm", "embedding_api_key"]),
        (["ETL_EMBEDDING_API_SECRET"], ["etl", "llm", "embedding_api_secret"]),
        (["ETL_LLM_EXTRACTION_MODEL"], ["etl", "llm", "extraction_model"]),
        (["ETL_EMBEDDING_MODEL"], ["etl", "llm", "embedding_model"]),
        (["REDIS_URL"], ["notifications", "redis_url"]),
        (["BASE_URL"], ["notifications", "base_url"]),
    ]

    for env_vars, keys in env_mappings:
        val = next((os.environ.get(ev) for ev in env_vars if os.environ.get(ev)), None)
        if val:
            _set_nested(config_dict, keys, val)

    header_mappings = [
        ("ETL_EXTRACTION_MODEL_HEADER_ENV_VARS", ["etl", "llm", "extraction_headers"]),
        ("ETL_EMBEDDING_MODEL_HEADER_ENV_VARS", ["etl", "llm", "embedding_headers"]),
    ]

    for env_var, keys in header_mappings:
        env_val = os.environ.get(env_var)
        if env_val:
            try:
                header_map = json.loads(env_val)
                resolved_headers = {k: os.environ.get(v, "") for k, v in header_map.items()}
                _set_nested(config_dict, keys, resolved_headers)
            except json.JSONDecodeError:
                logger.warning(
                    f"Invalid JSON in {env_var}, expected format: "
                    f'{{"Header-Name": "ENV_VAR_NAME"}}. Headers will not be set.'
                )

    return config_dict


@lru_cache()
def get_config() -> AppConfig:
    """
    Get application configuration with caching.
    
    Loads from YAML file and applies environment variable overrides.
    Result is cached for performance.
    
    Returns:
        AppConfig: The application configuration.
    """
    raw_config = _load_yaml_config()
    raw_config = _apply_env_overrides(raw_config)
    
    return AppConfig(**raw_config)


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent
