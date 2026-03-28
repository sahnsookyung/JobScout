import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Union

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

EnvMapping = tuple[Sequence[str], Sequence[str]]
HeaderMapping = tuple[str, Sequence[str]]


class ScraperConfig(BaseModel):
    site_type: List[str]
    search_term: Optional[str] = None
    location: Optional[str] = None
    country: Optional[str] = None
    results_wanted: int = 10
    hours_old: Optional[int] = None
    options: Optional[Dict[str, Any]] = {}


class ScheduleConfig(BaseModel):
    interval_seconds: int = 3600


class DatabaseConfig(BaseModel):
    url: str


class JobSpyConfig(BaseModel):
    url: str
    poll_interval_seconds: int = 10
    job_timeout_seconds: int = 300
    request_timeout_seconds: int = 30


class LlmConfig(BaseModel):
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    extraction_headers: Optional[Dict[str, str]] = None
    extraction_model: Optional[str] = "gpt-4o-mini"
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


class ResumeConfig(BaseModel):
    """Configuration for resume processing in ETL."""

    resume_file: str = "resume.json"
    force_re_extraction: bool = False


class EtlConfig(BaseModel):
    llm: Optional[LlmConfig] = LlmConfig()
    resume: Optional[ResumeConfig] = None
    resume_file: Optional[str] = None


class ResultPolicy(BaseModel):
    """Post-scoring result filtering and truncation policy."""

    min_fit: float = 0.0
    top_k: int = 100
    min_jd_required_coverage: Optional[float] = None


class MatcherConfig(BaseModel):
    """Configuration for vector retrieval."""

    enabled: bool = True
    similarity_threshold: float = 0.5
    batch_size: Optional[int] = None


class FacetWeights(BaseModel):
    """Weights for each facet in Want score calculation."""

    remote_flexibility: float = 0.15
    compensation: float = 0.20
    learning_growth: float = 0.15
    company_culture: float = 0.15
    work_life_balance: float = 0.15
    tech_stack: float = 0.10
    visa_sponsorship: float = 0.10


class ScorerConfig(BaseModel):
    """Configuration for the scoring stage."""

    enabled: bool = True
    weight_required: float = 0.7
    weight_preferred: float = 0.3

    fit_weight: float = 0.80
    want_weight: float = 0.20

    facet_weights: FacetWeights = Field(default_factory=FacetWeights)

    penalty_missing_required: float = 15.0
    penalty_seniority_mismatch: float = 10.0
    penalty_compensation_mismatch: float = 10.0
    penalty_experience_shortfall: float = 15.0

    wants_remote: bool = True
    min_salary: Optional[int] = None
    target_seniority: Optional[str] = None


class MatchingConfig(BaseModel):
    """Top-level matching configuration."""

    enabled: bool = True
    user_wants_file: Optional[str] = None
    matcher: MatcherConfig = MatcherConfig()
    scorer: ScorerConfig = ScorerConfig()
    result_policy: ResultPolicy = Field(default_factory=ResultPolicy)
    invalidate_on_job_change: bool = True
    invalidate_on_resume_change: bool = True
    recalculate_existing: bool = False


class NotificationChannelConfig(BaseModel):
    """Configuration for a single notification channel."""

    enabled: bool = True
    recipient: Optional[str] = None


class NotificationSmtpConfig(BaseModel):
    """SMTP configuration for email notifications."""

    server: Optional[str] = None
    port: int = 587
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = True
    from_email: Optional[str] = None


class NotificationConfig(BaseModel):
    """Configuration for notifications."""

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
    dry_run: bool = False
    telegram_bot_token: Optional[str] = None
    smtp: NotificationSmtpConfig = Field(default_factory=NotificationSmtpConfig)


class AppConfig(BaseModel):
    database: DatabaseConfig
    jobspy: Optional[JobSpyConfig] = None
    etl: Optional[EtlConfig] = EtlConfig()
    matching: Optional[MatchingConfig] = MatchingConfig()
    notifications: Optional[NotificationConfig] = NotificationConfig()
    schedule: ScheduleConfig
    scrapers: List[ScraperConfig] = Field(default_factory=list)


def _set_nested(data: dict, keys: list, value: Any) -> None:
    """Set a value in a nested dictionary, creating intermediate dicts as needed."""
    for key in keys[:-1]:
        data = data.setdefault(key, {})
    data[keys[-1]] = value


DEFAULT_ENV_MAPPINGS: tuple[EnvMapping, ...] = (
    (["DATABASE_URL"], ["database", "url"]),
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
    (["NOTIFICATION_EMAIL", "EMAIL"], ["notifications", "channels", "email", "recipient"]),
    (["DISCORD_WEBHOOK_URL"], ["notifications", "channels", "discord", "recipient"]),
    (["TELEGRAM_CHAT_ID"], ["notifications", "channels", "telegram", "recipient"]),
    (["NOTIFICATION_WEBHOOK_URL"], ["notifications", "channels", "webhook", "recipient"]),
    (["TELEGRAM_BOT_TOKEN"], ["notifications", "telegram_bot_token"]),
    (["SMTP_SERVER"], ["notifications", "smtp", "server"]),
    (["SMTP_PORT"], ["notifications", "smtp", "port"]),
    (["SMTP_USERNAME"], ["notifications", "smtp", "username"]),
    (["SMTP_PASSWORD"], ["notifications", "smtp", "password"]),
    (["SMTP_USE_TLS"], ["notifications", "smtp", "use_tls"]),
    (["FROM_EMAIL"], ["notifications", "smtp", "from_email"]),
    (["NOTIFICATION_DRY_RUN"], ["notifications", "dry_run"]),
)

DEFAULT_HEADER_MAPPINGS: tuple[HeaderMapping, ...] = (
    ("ETL_EXTRACTION_MODEL_HEADER_ENV_VARS", ["etl", "llm", "extraction_headers"]),
    ("ETL_EMBEDDING_MODEL_HEADER_ENV_VARS", ["etl", "llm", "embedding_headers"]),
)


def resolve_config_path(
    config_path: Union[str, os.PathLike[str]] = "config.yaml",
    *,
    fallback_path: Optional[Union[str, os.PathLike[str]]] = None,
) -> Path:
    """Resolve the config file path, falling back to the repository config when needed."""
    resolved = Path(config_path)
    if resolved.exists():
        return resolved

    if fallback_path is not None:
        fallback = Path(fallback_path)
        if fallback.exists():
            return fallback

    return Path(__file__).resolve().parents[1] / "config.yaml"


def apply_env_overrides(
    data: Dict[str, Any],
    *,
    env_mappings: Sequence[EnvMapping] = DEFAULT_ENV_MAPPINGS,
    header_mappings: Sequence[HeaderMapping] = DEFAULT_HEADER_MAPPINGS,
) -> Dict[str, Any]:
    """Apply environment-variable overrides to a raw config dictionary."""
    for env_vars, keys in env_mappings:
        val = next((os.environ.get(env_var) for env_var in env_vars if os.environ.get(env_var)), None)
        if val:
            _set_nested(data, list(keys), val)

    for env_var, keys in header_mappings:
        env_val = os.environ.get(env_var)
        if env_val:
            header_map = json.loads(env_val)
            resolved_headers = {k: os.environ.get(v, "") for k, v in header_map.items()}
            _set_nested(data, list(keys), resolved_headers)

    return data


def load_config_data(
    config_path: Union[str, os.PathLike[str]] = "config.yaml",
    *,
    fallback_path: Optional[Union[str, os.PathLike[str]]] = None,
    env_mappings: Sequence[EnvMapping] = DEFAULT_ENV_MAPPINGS,
    header_mappings: Sequence[HeaderMapping] = DEFAULT_HEADER_MAPPINGS,
) -> Dict[str, Any]:
    """Load YAML configuration and apply environment overrides."""
    resolved_path = resolve_config_path(config_path, fallback_path=fallback_path)
    with open(resolved_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return apply_env_overrides(
        data,
        env_mappings=env_mappings,
        header_mappings=header_mappings,
    )


def load_config(config_path: str = "config.yaml") -> AppConfig:
    data = load_config_data(config_path)
    return AppConfig(**data)
