import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

EnvMapping = tuple[Sequence[str], Sequence[str]]
HeaderMapping = tuple[str, Sequence[str]]
ConfigPath = str | os.PathLike[str]
DEFAULT_CONFIG_FILENAME = "config.yaml"


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

class PreferenceModelConfig(BaseModel):
    enabled: bool = True
    provider: str = "openai"
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    model: Optional[str] = None
    temperature: float = 0.0
    timeout_seconds: int = 30
    max_input_tokens: int = 2048
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1024
    embedding_base_url: Optional[str] = None
    embedding_api_key: Optional[str] = None
    embedding_api_secret: Optional[str] = None
    embedding_headers: Optional[Dict[str, str]] = None

class PreferencesConfig(BaseModel):
    default_mode: Literal["semantic_rerank", "llm_judge"] = "semantic_rerank"
    allowed_modes: List[Literal["semantic_rerank", "llm_judge"]] = Field(
        default_factory=lambda: ["semantic_rerank"]
    )
    parser: PreferenceModelConfig = Field(default_factory=PreferenceModelConfig)
    semantic_reranker: PreferenceModelConfig = Field(default_factory=PreferenceModelConfig)
    llm_judge: PreferenceModelConfig = Field(
        default_factory=lambda: PreferenceModelConfig(enabled=False)
    )


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
    hybrid_retrieval_enabled: bool = True
    lexical_limit: Optional[int] = None
    fusion_rank_constant: int = 60
    lexical_query_token_limit: int = 24


class SemanticFitSerializationConfig(BaseModel):
    requirement_text_max_chars: int = 500
    evidence_text_max_chars: int = 2500
    evidence_section_max_chars: int = 64
    job_title_max_chars: int = 200
    job_company_max_chars: int = 200
    job_summary_max_chars: int = 1800


class SemanticFitCrossEncoderLocalConfig(BaseModel):
    enabled: bool = True
    model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    model_cache_path: Optional[str] = None
    device_policy: Literal["cpu"] = "cpu"
    max_batch_size: int = 32
    max_concurrency: int = 1
    timeout_ms: int = 2000


class SemanticFitCrossEncoderRemoteConfig(BaseModel):
    enabled: bool = False
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    model: str = "fit-cross-encoder-v1"
    timeout_ms: int = 1500
    max_batch_size: int = 64


class SemanticFitCrossEncoderConfig(BaseModel):
    route_policy: Literal["local", "remote", "auto"] = "local"
    remote_promote_pair_count: int = 40
    local: SemanticFitCrossEncoderLocalConfig = Field(
        default_factory=SemanticFitCrossEncoderLocalConfig
    )
    remote: SemanticFitCrossEncoderRemoteConfig = Field(
        default_factory=SemanticFitCrossEncoderRemoteConfig
    )


class SemanticFitLlmConfig(BaseModel):
    enabled: bool = False
    provider: Literal["openai_compatible"] = "openai_compatible"
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    model: str = "gpt-4o-mini"
    temperature: float = 0.0
    timeout_seconds: int = 20
    max_input_tokens: int = 4000


class SemanticFitConfig(BaseModel):
    enabled: bool = True
    deploy_allowed_modes: List[Literal["cross_encoder", "llm"]] = Field(
        default_factory=lambda: ["cross_encoder"]
    )
    baseline_allowed_modes: List[Literal["cross_encoder", "llm"]] = Field(
        default_factory=lambda: ["cross_encoder"]
    )
    default_mode: Literal["cross_encoder", "llm"] = "cross_encoder"
    threshold_fallback_enabled: bool = True
    recall_top_k: int = 5
    cross_encoder: SemanticFitCrossEncoderConfig = Field(
        default_factory=SemanticFitCrossEncoderConfig
    )
    llm: SemanticFitLlmConfig = Field(default_factory=SemanticFitLlmConfig)
    serialization: SemanticFitSerializationConfig = Field(
        default_factory=SemanticFitSerializationConfig
    )


class ScorerConfig(BaseModel):
    """Configuration for the scoring stage."""

    enabled: bool = True
    weight_required: float = 0.7
    weight_preferred: float = 0.3
    semantic_fit: SemanticFitConfig = Field(default_factory=SemanticFitConfig)
    semantic_fit_enabled: Optional[bool] = None
    semantic_fit_fallback_to_threshold: Optional[bool] = None

    penalty_missing_required: float = 15.0
    penalty_seniority_mismatch: float = 10.0
    penalty_compensation_mismatch: float = 10.0
    penalty_experience_shortfall: float = 15.0

    wants_remote: bool = True
    min_salary: Optional[int] = None
    target_seniority: Optional[str] = None

    def model_post_init(self, __context: Any) -> None:
        del __context
        if self.semantic_fit_enabled is not None:
            self.semantic_fit.enabled = bool(self.semantic_fit_enabled)
        if self.semantic_fit_fallback_to_threshold is not None:
            self.semantic_fit.threshold_fallback_enabled = bool(
                self.semantic_fit_fallback_to_threshold
            )
        self.semantic_fit_enabled = self.semantic_fit.enabled
        self.semantic_fit_fallback_to_threshold = self.semantic_fit.threshold_fallback_enabled


class MatchingConfig(BaseModel):
    """Top-level matching configuration."""

    enabled: bool = True
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
    preferences: PreferencesConfig = Field(default_factory=PreferencesConfig)
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
    (["PREFERENCES_DEFAULT_MODE"], ["preferences", "default_mode"]),
    (["PREFERENCES_PARSER_BASE_URL"], ["preferences", "parser", "base_url"]),
    (["PREFERENCES_PARSER_API_KEY"], ["preferences", "parser", "api_key"]),
    (["PREFERENCES_PARSER_API_SECRET"], ["preferences", "parser", "api_secret"]),
    (["PREFERENCES_PARSER_MODEL"], ["preferences", "parser", "model"]),
    (["PREFERENCES_SEMANTIC_RERANKER_BASE_URL"], ["preferences", "semantic_reranker", "base_url"]),
    (["PREFERENCES_SEMANTIC_RERANKER_API_KEY"], ["preferences", "semantic_reranker", "api_key"]),
    (["PREFERENCES_SEMANTIC_RERANKER_API_SECRET"], ["preferences", "semantic_reranker", "api_secret"]),
    (["PREFERENCES_SEMANTIC_RERANKER_MODEL"], ["preferences", "semantic_reranker", "model"]),
    (["PREFERENCES_LLM_JUDGE_BASE_URL"], ["preferences", "llm_judge", "base_url"]),
    (["PREFERENCES_LLM_JUDGE_API_KEY"], ["preferences", "llm_judge", "api_key"]),
    (["PREFERENCES_LLM_JUDGE_API_SECRET"], ["preferences", "llm_judge", "api_secret"]),
    (["PREFERENCES_LLM_JUDGE_MODEL"], ["preferences", "llm_judge", "model"]),
    (["FIT_SEMANTIC_ENABLED"], ["matching", "scorer", "semantic_fit", "enabled"]),
    (["FIT_SEMANTIC_DEFAULT_MODE"], ["matching", "scorer", "semantic_fit", "default_mode"]),
    (["FIT_SEMANTIC_RECALL_TOP_K"], ["matching", "scorer", "semantic_fit", "recall_top_k"]),
    (["FIT_CROSS_ENCODER_ROUTE_POLICY"], ["matching", "scorer", "semantic_fit", "cross_encoder", "route_policy"]),
    (["FIT_CROSS_ENCODER_REMOTE_BASE_URL"], ["matching", "scorer", "semantic_fit", "cross_encoder", "remote", "base_url"]),
    (["FIT_CROSS_ENCODER_REMOTE_API_KEY"], ["matching", "scorer", "semantic_fit", "cross_encoder", "remote", "api_key"]),
    (["FIT_CROSS_ENCODER_REMOTE_MODEL"], ["matching", "scorer", "semantic_fit", "cross_encoder", "remote", "model"]),
    (["FIT_LLM_BASE_URL"], ["matching", "scorer", "semantic_fit", "llm", "base_url"]),
    (["FIT_LLM_API_KEY"], ["matching", "scorer", "semantic_fit", "llm", "api_key"]),
    (["FIT_LLM_API_SECRET"], ["matching", "scorer", "semantic_fit", "llm", "api_secret"]),
    (["FIT_LLM_MODEL"], ["matching", "scorer", "semantic_fit", "llm", "model"]),
    (["REDIS_URL"], ["notifications", "redis_url"]),
    (["BASE_URL"], ["notifications", "base_url"]),
    (["NOTIFICATION_RATE_LIMIT_MAX_WAIT"], ["notifications", "rate_limit_max_wait_seconds"]),
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
    ("PREFERENCES_PARSER_HEADER_ENV_VARS", ["preferences", "parser", "headers"]),
    ("PREFERENCES_SEMANTIC_RERANKER_HEADER_ENV_VARS", ["preferences", "semantic_reranker", "headers"]),
    ("PREFERENCES_LLM_JUDGE_HEADER_ENV_VARS", ["preferences", "llm_judge", "headers"]),
    ("FIT_LLM_HEADER_ENV_VARS", ["matching", "scorer", "semantic_fit", "llm", "headers"]),
)


def resolve_config_path(
    config_path: ConfigPath = DEFAULT_CONFIG_FILENAME,
    *,
    fallback_path: ConfigPath | None = None,
) -> Path:
    """Resolve the config file path, falling back to the repository config when needed."""
    resolved = Path(config_path)
    if resolved.exists():
        return resolved

    if fallback_path is not None:
        fallback = Path(fallback_path)
        if fallback.exists():
            return fallback

    return Path(__file__).resolve().parents[1] / DEFAULT_CONFIG_FILENAME


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
    config_path: ConfigPath = DEFAULT_CONFIG_FILENAME,
    *,
    fallback_path: ConfigPath | None = None,
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


def load_config(config_path: ConfigPath = DEFAULT_CONFIG_FILENAME) -> AppConfig:
    data = load_config_data(config_path)
    return AppConfig(**data)
