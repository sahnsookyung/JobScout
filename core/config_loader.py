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
    provider: Literal["openai_compatible"] = "openai_compatible"
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    extraction_headers: Optional[Dict[str, str]] = None
    extraction_model: Optional[str] = "gpt-4o-mini"
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
    provider: Literal["openai_compatible"] = "openai_compatible"
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


class PreferenceCrossEncoderConfig(BaseModel):
    enabled: bool = False
    # Default is a commonly available open-source CE for quick local testing.
    # Choose the best model for your language, domain, and latency budget.
    model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    cache_path: Optional[str] = None
    runtime: Literal["auto", "heuristic", "flag_embedding", "sentence_transformers"] = "auto"
    max_batch_size: int = 32
    trust_remote_code: bool = False


class PreferencesConfig(BaseModel):
    default_mode: Literal["semantic_rerank", "llm_judge"] = "semantic_rerank"
    allowed_modes: List[Literal["semantic_rerank", "llm_judge"]] = Field(
        default_factory=lambda: ["semantic_rerank"]
    )
    reranker: Literal["llm", "cross_encoder"] = "llm"
    parser: PreferenceModelConfig = Field(default_factory=PreferenceModelConfig)
    semantic_reranker: PreferenceModelConfig = Field(default_factory=PreferenceModelConfig)
    llm_judge: PreferenceModelConfig = Field(
        default_factory=lambda: PreferenceModelConfig(enabled=False)
    )
    cross_encoder: PreferenceCrossEncoderConfig = Field(default_factory=PreferenceCrossEncoderConfig)

    def allowed_modes_normalized(self) -> List[str]:
        """Return deduplicated allowed modes filtered to valid values, falling back to default_mode."""
        _valid = {"semantic_rerank", "llm_judge"}
        normalized = [
            m for m in dict.fromkeys(
                str(x).strip().lower() for x in (self.allowed_modes or [])
            )
            if m in _valid
        ]
        return normalized or [self.default_mode]


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

    def model_post_init(self, __context: Any) -> None:
        del __context
        if not 0.0 <= float(self.similarity_threshold) <= 1.0:
            raise ValueError("matching.matcher.similarity_threshold must be between 0 and 1")
        if self.batch_size is not None and int(self.batch_size) <= 0:
            raise ValueError("matching.matcher.batch_size must be positive when set")
        if self.lexical_limit is not None and int(self.lexical_limit) <= 0:
            raise ValueError("matching.matcher.lexical_limit must be positive when set")
        if int(self.fusion_rank_constant) <= 0:
            raise ValueError("matching.matcher.fusion_rank_constant must be positive")
        if int(self.lexical_query_token_limit) <= 0:
            raise ValueError("matching.matcher.lexical_query_token_limit must be positive")


class SemanticFitSerializationConfig(BaseModel):
    requirement_text_max_chars: int = 500
    evidence_text_max_chars: int = 2500
    evidence_section_max_chars: int = 64
    job_title_max_chars: int = 200
    job_company_max_chars: int = 200
    job_summary_max_chars: int = 1800

    def model_post_init(self, __context: Any) -> None:
        del __context
        positive_fields = (
            "requirement_text_max_chars",
            "evidence_text_max_chars",
            "evidence_section_max_chars",
            "job_title_max_chars",
            "job_company_max_chars",
            "job_summary_max_chars",
        )
        for field_name in positive_fields:
            value = int(getattr(self, field_name))
            if value <= 0:
                raise ValueError(f"matching.scorer.semantic_fit.serialization.{field_name} must be positive")


class SemanticFitCrossEncoderLocalConfig(BaseModel):
    enabled: bool = True
    runtime: Literal["auto", "flag_embedding", "sentence_transformers", "heuristic"] = "auto"
    model_name: str = "BAAI/bge-reranker-v2-m3"
    model_cache_path: Optional[str] = None
    device_policy: Literal["cpu"] = "cpu"
    max_batch_size: int = 32
    max_concurrency: int = 1
    timeout_ms: int = 2000
    trust_remote_code: bool = False

    def model_post_init(self, __context: Any) -> None:
        del __context
        if int(self.max_batch_size) <= 0:
            raise ValueError("matching.scorer.semantic_fit.cross_encoder.local.max_batch_size must be positive")
        if int(self.max_concurrency) <= 0:
            raise ValueError("matching.scorer.semantic_fit.cross_encoder.local.max_concurrency must be positive")
        if int(self.timeout_ms) <= 0:
            raise ValueError("matching.scorer.semantic_fit.cross_encoder.local.timeout_ms must be positive")


class SemanticFitCrossEncoderRemoteConfig(BaseModel):
    enabled: bool = False
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    model: str = "fit-cross-encoder-v1"
    timeout_ms: int = 1500
    max_batch_size: int = 64

    def model_post_init(self, __context: Any) -> None:
        del __context
        if int(self.timeout_ms) <= 0:
            raise ValueError("matching.scorer.semantic_fit.cross_encoder.remote.timeout_ms must be positive")
        if int(self.max_batch_size) <= 0:
            raise ValueError("matching.scorer.semantic_fit.cross_encoder.remote.max_batch_size must be positive")


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

    def model_post_init(self, __context: Any) -> None:
        del __context
        if int(self.timeout_seconds) <= 0:
            raise ValueError("matching.scorer.semantic_fit.llm.timeout_seconds must be positive")
        if int(self.max_input_tokens) <= 0:
            raise ValueError("matching.scorer.semantic_fit.llm.max_input_tokens must be positive")


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
    # Gate for the §B evidence cross-encoder rerank path. Defaults to False
    # so scorer/matcher can ship independently of the rerank rollout; flip to
    # True once the reranker has been validated against production jobs.
    evidence_rerank_enabled: bool = False
    # Opt-in LLM escalation: when on, primary-tier requirements whose rerank
    # evidence_score lands inside the borderline band AND whose threshold /
    # cross-encoder verdicts disagree get re-judged by the LLM scorer. Default
    # off to avoid surprise latency/cost; the reranker alone resolves most
    # confusions, and LLM is a targeted tiebreaker.
    evidence_llm_escalation: bool = False
    evidence_llm_borderline_band: tuple[float, float] = (0.40, 0.65)
    cross_encoder: SemanticFitCrossEncoderConfig = Field(
        default_factory=SemanticFitCrossEncoderConfig
    )
    llm: SemanticFitLlmConfig = Field(default_factory=SemanticFitLlmConfig)
    serialization: SemanticFitSerializationConfig = Field(
        default_factory=SemanticFitSerializationConfig
    )

    def _normalize_allowed_modes(self) -> None:
        deploy_allowed = list(
            dict.fromkeys(
                mode for mode in self.deploy_allowed_modes if mode in {"cross_encoder", "llm"}
            )
        )
        if not deploy_allowed:
            deploy_allowed = [self.default_mode]
        self.deploy_allowed_modes = deploy_allowed

        configured_baseline = list(dict.fromkeys(self.baseline_allowed_modes))
        invalid_baseline_modes = [
            mode for mode in configured_baseline if mode not in self.deploy_allowed_modes
        ]
        if invalid_baseline_modes:
            raise ValueError(
                "matching.scorer.semantic_fit.baseline_allowed_modes contains modes that are not deploy-allowed: "
                + ", ".join(invalid_baseline_modes)
            )
        self.baseline_allowed_modes = configured_baseline or [self.default_mode]

        if self.default_mode not in self.deploy_allowed_modes:
            raise ValueError(
                "matching.scorer.semantic_fit.default_mode must be included in deploy_allowed_modes"
            )
        if self.default_mode not in self.baseline_allowed_modes:
            raise ValueError(
                "matching.scorer.semantic_fit.default_mode must be included in baseline_allowed_modes"
            )

    def _validate_scalar_limits(self) -> None:
        self.recall_top_k = int(self.recall_top_k)
        if self.recall_top_k <= 0:
            raise ValueError("matching.scorer.semantic_fit.recall_top_k must be positive")
        self.cross_encoder.remote_promote_pair_count = int(
            self.cross_encoder.remote_promote_pair_count
        )
        if self.cross_encoder.remote_promote_pair_count <= 0:
            raise ValueError(
                "matching.scorer.semantic_fit.cross_encoder.remote_promote_pair_count must be positive"
            )

    def _validate_cross_encoder_inputs(
        self,
        *,
        route_policy: str,
        local_enabled: bool,
        remote_enabled: bool,
    ) -> None:
        if local_enabled and not str(self.cross_encoder.local.model_name).strip():
            raise ValueError(
                "matching.scorer.semantic_fit.cross_encoder.local.model_name is required when local cross-encoder is enabled"
            )
        if remote_enabled:
            if not str(self.cross_encoder.remote.base_url or "").strip():
                raise ValueError(
                    "matching.scorer.semantic_fit.cross_encoder.remote.base_url is required when remote cross-encoder is enabled"
                )
            if not str(self.cross_encoder.remote.model).strip():
                raise ValueError(
                    "matching.scorer.semantic_fit.cross_encoder.remote.model is required when remote cross-encoder is enabled"
                )

        if route_policy == "local" and not local_enabled:
            raise ValueError(
                "matching.scorer.semantic_fit.cross_encoder.route_policy='local' requires local cross-encoder to be enabled"
            )
        if route_policy == "remote" and not remote_enabled:
            raise ValueError(
                "matching.scorer.semantic_fit.cross_encoder.route_policy='remote' requires remote cross-encoder to be enabled"
            )
        if route_policy == "auto" and not (local_enabled or remote_enabled):
            raise ValueError(
                "matching.scorer.semantic_fit.cross_encoder.route_policy='auto' requires at least one cross-encoder provider to be enabled"
            )

    def _validate_deployable_modes(self) -> None:
        if "llm" in self.deploy_allowed_modes and not self.llm.enabled:
            raise ValueError(
                "matching.scorer.semantic_fit.deploy_allowed_modes includes 'llm' but llm semantic fit is disabled"
            )
        if self.llm.enabled:
            if not str(self.llm.base_url or "").strip():
                raise ValueError(
                    "matching.scorer.semantic_fit.llm.base_url is required when llm semantic fit is enabled"
                )
            if not str(self.llm.model).strip():
                raise ValueError(
                    "matching.scorer.semantic_fit.llm.model is required when llm semantic fit is enabled"
                )

    def model_post_init(self, __context: Any) -> None:
        del __context
        self._normalize_allowed_modes()
        self._validate_scalar_limits()

        if not self.enabled:
            return

        route_policy = self.cross_encoder.route_policy
        local_enabled = bool(self.cross_encoder.local.enabled)
        remote_enabled = bool(self.cross_encoder.remote.enabled)
        self._validate_cross_encoder_inputs(
            route_policy=route_policy,
            local_enabled=local_enabled,
            remote_enabled=remote_enabled,
        )
        self._validate_deployable_modes()


class ScorerConfig(BaseModel):
    """Configuration for the scoring stage."""

    enabled: bool = True
    weight_required: float = 0.7  # required coverage vs job_similarity blend
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
    min_fit_for_alerts: float = 70.0
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


class RankingConfig(BaseModel):
    """Retrieve-then-rerank ranking configuration.

    max_ranking_candidates controls how many rows are fetched from the DB
    (ordered by fit_score DESC) before ranking.  This is the explicit scaling
    boundary: increase it as the job database grows beyond pre-production volumes.

    balanced_w_pref + balanced_w_fit must equal 1.0 (validated at init).
    Initial weights (0.6 / 0.4) are a starting point — tune after rollout.
    """

    config_version: str = "1.0.0"
    active_default_mode: Literal["preference_first", "fit_first", "balanced"] = "balanced"
    balanced_w_pref: float = Field(default=0.6, ge=0.0, le=1.0)
    balanced_w_fit: float = Field(default=0.4, ge=0.0, le=1.0)
    stable_tie_break_key: Literal["job_id", "match_id"] = "match_id"
    max_ranking_candidates: int = Field(default=500, ge=10, le=10_000)
    default_top_k: int = Field(default=25, ge=1, le=500)
    max_top_k: int = Field(default=100, ge=1, le=1_000)
    explanation_labels: Dict[str, str] = Field(
        default_factory=lambda: {
            "preference_first": "Sorted by your soft preference match",
            "fit_first": "Sorted by skill & requirement fit",
            "balanced": "Balanced blend of preference and fit",
        }
    )

    def model_post_init(self, __context: Any) -> None:
        del __context
        total = round(self.balanced_w_pref + self.balanced_w_fit, 10)
        if abs(total - 1.0) > 1e-9:
            raise ValueError(
                f"ranking.balanced_w_pref + ranking.balanced_w_fit must equal 1.0, "
                f"got {self.balanced_w_pref} + {self.balanced_w_fit} = {total}"
            )

    def label_for_mode(self, mode: str) -> str:
        return self.explanation_labels.get(mode, mode)

    def effective_top_k(self, requested: Optional[int]) -> int:
        """Return the effective top_k, applying default and max cap."""
        k = requested if requested is not None else self.default_top_k
        return min(k, self.max_top_k)


class AppConfig(BaseModel):
    database: DatabaseConfig
    jobspy: Optional[JobSpyConfig] = None
    etl: Optional[EtlConfig] = EtlConfig()
    matching: Optional[MatchingConfig] = MatchingConfig()
    preferences: PreferencesConfig = Field(default_factory=PreferencesConfig)
    ranking: RankingConfig = Field(default_factory=RankingConfig)
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
    (["ETL_LLM_PROVIDER"], ["etl", "llm", "provider"]),
    (["ETL_LLM_EXTRACTION_BASE_URL", "ETL_LLM_BASE_URL"], ["etl", "llm", "base_url"]),
    (["ETL_LLM_EXTRACTION_API_KEY", "ETL_LLM_API_KEY"], ["etl", "llm", "api_key"]),
    (["ETL_LLM_EXTRACTION_API_SECRET", "ETL_LLM_API_SECRET"], ["etl", "llm", "api_secret"]),
    (["ETL_EMBEDDING_BASE_URL"], ["etl", "llm", "embedding_base_url"]),
    (["ETL_EMBEDDING_API_KEY"], ["etl", "llm", "embedding_api_key"]),
    (["ETL_EMBEDDING_API_SECRET"], ["etl", "llm", "embedding_api_secret"]),
    (["ETL_LLM_EXTRACTION_MODEL"], ["etl", "llm", "extraction_model"]),
    (["ETL_EMBEDDING_MODEL"], ["etl", "llm", "embedding_model"]),
    (["PREFERENCES_DEFAULT_MODE"], ["preferences", "default_mode"]),
    (["PREFERENCES_PARSER_PROVIDER"], ["preferences", "parser", "provider"]),
    (["PREFERENCES_PARSER_BASE_URL"], ["preferences", "parser", "base_url"]),
    (["PREFERENCES_PARSER_API_KEY"], ["preferences", "parser", "api_key"]),
    (["PREFERENCES_PARSER_API_SECRET"], ["preferences", "parser", "api_secret"]),
    (["PREFERENCES_PARSER_MODEL"], ["preferences", "parser", "model"]),
    (["PREFERENCES_SEMANTIC_RERANKER_PROVIDER"], ["preferences", "semantic_reranker", "provider"]),
    (["PREFERENCES_SEMANTIC_RERANKER_BASE_URL"], ["preferences", "semantic_reranker", "base_url"]),
    (["PREFERENCES_SEMANTIC_RERANKER_API_KEY"], ["preferences", "semantic_reranker", "api_key"]),
    (["PREFERENCES_SEMANTIC_RERANKER_API_SECRET"], ["preferences", "semantic_reranker", "api_secret"]),
    (["PREFERENCES_SEMANTIC_RERANKER_MODEL"], ["preferences", "semantic_reranker", "model"]),
    (["PREFERENCES_LLM_JUDGE_PROVIDER"], ["preferences", "llm_judge", "provider"]),
    (["PREFERENCES_LLM_JUDGE_BASE_URL"], ["preferences", "llm_judge", "base_url"]),
    (["PREFERENCES_LLM_JUDGE_API_KEY"], ["preferences", "llm_judge", "api_key"]),
    (["PREFERENCES_LLM_JUDGE_API_SECRET"], ["preferences", "llm_judge", "api_secret"]),
    (["PREFERENCES_LLM_JUDGE_MODEL"], ["preferences", "llm_judge", "model"]),
    (["PREFERENCES_RERANKER"], ["preferences", "reranker"]),
    (["PREFERENCES_CROSS_ENCODER_ENABLED"], ["preferences", "cross_encoder", "enabled"]),
    (["PREFERENCES_CROSS_ENCODER_MODEL_NAME"], ["preferences", "cross_encoder", "model_name"]),
    (["PREFERENCES_CROSS_ENCODER_CACHE_PATH"], ["preferences", "cross_encoder", "cache_path"]),
    (["PREFERENCES_CROSS_ENCODER_RUNTIME"], ["preferences", "cross_encoder", "runtime"]),
    (["PREFERENCES_CROSS_ENCODER_MAX_BATCH_SIZE"], ["preferences", "cross_encoder", "max_batch_size"]),
    (["PREFERENCES_CROSS_ENCODER_TRUST_REMOTE_CODE"], ["preferences", "cross_encoder", "trust_remote_code"]),
    (["FIT_SEMANTIC_ENABLED"], ["matching", "scorer", "semantic_fit", "enabled"]),
    (["FIT_SEMANTIC_DEFAULT_MODE"], ["matching", "scorer", "semantic_fit", "default_mode"]),
    (["FIT_SEMANTIC_RECALL_TOP_K"], ["matching", "scorer", "semantic_fit", "recall_top_k"]),
    (["EVIDENCE_RERANK_ENABLED"], ["matching", "scorer", "semantic_fit", "evidence_rerank_enabled"]),
    (["EVIDENCE_LLM_ESCALATION"], ["matching", "scorer", "semantic_fit", "evidence_llm_escalation"]),
    (["FIT_CROSS_ENCODER_ROUTE_POLICY"], ["matching", "scorer", "semantic_fit", "cross_encoder", "route_policy"]),
    (["FIT_CROSS_ENCODER_LOCAL_RUNTIME"], ["matching", "scorer", "semantic_fit", "cross_encoder", "local", "runtime"]),
    (["FIT_CROSS_ENCODER_LOCAL_MODEL"], ["matching", "scorer", "semantic_fit", "cross_encoder", "local", "model_name"]),
    (["FIT_CROSS_ENCODER_LOCAL_MODEL_CACHE_PATH"], ["matching", "scorer", "semantic_fit", "cross_encoder", "local", "model_cache_path"]),
    (["FIT_CROSS_ENCODER_LOCAL_TIMEOUT_MS"], ["matching", "scorer", "semantic_fit", "cross_encoder", "local", "timeout_ms"]),
    (["FIT_CROSS_ENCODER_REMOTE_BASE_URL"], ["matching", "scorer", "semantic_fit", "cross_encoder", "remote", "base_url"]),
    (["FIT_CROSS_ENCODER_REMOTE_API_KEY"], ["matching", "scorer", "semantic_fit", "cross_encoder", "remote", "api_key"]),
    (["FIT_CROSS_ENCODER_REMOTE_MODEL"], ["matching", "scorer", "semantic_fit", "cross_encoder", "remote", "model"]),
    (["FIT_LLM_PROVIDER"], ["matching", "scorer", "semantic_fit", "llm", "provider"]),
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
    (["RANKING_DEFAULT_MODE"], ["ranking", "active_default_mode"]),
    (["RANKING_CONFIG_VERSION"], ["ranking", "config_version"]),
    (["RANKING_BALANCED_W_PREF"], ["ranking", "balanced_w_pref"]),
    (["RANKING_BALANCED_W_FIT"], ["ranking", "balanced_w_fit"]),
    (["RANKING_MAX_CANDIDATES"], ["ranking", "max_ranking_candidates"]),
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
