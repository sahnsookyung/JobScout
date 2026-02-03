import yaml
import os
from typing import List, Optional, Dict, Any, Union, Literal
from pydantic import BaseModel, Field

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
    extraction_model: Optional[str] = "gpt-4o-mini"
    extraction_url: Optional[str] = None  # GLiNER endpoint
    extraction_type: str = "openai"  # "openai" or "gliner"
    extraction_labels: Optional[List[str]] = None  # GLiNER labels
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1024

class EtlConfig(BaseModel):
    llm: Optional[LlmConfig] = LlmConfig()

class DiscoveryModeConfig(BaseModel):
    """
    Configuration for discovery mode ranking (optimize for breadth/recall).
    
    Discovery mode surfaces adjacent fits while preventing weak fits from dominating.
    """
    candidate_pool_size_k: int = 500  # Stage 1 retrieval size
    final_results_n: int = 50  # Stage 2 output size
    
    # Scoring weights (must sum to 1.0)
    job_similarity_weight: float = 0.55
    preferred_coverage_weight: float = 0.25
    preferences_alignment_weight: float = 0.20
    
    # Required coverage factor parameters
    required_coverage_factor_floor: float = 0.25
    required_coverage_factor_power: float = 2.0
    
    # Missing required handling: disabled (default), soft, or hard
    missing_required_policy: Literal["disabled", "soft", "hard"] = "disabled"
    
    # Penalty scaling multiplier
    penalties_multiplier: float = 1.0


class StrictModeConfig(BaseModel):
    """
    Configuration for strict mode ranking (optimize for precision).
    
    Strict mode aggressively filters for high-confidence fits.
    """
    candidate_pool_size_k: int = 200  # Stage 1 retrieval size
    final_results_n: int = 30  # Stage 2 output size
    
    # Scoring weights (must sum to 1.0)
    required_coverage_weight: float = 0.55
    job_similarity_weight: float = 0.25
    preferred_coverage_weight: float = 0.15
    preferences_alignment_weight: float = 0.05
    
    # Required coverage gate
    required_coverage_minimum: float = 0.75  # Minimum required coverage to pass gate
    required_coverage_emphasis_power: float = 3.0  # Exponent for req coverage factor
    
    # Low-fit handling: reject (score=0) or cap (score capped at low_fit_score_cap)
    low_fit_policy: Literal["reject", "cap"] = "reject"
    low_fit_score_cap: float = 50.0  # Maximum score if coverage < minimum and policy is "cap"
    
    # Missing required handling: soft or hard (default enabled_soft)
    missing_required_policy: Literal["disabled", "soft", "hard"] = "soft"
    
    # Penalty scaling multiplier (strict mode penalizes more)
    penalties_multiplier: float = 1.3


class RankingConfig(BaseModel):
    """
    Configuration for two-stage ranking with configurable modes.
    
    Supports "discovery" mode (breadth/recall) and "strict" mode (precision).
    """
    mode: Literal["discovery", "strict"] = "discovery"
    discovery: DiscoveryModeConfig = Field(default_factory=DiscoveryModeConfig)
    strict: StrictModeConfig = Field(default_factory=StrictModeConfig)


class PreferenceWeights(BaseModel):
    """Weights for preference alignment scoring."""
    location: float = 0.35
    company_size: float = 0.15
    industry: float = 0.25
    role: float = 0.25


class Stage1EmbeddingConfig(BaseModel):
    """Configuration for Stage-1 resume embedding generation."""
    
    # Embedding mode: text concatenation vs pooled REU embeddings
    mode: Literal["text", "pooled_reu"] = "pooled_reu"
    
    # Text mode: evidence slice limit (legacy, for backward compat)
    text_evidence_slice_limit: int = 10
    
    # Embedding dimension (fallback if cannot be inferred from embeddings)
    embedding_dim: int = 1024
    
    # Pooled mode: weights for different evidence sections
    section_weights: Dict[str, float] = Field(default_factory=lambda: {
        "summary": 3.0,
        "skills": 2.0,
        "experience": 1.5,
        "projects": 0.5,
        "education": 0.0
    })
    
    # Pooling method
    pooling_method: Literal["mean", "weighted_mean"] = "weighted_mean"
    
    # Include/exclude settings
    include_projects: bool = False
    include_education: bool = False


class MatcherConfig(BaseModel):
    """
    Configuration for the MatcherService (Stage 1: Vector Retrieval).
    
    Handles matching resume evidence to job requirements via vector similarity.
    """
    enabled: bool = True
    similarity_threshold: float = 0.5  # Minimum similarity for a match
    top_k_requirements: int = 3  # Number of best matches to consider per requirement
    embedding_model: Optional[str] = None  # Uses ETL model if not specified
    embedding_dimensions: int = 1024
    batch_size: int = 100  # Number of jobs to process per batch
    include_job_level_matching: bool = True  # Also match at job summary level
    
    # NEW: Stage-1 embedding configuration
    stage1_embedding: Stage1EmbeddingConfig = Field(default_factory=Stage1EmbeddingConfig)
    
    # NEW: Preference weights (was hard-coded in matcher_service)
    preference_weights: PreferenceWeights = Field(default_factory=PreferenceWeights)
    
    # Ranking mode configuration for two-stage pipeline
    ranking: RankingConfig = Field(default_factory=RankingConfig)

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
    """
    Configuration for the ScoringService (Stage 2: Rule-based Scoring).

    Handles calculating final scores with coverage metrics and penalties.
    """
    enabled: bool = True
    # Scoring weights (per A4.3)
    weight_required: float = 0.7
    weight_preferred: float = 0.3

    # NEW: Fit/Want weights for overall score
    # overall_score = fit_weight * fit_score + want_weight * want_score
    fit_weight: float = 0.70
    want_weight: float = 0.30

    # NEW: Facet weights for Want score calculation
    facet_weights: FacetWeights = Field(default_factory=FacetWeights)

    # Penalty amounts
    # Capability penalties (used in Fit score)
    penalty_missing_required: float = 15.0
    penalty_seniority_mismatch: float = 10.0
    penalty_compensation_mismatch: float = 10.0
    penalty_experience_shortfall: float = 15.0

    # Legacy penalties (kept for backward compatibility with old scoring path)
    penalty_location_mismatch: float = 10.0

    # Preferences boost (kept for backward compatibility)
    preferences_boost_max: float = 15.0

    # User preferences (for display-time hard filters)
    wants_remote: bool = True
    min_salary: Optional[int] = None
    max_salary: Optional[int] = None
    target_seniority: Optional[str] = None
    avoid_industries: List[str] = Field(default_factory=list)
    avoid_roles: List[str] = Field(default_factory=list)

class MatchingConfig(BaseModel):
    """
    Top-level matching configuration.
    
    Supports two modes:
    - requirements_only: Match resume to job requirements only
    - with_preferences: Also match job to user preferences
    """
    enabled: bool = True
    mode: str = "requirements_only"  # requirements_only|with_preferences
    
    # Resume file path (relative to config or absolute)
    resume_file: str = "resume.json"
    
    # Preferences file (only used if mode == "with_preferences")
    preferences_file: Optional[str] = None  # e.g., "preferences.json"
    
    # Sub-service configs (can be updated independently)
    matcher: MatcherConfig = MatcherConfig()
    scorer: ScorerConfig = ScorerConfig()
    
    # Invalidation settings
    invalidate_on_job_change: bool = True
    invalidate_on_resume_change: bool = True
    recalculate_existing: bool = False  # If True, recalculate even if match exists

class NotificationChannelConfig(BaseModel):
    """Configuration for a single notification channel."""
    enabled: bool = True
    recipient: Optional[str] = None  # Email, webhook URL, chat ID, etc.
    
class NotificationConfig(BaseModel):
    """
    Configuration for notifications.
    
    Controls when and how users are notified about job matches.
    """
    enabled: bool = False  # Disabled by default - user must opt-in
    
    # User identification
    user_id: Optional[str] = None  # Unique identifier for the user
    
    # Base URL for links in notifications
    base_url: str = "http://localhost:8080"  # Base URL for notification links
    
    # Notification thresholds
    min_score_threshold: float = 70.0  # Only notify for matches above this score
    notify_on_new_match: bool = True  # Notify when new high-score match found
    notify_on_batch_complete: bool = True  # Send summary after each batch
    
    # Channels to use (at least one must be configured)
    channels: Dict[str, NotificationChannelConfig] = {}
    
    # Deduplication settings
    deduplication_enabled: bool = True  # Prevent duplicate notifications
    resend_interval_hours: int = 24  # Minimum hours between resending
    
    # Redis queue settings
    use_async_queue: bool = True  # Use Redis queue for async processing
    redis_url: Optional[str] = None  # Override default Redis URL

class AppConfig(BaseModel):
    database: DatabaseConfig
    jobspy: Optional[JobSpyConfig] = None
    etl: Optional[EtlConfig] = EtlConfig()
    matching: Optional[MatchingConfig] = MatchingConfig()
    notifications: Optional[NotificationConfig] = None
    schedule: ScheduleConfig
    scrapers: List[ScraperConfig]

def load_config(config_path: str = "config.yaml") -> AppConfig:
    # If not found at relative path (e.g. running from root), try absolute or adjusted path
    if not os.path.exists(config_path):
        # Specific fallback for Docker where WORKDIR is /app and config is in /app/config.yaml
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, "..", "config.yaml")

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
    
    # Allow env var override for DB URL
    env_db_url = os.environ.get("DATABASE_URL")
    if env_db_url:
        data['database']['url'] = env_db_url

    # Allow env var override for JobSpy URL
    env_jobspy_url = os.environ.get("JOBSPY_URL")
    if env_jobspy_url:
        if 'jobspy' not in data or data['jobspy'] is None:
            data['jobspy'] = {}
        data['jobspy']['url'] = env_jobspy_url

    # Allow env var override for LLM Base URL
    env_llm_base_url = os.environ.get("ETL_LLM_BASE_URL")
    if env_llm_base_url:
        if 'etl' not in data:
            data['etl'] = {}
        if 'llm' not in data['etl']:
            data['etl']['llm'] = {}
        data['etl']['llm']['base_url'] = env_llm_base_url

    return AppConfig(**data)
