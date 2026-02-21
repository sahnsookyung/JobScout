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
    extraction_temperature: float = 0.0  # Temperature for extraction (0.0 = deterministic)


class ResumeConfig(BaseModel):
    """Configuration for resume processing in ETL."""
    # Resume file path (relative to config or absolute)
    resume_file: str = "resume.json"

    # Force re-extraction of resume instead of using pre-extracted data from storage.
    # When enabled, always runs LLM extraction regardless of fingerprint match.
    # Useful for validating extraction behavior or testing new extraction models.
    force_re_extraction: bool = False


class EtlConfig(BaseModel):
    llm: Optional[LlmConfig] = LlmConfig()
    resume: Optional[ResumeConfig] = None  # Resume processing configuration
    resume_file: Optional[str] = None  # Backward compatibility: use resume.resume_file instead


class ResultPolicy(BaseModel):
    """Post-scoring result filtering and truncation policy.

    Applied after scoring to filter and truncate results.
    """
    min_fit: float = 0.0  # 0-100, filter threshold
    top_k: int = 100  # Maximum results to return
    min_jd_required_coverage: Optional[float] = None  # 0-1, optional gate


class MatcherConfig(BaseModel):
    """
    Configuration for the MatcherService (Stage 1: Vector Retrieval).
    
    Handles matching resume evidence to job requirements via vector similarity.
    """
    enabled: bool = True
    similarity_threshold: float = 0.5  # Minimum similarity for a match
    batch_size: Optional[int] = None  # None = process all jobs

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

    # User preferences (for display-time hard filters)
    wants_remote: bool = True
    min_salary: Optional[int] = None
    target_seniority: Optional[str] = None


class MatchingConfig(BaseModel):
    """
    Top-level matching configuration.
    """
    enabled: bool = True

    # User wants file for Want score (semantic matching via embeddings)
    # Free-text file, one want per line. Example:
    #   I want remote work flexibility
    #   Looking for Python and TypeScript roles
    #   Company that values work-life balance
    user_wants_file: Optional[str] = None  # e.g., "wants.txt"

    # Sub-service configs (can be updated independently)
    matcher: MatcherConfig = MatcherConfig()
    scorer: ScorerConfig = ScorerConfig()

    # Result policy for post-scoring filtering and truncation
    result_policy: ResultPolicy = Field(default_factory=ResultPolicy)

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
    
    # Rate limiting settings
    rate_limit_max_wait_seconds: int = 300  # Max seconds to wait on rate limit (5 min)


class AppConfig(BaseModel):
    database: DatabaseConfig
    jobspy: Optional[JobSpyConfig] = None
    etl: Optional[EtlConfig] = EtlConfig()
    matching: Optional[MatchingConfig] = MatchingConfig()
    notifications: Optional[NotificationConfig] = NotificationConfig()
    schedule: ScheduleConfig
    scrapers: List[ScraperConfig] = Field(default_factory=list)

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

    # Allow env var override for Redis URL
    env_redis_url = os.environ.get("REDIS_URL")
    if env_redis_url:
        if 'notifications' not in data:
            data['notifications'] = {}
        data['notifications']['redis_url'] = env_redis_url

    return AppConfig(**data)
