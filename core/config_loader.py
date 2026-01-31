import yaml
import os
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel

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

class AppConfig(BaseModel):
    database: DatabaseConfig
    jobspy: Optional[JobSpyConfig] = None
    etl: Optional[EtlConfig] = EtlConfig()
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
