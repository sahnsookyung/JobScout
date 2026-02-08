#!/usr/bin/env python3
"""
Configuration management for JobScout web application.
"""

import os
import yaml
from pathlib import Path
from functools import lru_cache
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


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


class MatchingConfig(BaseModel):
    """Matching configuration."""
    scorer: ScorerConfig = Field(default_factory=ScorerConfig)


class AppConfig(BaseModel):
    """Main application configuration."""
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    matching: MatchingConfig = Field(default_factory=MatchingConfig)


def _load_yaml_config() -> Dict[str, Any]:
    """Load configuration from YAML file."""
    project_root = Path(__file__).parent.parent
    config_path = project_root / 'config.yaml'
    
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f) or {}
    return {}


def _apply_env_overrides(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Apply environment variable overrides to configuration."""
    # Database overrides
    if 'DATABASE_URL' in os.environ:
        if 'database' not in config_dict:
            config_dict['database'] = {}
        config_dict['database']['url'] = os.environ['DATABASE_URL']
    
    # Web server overrides
    if 'WEB_HOST' in os.environ:
        if 'web' not in config_dict:
            config_dict['web'] = {}
        config_dict['web']['host'] = os.environ['WEB_HOST']
    
    if 'WEB_PORT' in os.environ:
        if 'web' not in config_dict:
            config_dict['web'] = {}
        config_dict['web']['port'] = int(os.environ['WEB_PORT'])
    
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
