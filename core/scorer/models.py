#!/usr/bin/env python3
"""
Scoring Models - Data structures for scoring results.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from database.models import JobPost


@dataclass
class ScoredJobMatch:
    """Complete scored match result with Fit/Want/Overall scores."""
    job: JobPost

    fit_score: float = 0.0
    want_score: float = 0.0
    overall_score: float = 0.0

    fit_components: Dict[str, Any] = field(default_factory=dict)
    want_components: Dict[str, Any] = field(default_factory=dict)

    base_score: float = 0.0
    preferences_boost: float = 0.0
    penalties: float = 0.0
    required_coverage: float = 0.0
    preferred_coverage: float = 0.0
    job_similarity: float = 0.0
    preferences_alignment: Optional['PreferencesAlignmentScore'] = None
    penalty_details: List[Dict[str, Any]] = field(default_factory=list)
    matched_requirements: List['RequirementMatchResult'] = field(default_factory=list)
    missing_requirements: List['RequirementMatchResult'] = field(default_factory=list)
    resume_fingerprint: str = ""
    match_type: str = "requirements_only"
    ranking_mode: str = "discovery"
    score_components: Optional[Dict[str, Any]] = field(default_factory=dict)
