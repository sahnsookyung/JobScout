#!/usr/bin/env python3
"""
Scoring Models - Data structures for scoring results.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from database.models import JobPost


@dataclass
class ScoredJobMatch:
    """Complete scored match result."""
    job: JobPost
    overall_score: float
    base_score: float
    preferences_boost: float
    penalties: float
    required_coverage: float
    preferred_coverage: float
    job_similarity: float
    preferences_alignment: Optional['PreferencesAlignmentScore']
    penalty_details: List[Dict[str, Any]]
    matched_requirements: List['RequirementMatchResult']
    missing_requirements: List['RequirementMatchResult']
    resume_fingerprint: str
    match_type: str
    ranking_mode: str = "discovery"
    score_components: Optional[Dict[str, Any]] = field(default=None)
