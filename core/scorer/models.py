"""
Scoring Models - Data structures for scoring results.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from database.models import JobPost


@dataclass
class ScoredJobMatch:
    """Complete scored match result.

    preference_score is None until apply_preference_semantic_reranking() runs.
    None means "evaluator did not run" — distinct from 0.0 ("scored, poor match").
    """
    job: JobPost

    fit_score: float = 0.0
    preference_score: Optional[float] = None  # 0–100 or None (not evaluated)

    fit_components: Dict[str, Any] = field(default_factory=dict)
    preference_components: Dict[str, Any] = field(default_factory=dict)
    fit_confidence: float = 0.0
    fit_explanation: Dict[str, Any] = field(default_factory=dict)
    fit_scorer: Dict[str, Any] = field(default_factory=dict)

    base_score: float = 0.0
    penalties: float = 0.0
    jd_required_coverage: float = 0.0
    jd_preferred_requirement_coverage: float = 0.0
    job_similarity: float = 0.0
    penalty_details: List[Dict[str, Any]] = field(default_factory=list)
    matched_requirements: List['RequirementMatchResult'] = field(default_factory=list)
    missing_requirements: List['RequirementMatchResult'] = field(default_factory=list)
    resume_fingerprint: str = ""
    match_type: str = "requirements_only"
    policy_applied: Optional[Dict[str, Any]] = None
