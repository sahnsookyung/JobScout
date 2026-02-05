#!/usr/bin/env python3
"""
Coverage Calculations - Required and preferred coverage metrics.

Calculates what percentage of job requirements are covered by the candidate's resume.
"""

from typing import List, Tuple
import logging

from core.config_loader import ScorerConfig
from core.matcher import RequirementMatchResult

logger = logging.getLogger(__name__)


def calculate_coverage(
    matched_requirements: List[RequirementMatchResult],
    missing_requirements: List[RequirementMatchResult]
) -> Tuple[float, float]:
    """
    Calculate required and preferred coverage percentages.

    Returns: (required_coverage, preferred_coverage)
    """
    all_reqs = matched_requirements + missing_requirements

    required_total = len([r for r in all_reqs if r.requirement.req_type == 'required'])
    required_covered = len([m for m in matched_requirements if m.requirement.req_type == 'required'])

    preferred_total = len([r for r in all_reqs if r.requirement.req_type == 'preferred'])
    preferred_covered = len([m for m in matched_requirements if m.requirement.req_type == 'preferred'])

    required_coverage = required_covered / required_total if required_total > 0 else 0.0
    preferred_coverage = preferred_covered / preferred_total if preferred_total > 0 else 0.0

    return required_coverage, preferred_coverage
