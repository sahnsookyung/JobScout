#!/usr/bin/env python3
"""
Preferences Boost - Calculate bonus score from preferences alignment.

Good preference matches get a boost to overall score up to preferences_boost_max.
"""

from typing import Optional, Dict, Any, Tuple
import logging

from core.config_loader import ScorerConfig
from core.matcher import PreferencesAlignmentScore

logger = logging.getLogger(__name__)


def calculate_preferences_boost(
    preferences_alignment: Optional[PreferencesAlignmentScore],
    config: ScorerConfig
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate bonus score from preferences alignment.

    Good preference matches get a boost to overall score.

    Returns: (boost_amount, boost_details)
    """
    if not preferences_alignment:
        return 0.0, {'reason': 'No preferences provided', 'boost': 0.0}

    # Calculate boost based on overall alignment
    # Scale alignment score (0.0-1.0) to boost range
    alignment = preferences_alignment.overall_score

    # Non-linear scaling: higher alignment gets disproportionately more boost
    # Score 0.5 -> small boost
    # Score 0.8 -> good boost
    # Score 1.0 -> max boost
    if alignment >= 0.9:
        boost = config.preferences_boost_max
    elif alignment >= 0.75:
        boost = config.preferences_boost_max * 0.7
    elif alignment >= 0.6:
        boost = config.preferences_boost_max * 0.4
    elif alignment >= 0.5:
        boost = config.preferences_boost_max * 0.2
    else:
        boost = 0.0

    details = {
        'reason': f"Preferences alignment: {alignment:.2f}",
        'boost': boost,
        'alignment_breakdown': {
            'location': preferences_alignment.location_match,
            'company_size': preferences_alignment.company_size_match,
            'industry': preferences_alignment.industry_match,
            'role': preferences_alignment.role_match
        }
    }

    return boost, details
