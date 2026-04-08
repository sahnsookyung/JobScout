#!/usr/bin/env python3
"""
Scoring Module - Stage 2: Fit Scoring.

Public API:
- ScoringService: Main scoring service orchestrator
- ScoredJobMatch: Dataclass for scored match results

The scoring module is refactored from the original scorer_service.py
into focused, single-responsibility modules:

- models.py: Data structures (ScoredJobMatch)
- fit_score.py: Fit aggregation over required coverage, similarity, and penalties
- penalties.py: Penalty calculations (location, seniority, compensation, experience)
- persistence.py: Database operations (save_match_to_db)
- service.py: ScoringService orchestrator
"""

from core.scorer.models import ScoredJobMatch
from core.scorer.semantic_fit import (
    LLMSemanticFitScorer,
    SemanticFitScorer,
    SemanticFitScoreResult,
    ThresholdSemanticFitScorer,
)
from core.scorer.service import ScoringService

__all__ = [
    'ScoringService',
    'ScoredJobMatch',
    'LLMSemanticFitScorer',
    'SemanticFitScorer',
    'SemanticFitScoreResult',
    'ThresholdSemanticFitScorer',
]
