#!/usr/bin/env python3
"""
Scoring Module - Stage 2: Rule-based Scoring.

Public API:
- ScoringService: Main scoring service orchestrator
- ScoredJobMatch: Dataclass for scored match results

The scoring module is refactored from the original scorer_service.py
into focused, single-responsibility modules:

- models.py: Data structures (ScoredJobMatch)
- coverage.py: Coverage calculations (required/preferred coverage)
- scoring_modes.py: Discovery and strict mode scoring formulas
- similarity.py: Multi-embedding similarity matching
- penalties.py: Penalty calculations (location, seniority, compensation, experience)
- persistence.py: Database operations (save_match_to_db)
- service.py: ScoringService orchestrator
"""

from core.scorer.models import ScoredJobMatch
from core.scorer.service import ScoringService

__all__ = ['ScoringService', 'ScoredJobMatch']
