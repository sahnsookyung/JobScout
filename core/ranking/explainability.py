"""Ranking explanation data structure."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class RankingExplanation:
    """Per-match metadata produced by rank_matches(), derived from actual ranking logic.

    preference_score is preserved as None when the evaluator was not run or failed —
    it is never substituted with 0.0 here (that substitution only happens in the
    balanced blend, and is recorded in missing_scores).

    dominant_reason_code reflects the ranking path, not a qualitative score assessment:
      "preference_first"     — sorted by preference score
      "fit_first"            — sorted by fit score
      "balanced_blend"       — sorted by w_pref*pref + w_fit*fit blend
      "preference_unavailable" — preference_score was NULL; fallback applied
    """

    ranking_mode_used: str
    config_version: str
    preference_score: Optional[float]   # None when evaluator did not run
    fit_score: float                    # normalised [0, 1]
    similarity_score: float             # normalised [0, 1]
    balanced_primary_score: Optional[float] = None   # set only for "balanced" mode
    dominant_reason_code: str = ""
    explanation_label: str = ""
    missing_scores: List[str] = field(default_factory=list)
