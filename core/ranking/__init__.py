"""Core ranking package.

Provides the retrieve-then-rerank engine that applies a declared ranking mode
(preference_first, fit_first, balanced) to a bounded candidate pool.

Usage in match_service.py:
    from core.ranking import rank_matches, RankingContext, RankingMode, RankingConfig

    ctx = RankingContext(mode=RankingMode(ranking_mode), config=ranking_cfg)
    ranked = rank_matches(candidate_pool, ctx)
    results = ranked[:ranking_cfg.effective_top_k(requested_top_k)]
"""

from core.ranking.engine import RankingContext, RankingMode, rank_matches
from core.ranking.explainability import RankingExplanation
from core.ranking.policy import RankingConfig, RankingPolicyStore, get_ranking_policy_store

__all__ = [
    "RankingContext",
    "RankingMode",
    "rank_matches",
    "RankingExplanation",
    "RankingConfig",
    "RankingPolicyStore",
    "get_ranking_policy_store",
]
