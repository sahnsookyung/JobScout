"""Query, ranking, pagination, and presentation helpers for match lists."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.ranking import RankingContext, RankingMode, rank_matches as _default_rank_matches
from web.backend.models.responses import MatchSummary

DEFAULT_ALL_TIER_PAGE_LIMIT = 100
MAX_MATCH_PAGE_LIMIT = 500


@dataclass
class MatchSummaryCandidate:
    """Selection-run backed candidate used for read-time presentation reranking."""

    id: str
    job_id: str
    title: str
    company: str
    location: Optional[str]
    is_remote: bool
    fit_score: Optional[float]
    preference_score: Optional[float]
    job_similarity: Optional[float]
    penalties: Optional[float]
    required_coverage: Optional[float]
    preferred_requirement_coverage: Optional[float]
    match_type: str
    is_hidden: bool
    created_at: Any
    calculated_at: Any
    ranking_explanation: Any = None
    selection_tier: str = "primary"
    excluded_reason: Optional[str] = None
    llm_evaluation: Any = None
    resume_fingerprint: Optional[str] = None
    job_post_id: Optional[str] = None
    job_content_hash: Optional[str] = None
    llm_original_rank: Optional[int] = None
    llm_reranked_rank: Optional[int] = None
    llm_effective_for_rerank: bool = False
    llm_ignored_for_rerank_reason: Optional[str] = None
    llm_stale_status: Optional[str] = None
    llm_rerank_score: Optional[float] = None
    llm_rerank_confidence: Optional[float] = None


class MatchQueryBuilder:
    """Canonical DB retrieval boundary for match list queries."""

    def __init__(self, service: Any) -> None:
        self.service = service

    def resolve_canonical_selection(self, *, owner_id: Optional[Any], tenant_id: Optional[Any]):
        return self.service._resolve_canonical_selection(owner_id=owner_id, tenant_id=tenant_id)

    def load_rankable_pool(
        self,
        canonical_selection,
        *,
        status: str,
        min_fit: Optional[float],
        remote_only: bool,
        show_hidden: bool,
        tier: str,
        tenant_id: Optional[Any],
    ) -> List[MatchSummaryCandidate]:
        return self.service._load_rankable_pool(
            canonical_selection,
            status=status,
            min_fit=min_fit,
            remote_only=remote_only,
            show_hidden=show_hidden,
            tier=tier,
            tenant_id=tenant_id,
        )


class MatchRankingService:
    """Deterministic ranking plus optional LLM display-time reranking."""

    def __init__(
        self,
        service: Any,
        *,
        rank_matches_func: Optional[Callable[[List[Any], RankingContext], Any]] = None,
    ) -> None:
        self.service = service
        self.rank_matches_func = rank_matches_func or _default_rank_matches

    def rank(
        self,
        pool: List[MatchSummaryCandidate],
        *,
        mode: RankingMode,
        ranking_config,
        top_k: Optional[int],
        tier: str,
        owner_id: Optional[Any],
        tenant_id: Optional[Any],
    ) -> tuple[List[MatchSummaryCandidate], Dict[str, Any]]:
        primary_pool = [
            candidate for candidate in pool
            if getattr(candidate, "selection_tier", "primary") == "primary"
        ]
        excluded_pool = [
            candidate for candidate in pool
            if getattr(candidate, "selection_tier", "primary") != "primary"
        ]
        ctx = RankingContext(mode=mode, config=ranking_config)
        self.rank_matches_func(primary_pool, ctx)
        for index, candidate in enumerate(primary_pool, start=1):
            setattr(candidate, "llm_original_rank", index)

        self.service._attach_latest_evaluations(
            primary_pool,
            owner_id=owner_id,
            tenant_id=tenant_id,
        )
        llm_rerank_metadata = self.service._apply_llm_rerank(
            primary_pool,
            owner_id=owner_id,
            tenant_id=tenant_id,
        )

        if tier == "all":
            self.service._attach_latest_evaluations(
                excluded_pool,
                owner_id=owner_id,
                tenant_id=tenant_id,
            )
            ranked = primary_pool + excluded_pool
            if top_k is not None:
                ranked = ranked[:ranking_config.effective_top_k(top_k)]
            return ranked, llm_rerank_metadata

        effective_k = ranking_config.effective_top_k(top_k)
        return primary_pool[:effective_k], llm_rerank_metadata


class MatchPagination:
    """Pagination normalization and slicing for ranked matches."""

    @staticmethod
    def normalize_limit(*, tier: str, limit: Optional[int]) -> Optional[int]:
        if limit is None:
            return DEFAULT_ALL_TIER_PAGE_LIMIT if tier == "all" else None
        return max(1, min(int(limit), MAX_MATCH_PAGE_LIMIT))

    @staticmethod
    def page(matches: List[Any], *, limit: Optional[int], offset: int) -> List[Any]:
        start = max(0, int(offset or 0))
        if limit is None:
            return matches[start:]
        return matches[start:start + limit]


class MatchSummaryPresenter:
    """Presentation adapter from rank candidates to API response models."""

    def __init__(self, service: Any) -> None:
        self.service = service

    def present(self, matches: List[MatchSummaryCandidate]) -> List[MatchSummary]:
        return [self.service._to_match_summary(match) for match in matches]
