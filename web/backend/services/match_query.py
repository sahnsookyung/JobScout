"""Query, ranking, pagination, and presentation helpers for match lists."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import uuid

from core.ranking import RankingContext, RankingMode, rank_matches as _default_rank_matches
from database.models import JobMatch, JobPost, MatchSelectionItem
from sqlalchemy import and_, func, or_, select
from web.backend.models.responses import MatchSummary
from web.backend.services.cursors import MatchCursorCodec
from web.backend.utils import safe_float, safe_str

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
    selection_item_id: Optional[str] = None
    rank_position: Optional[int] = None
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


@dataclass
class MatchListReadPage:
    candidates: List[MatchSummaryCandidate]
    total: int
    limit: int
    offset: int
    has_more: bool
    next_cursor: Optional[str]
    llm_rerank: Dict[str, Any]
    rows_loaded: int
    rank_source: str = "selection_run"


class MatchListReadService:
    """Bounded cursor-capable read path for canonical match lists."""

    def __init__(self, service: Any) -> None:
        self.service = service

    def load_cursor_page(
        self,
        canonical_selection,
        *,
        status: str,
        min_fit: Optional[float],
        top_k: Optional[int],
        remote_only: bool,
        show_hidden: bool,
        tier: str,
        tenant_id: Optional[Any],
        owner_id: Optional[Any],
        limit: Optional[int],
        cursor: Optional[str],
        include_llm: bool,
        ranking_config: Any,
    ) -> MatchListReadPage:
        decoded = MatchCursorCodec.decode(cursor, expected_kind="matches")
        after_rank = int(decoded.get("rank", 0)) if decoded else 0
        after_id = str(decoded.get("id", "")) if decoded else ""
        try:
            after_uuid = uuid.UUID(after_id) if after_id else None
        except ValueError:
            after_uuid = None
        base_limit = self._cursor_limit(tier=tier, limit=limit, top_k=top_k, ranking_config=ranking_config)

        llm_policy = self.service._llm_policy_metadata(owner_id=owner_id)
        active_top_n = int(llm_policy.get("top_n", 0) or 0) if include_llm and not decoded else 0
        effective_limit = max(base_limit, active_top_n) if active_top_n > 0 else base_limit
        effective_limit = max(1, min(effective_limit, MAX_MATCH_PAGE_LIMIT))

        filter_top_k = top_k
        if active_top_n > 0:
            filter_top_k = max(int(top_k or 0), active_top_n)
        filters = self._filters(
            canonical_selection.selection_run_id,
            status=status,
            min_fit=min_fit,
            remote_only=remote_only,
            show_hidden=show_hidden,
            tier=tier,
            tenant_id=tenant_id,
            top_k=filter_top_k,
            ranking_config=ranking_config,
        )
        total = int(
            self.service.db.execute(
                select(func.count(MatchSelectionItem.id)).join(
                    JobMatch,
                    JobMatch.id == MatchSelectionItem.job_match_id,
                ).join(
                    JobPost,
                    JobPost.id == JobMatch.job_post_id,
                ).where(*filters)
            ).scalar_one() or 0
        )

        page_filters = list(filters)
        if after_rank > 0:
            if after_uuid is not None:
                page_filters.append(
                    or_(
                        MatchSelectionItem.rank_position > after_rank,
                        and_(
                            MatchSelectionItem.rank_position == after_rank,
                            MatchSelectionItem.id > after_uuid,
                        ),
                    )
                )
            else:
                page_filters.append(MatchSelectionItem.rank_position > after_rank)

        stmt = (
            select(
                MatchSelectionItem.id.label("selection_item_id"),
                MatchSelectionItem.rank_position,
                MatchSelectionItem.fit_score_at_selection,
                MatchSelectionItem.preference_score_at_selection,
                MatchSelectionItem.job_similarity_at_selection,
                MatchSelectionItem.required_coverage_at_selection,
                MatchSelectionItem.selection_tier,
                MatchSelectionItem.excluded_reason,
                JobMatch.id.label("match_id"),
                JobMatch.job_post_id,
                JobMatch.penalties,
                JobMatch.preferred_requirement_coverage,
                JobMatch.match_type,
                JobMatch.is_hidden,
                JobMatch.created_at,
                JobMatch.calculated_at,
                JobMatch.resume_fingerprint,
                JobMatch.job_content_hash,
                JobPost.title,
                JobPost.company,
                JobPost.location_text,
                JobPost.is_remote,
            )
            .join(JobMatch, JobMatch.id == MatchSelectionItem.job_match_id)
            .join(JobPost, JobPost.id == JobMatch.job_post_id)
            .where(*page_filters)
            .order_by(MatchSelectionItem.rank_position.asc(), MatchSelectionItem.id.asc())
            .limit(effective_limit + 1)
        )
        rows = list(self.service.db.execute(stmt).all())
        has_more = len(rows) > effective_limit
        rows_for_page = rows[:effective_limit]
        candidates = [self._row_to_candidate(row) for row in rows_for_page]

        llm_rerank = self.service._empty_llm_rerank_metadata(reason="not_requested")
        if include_llm and not decoded:
            primary = [
                candidate
                for candidate in candidates
                if getattr(candidate, "selection_tier", "primary") == "primary"
            ]
            for index, candidate in enumerate(primary, start=1):
                candidate.llm_original_rank = index
            self.service._attach_latest_evaluations(
                primary,
                owner_id=owner_id,
                tenant_id=tenant_id,
            )
            llm_rerank = self.service._apply_llm_rerank(
                primary,
                owner_id=owner_id,
                tenant_id=tenant_id,
                page_mode="cursor",
                policy_metadata=llm_policy,
            )
            if tier == "primary":
                candidates = primary
            else:
                excluded = [
                    candidate
                    for candidate in candidates
                    if getattr(candidate, "selection_tier", "primary") != "primary"
                ]
                candidates = primary + excluded
        elif include_llm:
            llm_rerank = self.service._empty_llm_rerank_metadata(
                reason="cursor_after_rerank_window"
            )

        next_cursor = None
        if has_more and candidates:
            cursor_candidate = max(
                candidates,
                key=lambda item: (
                    int(getattr(item, "rank_position", 0) or 0),
                    str(getattr(item, "selection_item_id", "") or ""),
                ),
            )
            next_cursor = MatchCursorCodec.encode(
                "matches",
                rank=int(getattr(cursor_candidate, "rank_position", 0) or 0),
                id=str(getattr(cursor_candidate, "selection_item_id", "") or ""),
            )

        return MatchListReadPage(
            candidates=candidates,
            total=total,
            limit=effective_limit,
            offset=after_rank,
            has_more=has_more,
            next_cursor=next_cursor,
            llm_rerank=llm_rerank,
            rows_loaded=len(rows),
        )

    @staticmethod
    def _cursor_limit(
        *,
        tier: str,
        limit: Optional[int],
        top_k: Optional[int],
        ranking_config: Any,
    ) -> int:
        if limit is not None:
            return max(1, min(int(limit), MAX_MATCH_PAGE_LIMIT))
        if tier == "all":
            return DEFAULT_ALL_TIER_PAGE_LIMIT
        return max(1, min(int(ranking_config.effective_top_k(top_k)), MAX_MATCH_PAGE_LIMIT))

    @staticmethod
    def _filters(
        selection_run_id: Any,
        *,
        status: str,
        min_fit: Optional[float],
        remote_only: bool,
        show_hidden: bool,
        tier: str,
        tenant_id: Optional[Any],
        top_k: Optional[int],
        ranking_config: Any,
    ) -> list[Any]:
        filters: list[Any] = [MatchSelectionItem.selection_run_id == selection_run_id]
        if tier == "primary":
            filters.append(MatchSelectionItem.selection_tier == "primary")
            if status != "all":
                filters.append(JobMatch.status == status)
            if not show_hidden:
                filters.append(or_(JobMatch.is_hidden.is_(False), JobMatch.is_hidden.is_(None)))
        else:
            if status != "all":
                filters.append(
                    or_(
                        MatchSelectionItem.selection_tier != "primary",
                        JobMatch.status == status,
                    )
                )
            if not show_hidden:
                filters.append(
                    or_(
                        MatchSelectionItem.selection_tier != "primary",
                        JobMatch.is_hidden.is_(False),
                        JobMatch.is_hidden.is_(None),
                    )
                )
        if min_fit is not None:
            filters.append(MatchSelectionItem.fit_score_at_selection >= min_fit)
        if remote_only:
            filters.append(JobPost.is_remote.is_(True))
        if tenant_id is not None:
            filters.append(JobPost.tenant_id == tenant_id)
        if top_k is not None:
            filters.append(
                MatchSelectionItem.rank_position <= ranking_config.effective_top_k(top_k)
            )
        return filters

    @staticmethod
    def _row_to_candidate(row: Any) -> MatchSummaryCandidate:
        return MatchSummaryCandidate(
            id=str(row.match_id),
            job_id=str(row.job_post_id),
            title=safe_str(row.title, "Unknown"),
            company=safe_str(row.company, "Unknown"),
            location=row.location_text,
            is_remote=bool(row.is_remote),
            fit_score=None if row.fit_score_at_selection is None else float(row.fit_score_at_selection),
            preference_score=(
                None
                if row.preference_score_at_selection is None
                else float(row.preference_score_at_selection)
            ),
            job_similarity=safe_float(row.job_similarity_at_selection),
            penalties=None if row.penalties is None else safe_float(row.penalties),
            required_coverage=safe_float(row.required_coverage_at_selection),
            preferred_requirement_coverage=(
                None
                if row.preferred_requirement_coverage is None
                else safe_float(row.preferred_requirement_coverage)
            ),
            match_type=safe_str(row.match_type, "unknown"),
            is_hidden=bool(row.is_hidden),
            created_at=row.created_at,
            calculated_at=row.calculated_at,
            selection_tier=row.selection_tier or "primary",
            excluded_reason=row.excluded_reason,
            resume_fingerprint=row.resume_fingerprint,
            job_post_id=str(row.job_post_id),
            job_content_hash=row.job_content_hash,
            selection_item_id=str(row.selection_item_id),
            rank_position=int(row.rank_position or 0),
        )
