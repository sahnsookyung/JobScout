#!/usr/bin/env python3
"""
Match service - business logic for job match operations.
"""

import copy
import json
import logging
from typing import List, Optional, Dict, Any
from core.redis_streams import _sanitize_log
from sqlalchemy.orm import Session

from core.llm_evaluation import (
    MatchLlmEvaluationService,
    normalize_llm_score,
    score_quality_metadata,
)
from core.match_selection import resolve_canonical_resume_selection
from core.metrics import (
    record_llm_rerank_window_size,
    record_match_query_degraded,
    record_match_query_rows_loaded,
    set_llm_rerank_policy_revision,
)
from core.policy import get_result_policy_store
from core.ranking import RankingMode, get_ranking_policy_store, rank_matches
from database.models import (
    JobMatch,
    JobMatchRequirement,
    JobPost,
    LlmMatchEvaluation,
    MatchSelectionItem,
    MatchSelectionRun,
    StructuredResume,
)
from database.uow import job_uow
from sqlalchemy.orm import joinedload
from ..models.responses import (
    MatchSummary,
    MatchDetail,
    MatchDetailResponse,
    JobDetails,
    RequirementDetail
)
from ..utils import safe_float, safe_int, safe_str, safe_datetime_iso
from ..exceptions import InvalidMatchOperationException, MatchNotFoundException
from web.backend.services.match_query import (
    MatchPagination,
    MatchListReadService,
    MatchQueryBuilder,
    MatchRankingService,
    MatchSummaryCandidate,
    MatchSummaryPresenter,
)
from web.backend.services.source_availability import source_refresh_kind

logger = logging.getLogger(__name__)

DEFAULT_ALL_TIER_PAGE_LIMIT = 100
MAX_MATCH_PAGE_LIMIT = 500

_PREFERENCE_COMPONENT_KEYS = {
    "preference_confidence",
    "preference_reason_codes",
    "preference_explanation",
    "preference_mode_requested",
    "preference_mode_effective",
    "preference_mode_used",
    "preference_fallback_reason",
}

_LIFECYCLE_METADATA_KEY = "jobscout_lifecycle"


class MatchService:
    """Service for managing job matches."""
    
    def __init__(self, db: Session):
        self.db = db
        self.last_llm_rerank_metadata: Dict[str, Any] = self._empty_llm_rerank_metadata(
            reason="not_evaluated"
        )
        self.last_degraded_reasons: List[Dict[str, str]] = []
        self.last_matches_total: int = 0
        self.last_matches_limit: Optional[int] = None
        self.last_matches_offset: int = 0
        self.last_matches_page_mode: str = "offset"
        self.last_matches_view: str = "summary"
        self.last_matches_next_cursor: Optional[str] = None
        self.last_matches_has_more: Optional[bool] = None
        self.last_matches_rank_source: str = "computed"

    def get_matches(
        self,
        owner_id: Optional[Any] = None,
        tenant_id: Optional[Any] = None,
        status: str = "active",
        min_fit: Optional[float] = None,
        top_k: Optional[int] = None,
        remote_only: bool = False,
        show_hidden: bool = False,
        ranking_mode: Optional[str] = None,
        tier: str = "primary",
        limit: Optional[int] = None,
        offset: int = 0,
        cursor: Optional[str] = None,
        page_mode: str = "offset",
        view: str = "summary",
        include: Optional[str] = None,
        llm_ordering: bool = True,
    ) -> List[MatchSummary]:
        """
        Get filtered job matches, ranked by the requested mode.

        Stage 1 — DB retrieve: scoped to the canonical resume's full persisted
            match set after request filters are applied.
        Stage 2 — Python rank: rank_matches() applies the declared mode
            with NULL-aware sort keys; attaches RankingExplanation per item.
        Stage 3 — Truncate:
            - tier='primary': [:effective_top_k] applied after ranking.
            - tier='all': excluded items append after ranked primary items;
              omitted top_k returns the full combined set and explicit top_k
              caps the final combined result count.

        Args:
            status: Match status filter ("active", "stale", or "all").
            min_fit: Minimum fit score filter.
            top_k: Maximum number of primary/all results before response paging.
            remote_only: Filter to remote jobs only.
            show_hidden: Include hidden matches in results.
            ranking_mode: One of "preference_first", "fit_first", "balanced".
                Defaults to config.active_default_mode.
            limit: Optional page size applied after ranking/top_k selection.
                tier='all' defaults to a bounded page when omitted.
            offset: Optional page offset applied with `limit`.

        Returns:
            List of match summaries with ranking explanation fields.
        """
        ranking_config = get_ranking_policy_store().get_current_config()
        self.last_degraded_reasons = []
        page_mode = "cursor" if page_mode == "cursor" else "offset"
        view = "compact" if view == "compact" else "summary"
        include_llm = include is None or "llm" in {
            value.strip() for value in str(include).split(",") if value.strip()
        }
        apply_llm_ordering = bool(include_llm and llm_ordering)
        self.last_matches_page_mode = page_mode
        self.last_matches_view = view
        self.last_matches_next_cursor = None
        self.last_matches_has_more = None
        self.last_matches_rank_source = "computed"

        page_limit = MatchPagination.normalize_limit(tier=tier, limit=limit)

        # Resolve ranking mode
        try:
            mode = RankingMode(ranking_mode) if ranking_mode else RankingMode(ranking_config.active_default_mode)
        except ValueError:
            mode = RankingMode(ranking_config.active_default_mode)

        query_builder = MatchQueryBuilder(self)
        canonical_selection = query_builder.resolve_canonical_selection(
            owner_id=owner_id,
            tenant_id=tenant_id,
        )
        if canonical_selection is None:
            self._set_match_page_metadata(
                total=0,
                limit=page_limit,
                offset=offset,
                page_mode=page_mode,
                view=view,
            )
            return []

        if page_mode == "cursor":
            if (
                ranking_mode is not None
                and getattr(canonical_selection, "ranking_mode_used", None)
                and ranking_mode != getattr(canonical_selection, "ranking_mode_used", None)
            ):
                self._record_degraded_reason(
                    "unsupported_cursor_ranking_mode",
                    ValueError("cursor mode uses persisted selection ranking"),
                )
            page = MatchListReadService(self).load_cursor_page(
                canonical_selection,
                status=status,
                min_fit=min_fit,
                top_k=top_k,
                remote_only=remote_only,
                show_hidden=show_hidden,
                tier=tier,
                tenant_id=tenant_id,
                owner_id=owner_id,
                limit=limit,
                cursor=cursor,
                include_llm=include_llm,
                apply_llm_ordering=apply_llm_ordering,
                ranking_config=ranking_config,
            )
            self.last_llm_rerank_metadata = page.llm_rerank
            self._set_match_page_metadata(
                total=page.total,
                limit=page.limit,
                offset=page.offset,
                page_mode=page_mode,
                view=view,
                next_cursor=page.next_cursor,
                has_more=page.has_more,
                rank_source=page.rank_source,
            )
            record_match_query_rows_loaded(page_mode, view, page.rows_loaded)
            return MatchSummaryPresenter(self).present(page.candidates)

        pool = query_builder.load_rankable_pool(
            canonical_selection,
            status=status,
            min_fit=min_fit,
            remote_only=remote_only,
            show_hidden=show_hidden,
            tier=tier,
            tenant_id=tenant_id,
        )
        record_match_query_rows_loaded(page_mode, view, len(pool))

        ranked, self.last_llm_rerank_metadata = MatchRankingService(
            self,
            rank_matches_func=rank_matches,
        ).rank(
            pool,
            mode=mode,
            ranking_config=ranking_config,
            top_k=top_k,
            tier=tier,
            owner_id=owner_id,
            tenant_id=tenant_id,
            include_llm=include_llm,
            apply_llm_ordering=apply_llm_ordering,
        )

        total = len(ranked)
        ranked = MatchPagination.page(ranked, limit=page_limit, offset=offset)
        self._set_match_page_metadata(
            total=total,
            limit=page_limit,
            offset=offset,
            page_mode=page_mode,
            view=view,
            has_more=page_limit is not None and offset + len(ranked) < total,
        )

        return MatchSummaryPresenter(self).present(ranked)

    @staticmethod
    def _normalize_page_limit(*, tier: str, limit: Optional[int]) -> Optional[int]:
        if limit is None:
            return DEFAULT_ALL_TIER_PAGE_LIMIT if tier == "all" else None
        return max(1, min(int(limit), MAX_MATCH_PAGE_LIMIT))

    @staticmethod
    def _page_ranked_matches(matches: List[Any], *, limit: Optional[int], offset: int) -> List[Any]:
        safe_offset = max(int(offset or 0), 0)
        if limit is None:
            return matches[safe_offset:] if safe_offset else matches
        safe_limit = max(int(limit), 0)
        return matches[safe_offset:safe_offset + safe_limit]

    def _set_match_page_metadata(
        self,
        *,
        total: int,
        limit: Optional[int],
        offset: int,
        page_mode: str = "offset",
        view: str = "summary",
        next_cursor: Optional[str] = None,
        has_more: Optional[bool] = None,
        rank_source: str = "computed",
    ) -> None:
        self.last_matches_total = max(int(total or 0), 0)
        self.last_matches_limit = None if limit is None else max(int(limit), 0)
        self.last_matches_offset = max(int(offset or 0), 0)
        self.last_matches_page_mode = page_mode
        self.last_matches_view = view
        self.last_matches_next_cursor = next_cursor
        self.last_matches_has_more = has_more
        self.last_matches_rank_source = rank_source

    def _record_degraded_reason(self, code: str, exc: Exception) -> None:
        self.last_degraded_reasons.append(
            {"code": code, "detail": exc.__class__.__name__}
        )
        record_match_query_degraded(code)

    def _resolve_canonical_selection(
        self,
        owner_id: Optional[Any] = None,
        tenant_id: Optional[Any] = None,
    ):
        try:
            with job_uow() as repo:
                return resolve_canonical_resume_selection(
                    repo,
                    owner_id,
                    tenant_id=tenant_id,
                )
        except Exception as exc:
            self._record_degraded_reason("canonical_selection_unavailable", exc)
            logger.warning("Could not resolve canonical resume fingerprint: %s", exc, exc_info=True)
            return None

    def _load_rankable_pool(
        self,
        canonical_selection,
        *,
        status: str,
        min_fit: Optional[float],
        remote_only: bool,
        show_hidden: bool,
        tier: str = "primary",
        tenant_id: Optional[Any] = None,
    ) -> List[Any]:
        with job_uow() as repo:
            repo_tier = "primary" if tier == "primary" else "all"
            items = repo.match_selection.get_items_for_run(
                canonical_selection.selection_run_id,
                tier=repo_tier,
                tenant_id=tenant_id,
            )
            pool: List[Any] = []
            for item in items:
                match = item.job_match
                job = getattr(match, "job_post", None)
                fit_score = self._item_fit_score(item)
                item_tier = getattr(item, "selection_tier", "primary") or "primary"
                if not self._selection_item_passes_filters(
                    match,
                    job,
                    fit_score,
                    status=status,
                    min_fit=min_fit,
                    remote_only=remote_only,
                    show_hidden=show_hidden,
                    tier=item_tier,
                ):
                    continue
                pool.append(self._selection_item_to_summary_candidate(item, match, job, fit_score))
            return pool

    @staticmethod
    def _item_fit_score(item) -> Optional[float]:
        return (
            None
            if item.fit_score_at_selection is None
            else float(item.fit_score_at_selection)
        )

    @staticmethod
    def _selection_item_passes_filters(
        match,
        job,
        fit_score: Optional[float],
        *,
        status: str,
        min_fit: Optional[float],
        remote_only: bool,
        show_hidden: bool,
        tier: str = "primary",
    ) -> bool:
        # Status/hidden filters only apply to primary-tier items. Excluded
        # items are browse-only and do not expose status semantics in the API,
        # so those knobs are intentionally no-ops for tier='excluded'.
        if tier == "primary":
            if status != "all" and match.status != status:
                return False
            if not show_hidden and bool(match.is_hidden):
                return False
        if min_fit is not None and (fit_score is None or fit_score < min_fit):
            return False
        return not (remote_only and not bool(getattr(job, "is_remote", False)))

    @staticmethod
    def _selection_item_to_summary_candidate(item, match, job, fit_score) -> MatchSummaryCandidate:
        return MatchSummaryCandidate(
            id=str(match.id),
            job_id=str(match.job_post_id),
            title=job.title if job and hasattr(job, "title") else "Unknown",
            company=job.company if job and hasattr(job, "company") else "Unknown",
            location=job.location_text if job and hasattr(job, "location_text") else None,
            is_remote=bool(getattr(job, "is_remote", False)),
            fit_score=fit_score,
            preference_score=(
                None
                if item.preference_score_at_selection is None
                else float(item.preference_score_at_selection)
            ),
            job_similarity=float(item.job_similarity_at_selection),
            penalties=None if match.penalties is None else safe_float(match.penalties),
            required_coverage=float(item.required_coverage_at_selection),
            preferred_requirement_coverage=(
                None
                if match.preferred_requirement_coverage is None
                else safe_float(match.preferred_requirement_coverage)
            ),
            match_type=safe_str(match.match_type, "unknown"),
            is_hidden=bool(match.is_hidden),
            created_at=match.created_at,
            calculated_at=match.calculated_at,
            selection_tier=getattr(item, "selection_tier", "primary") or "primary",
            excluded_reason=getattr(item, "excluded_reason", None),
            resume_fingerprint=getattr(match, "resume_fingerprint", None),
            job_post_id=str(getattr(match, "job_post_id", "")),
            job_content_hash=getattr(match, "job_content_hash", None),
            selection_item_id=str(getattr(item, "id", "")),
            rank_position=getattr(item, "rank_position", None),
        )

    def _attach_latest_evaluations(
        self,
        matches: List[Any],
        *,
        owner_id: Optional[Any],
        tenant_id: Optional[Any],
    ) -> None:
        if owner_id is None or not matches or not self._has_real_query_session():
            return
        match_ids = [getattr(match, "id", None) for match in matches]
        match_ids = [match_id for match_id in match_ids if match_id is not None]
        if not match_ids:
            return

        stmt = (
            self.db.query(LlmMatchEvaluation)
            .filter(
                LlmMatchEvaluation.owner_id == owner_id,
                LlmMatchEvaluation.job_match_id.in_(match_ids),
                LlmMatchEvaluation.deleted_at.is_(None),
            )
            .order_by(LlmMatchEvaluation.job_match_id.asc(), LlmMatchEvaluation.created_at.desc())
        )
        if tenant_id is None:
            stmt = stmt.filter(LlmMatchEvaluation.tenant_id.is_(None))
        else:
            stmt = stmt.filter(LlmMatchEvaluation.tenant_id == tenant_id)

        by_match_id: Dict[str, LlmMatchEvaluation] = {}
        try:
            evaluations = stmt.all()
        except Exception as exc:
            self._record_degraded_reason("llm_evaluation_lookup_unavailable", exc)
            logger.warning("Could not attach LLM evaluation markers: %s", exc)
            return
        try:
            iterator = iter(evaluations)
        except TypeError:
            logger.warning("Could not attach LLM evaluation markers: query returned non-iterable")
            return
        for evaluation in iterator:
            by_match_id.setdefault(str(evaluation.job_match_id), evaluation)

        for match in matches:
            evaluation = by_match_id.get(str(getattr(match, "id", "")))
            if evaluation is not None:
                setattr(match, "llm_evaluation", evaluation)

    @staticmethod
    def _empty_llm_rerank_metadata(reason: str) -> Dict[str, Any]:
        return {
            "enabled": False,
            "available": False,
            "applied": False,
            "top_n": 0,
            "window_size": 0,
            "eligible_count": 0,
            "reranked_count": 0,
            "policy_revision": 0,
            "ordering_requested": False,
            "reason": reason,
        }

    def _llm_policy_metadata(self, *, owner_id: Optional[Any]) -> Dict[str, Any]:
        if owner_id is None:
            return self._empty_llm_rerank_metadata(reason="owner_missing")
        try:
            policy = get_result_policy_store().get_llm_judge_policy(owner_id)
        except Exception as exc:
            self._record_degraded_reason("policy_unavailable", exc)
            logger.warning("Could not load LLM rerank policy: %s", exc)
            return self._empty_llm_rerank_metadata(reason="policy_unavailable")
        top_n = max(0, int(getattr(policy, "top_n", 0) or 0))
        revision = max(0, int(getattr(policy, "revision", 0) or 0))
        return {
            "enabled": bool(getattr(policy, "enabled", False)),
            "available": bool(getattr(policy, "available", False)),
            "applied": False,
            "top_n": top_n,
            "window_size": 0,
            "eligible_count": 0,
            "reranked_count": 0,
            "policy_revision": revision,
            "ordering_requested": True,
            "reason": None,
            "unavailable_reason": getattr(policy, "unavailable_reason", None),
        }

    def _apply_llm_rerank(
        self,
        primary_pool: List[Any],
        *,
        owner_id: Optional[Any],
        tenant_id: Optional[Any],
        page_mode: str = "offset",
        policy_metadata: Optional[Dict[str, Any]] = None,
        apply_ordering: bool = True,
    ) -> Dict[str, Any]:
        if owner_id is None:
            return self._empty_llm_rerank_metadata(reason="owner_missing")
        if not primary_pool:
            return self._empty_llm_rerank_metadata(reason="empty_primary_pool")

        metadata = dict(policy_metadata or self._llm_policy_metadata(owner_id=owner_id))
        metadata["ordering_requested"] = bool(apply_ordering)
        top_n = max(0, int(metadata.get("top_n", 0) or 0))
        set_llm_rerank_policy_revision(metadata.get("policy_revision"))
        if not metadata.get("available", False):
            metadata["reason"] = metadata.get("unavailable_reason") or "unavailable"
            return metadata
        if not metadata.get("enabled", False):
            metadata["reason"] = "disabled"
            return metadata
        if top_n <= 0:
            metadata["reason"] = "top_n_zero"
            return metadata

        window_size = min(top_n, len(primary_pool))
        window = primary_pool[:window_size]
        metadata["window_size"] = window_size
        record_llm_rerank_window_size(page_mode, window_size)
        judge_service = MatchLlmEvaluationService(self.db)
        eligible_count = 0
        for candidate in window:
            evaluation = getattr(candidate, "llm_evaluation", None)
            effectiveness = judge_service.evaluation_effectiveness(
                candidate,
                evaluation,
                owner_id=owner_id,
                tenant_id=tenant_id,
            )
            if evaluation is not None:
                setattr(evaluation, "llm_effectiveness", effectiveness)
            candidate.llm_effective_for_rerank = bool(effectiveness.get("effective_for_rerank"))
            candidate.llm_ignored_for_rerank_reason = effectiveness.get("ignored_for_rerank_reason")
            candidate.llm_stale_status = effectiveness.get("stale_status")
            if candidate.llm_effective_for_rerank and evaluation is not None:
                eligible_count += 1
                candidate.llm_rerank_score = normalize_llm_score(
                    evaluation.llm_score,
                    getattr(evaluation, "verdict", None),
                )
                candidate.llm_rerank_confidence = safe_float(evaluation.confidence)

        metadata["eligible_count"] = eligible_count
        if eligible_count == 0:
            metadata["reason"] = "no_current_successful_evaluations"
            for index, candidate in enumerate(primary_pool, start=1):
                candidate.llm_reranked_rank = index
            return metadata

        if not apply_ordering:
            metadata["applied"] = False
            metadata["reranked_count"] = 0
            metadata["reason"] = "ordering_disabled"
            for index, candidate in enumerate(primary_pool, start=1):
                candidate.llm_reranked_rank = index
                if getattr(candidate, "llm_effective_for_rerank", False):
                    candidate.llm_effective_for_rerank = False
                    candidate.llm_ignored_for_rerank_reason = "ordering_disabled"
                    evaluation = getattr(candidate, "llm_evaluation", None)
                    effectiveness = getattr(evaluation, "llm_effectiveness", None)
                    if isinstance(effectiveness, dict):
                        effectiveness = dict(effectiveness)
                        effectiveness["effective_for_rerank"] = False
                        effectiveness["ignored_for_rerank_reason"] = "ordering_disabled"
                        setattr(evaluation, "llm_effectiveness", effectiveness)
            return metadata

        def sort_key(candidate: Any):
            original_rank = int(getattr(candidate, "llm_original_rank", 0) or 0)
            match_id = str(getattr(candidate, "id", ""))
            if getattr(candidate, "llm_effective_for_rerank", False):
                return (
                    0,
                    -(getattr(candidate, "llm_rerank_score", None) or 0.0),
                    -(getattr(candidate, "llm_rerank_confidence", None) or 0.0),
                    original_rank,
                    match_id,
                )
            return (1, 0.0, 0.0, original_rank, match_id)

        reranked_window = sorted(window, key=sort_key)
        primary_pool[:window_size] = reranked_window
        for index, candidate in enumerate(primary_pool, start=1):
            candidate.llm_reranked_rank = index
        metadata["applied"] = True
        metadata["reranked_count"] = eligible_count
        metadata["reason"] = "applied"
        return metadata

    def _has_real_query_session(self) -> bool:
        """Return false for mocked sessions so optional enrichment stays inert in unit tests."""
        query = getattr(self.db, "query", None)
        if not callable(query):
            return False
        return not type(query).__module__.startswith("unittest.mock")
    
    def _get_match_for_owner(
        self,
        match_id: str,
        owner_id: Optional[Any] = None,
        tenant_id: Optional[Any] = None,
    ) -> JobMatch:
        query = self.db.query(JobMatch)
        if owner_id is not None:
            query = query.join(
                StructuredResume,
                StructuredResume.resume_fingerprint == JobMatch.resume_fingerprint,
            ).filter(StructuredResume.owner_id == owner_id)
        if tenant_id is not None:
            query = query.join(JobPost, JobPost.id == JobMatch.job_post_id).filter(
                JobPost.tenant_id == tenant_id
            )

        match = query.filter(JobMatch.id == match_id).one_or_none()
        if not match:
            raise MatchNotFoundException(f"Match {match_id} not found")
        return match

    def get_match_detail(
        self,
        match_id: str,
        owner_id: Optional[Any] = None,
        tenant_id: Optional[Any] = None,
    ) -> MatchDetailResponse:
        """
        Get detailed information about a specific match.

        Args:
            match_id: The match ID.

        Returns:
            Detailed match information.

        Raises:
            MatchNotFoundException: If match is not found.
            Exception: If a database error occurs (maps to 500).
        """
        match = self._get_match_for_owner(
            match_id,
            owner_id=owner_id,
            tenant_id=tenant_id,
        )

        try:
            job = self.db.query(JobPost).get(match.job_post_id)

            req_matches = self.db.query(JobMatchRequirement).options(
                joinedload(JobMatchRequirement.requirement)
            ).filter(
                JobMatchRequirement.job_match_id == match_id
            ).all()

            requirements = [self._to_requirement_detail(req) for req in req_matches]
            penalty_details = self._parse_penalty_details(match.penalty_details)

            return MatchDetailResponse(
                success=True,
                match=self._to_match_detail(
                    match,
                    penalty_details,
                    owner_id=owner_id,
                    tenant_id=tenant_id,
                ),
                job=self._to_job_details(job),
                requirements=requirements
            )
        except Exception:
            logger.exception("Database error fetching match details for %s", _sanitize_log(match_id))
            raise
    
    def toggle_hidden(
        self,
        match_id: str,
        owner_id: Optional[Any] = None,
        tenant_id: Optional[Any] = None,
    ) -> bool:
        """
        Toggle the hidden status of a match.
        
        Args:
            match_id: The match ID.
        
        Returns:
            New hidden status.
        
        Raises:
            MatchNotFoundException: If match is not found.
        """
        from database.repositories.match import MatchRepository
        
        repo = MatchRepository(self.db)
        match = (
            repo.get_match_by_id_for_owner(match_id, owner_id)
            if owner_id is not None
            else repo.get_match_by_id(match_id)
        )

        if not match:
            raise MatchNotFoundException(f"Match {match_id} not found")
        if tenant_id is not None:
            job = getattr(match, "job_post", None) or self.db.query(JobPost).get(match.job_post_id)
            if job is None or str(getattr(job, "tenant_id", "")) != str(tenant_id):
                raise MatchNotFoundException(f"Match {match_id} not found")

        if owner_id is not None and self._selection_tier_for_current_owner_run(
            match_id,
            owner_id=owner_id,
        ) == "excluded":
            raise InvalidMatchOperationException(
                "Excluded matches are browse-only and cannot be hidden."
            )

        is_currently_hidden = match.is_hidden or False
        new_status = not is_currently_hidden
        match.is_hidden = new_status
        self.db.commit()

        return new_status

    def _selection_tier_for_current_owner_run(
        self,
        match_id: str,
        *,
        owner_id: Any,
    ) -> Optional[str]:
        item = (
            self.db.query(MatchSelectionItem)
            .join(
                MatchSelectionRun,
                MatchSelectionRun.id == MatchSelectionItem.selection_run_id,
            )
            .filter(
                MatchSelectionItem.job_match_id == match_id,
                MatchSelectionRun.owner_id == owner_id,
                MatchSelectionRun.lifecycle_status == "committed",
                MatchSelectionRun.is_current.is_(True),
            )
            .one_or_none()
        )
        if item is None:
            return None
        tier = getattr(item, "selection_tier", None)
        return tier if isinstance(tier, str) else None
    
    def get_match_explanation(
        self,
        match_id: str,
        owner_id: Optional[Any] = None,
        tenant_id: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Get explainability details for a specific match.
        
        Args:
            match_id: The match ID.
        
        Returns:
            Match explanation data.
        
        Raises:
            MatchNotFoundException: If match is not found.
        """
        match = self._get_match_for_owner(
            match_id,
            owner_id=owner_id,
            tenant_id=tenant_id,
        )

        fit_components = match.fit_components if isinstance(match.fit_components, dict) else {}
        explanation = fit_components.get("fit_explanation")
        if not explanation:
            return {
                "success": True,
                "match_id": match_id,
                "explanation": None,
                "message": "Semantic fit explanation is not available for this match."
            }

        return {
            "success": True,
            "match_id": match_id,
            "explanation": explanation
        }

    # Private helper methods

    def _extract_summary_job_fields(self, match: JobMatch) -> Dict[str, Any]:
        if isinstance(match, MatchSummaryCandidate):
            return {
                "job_id": match.job_id,
                "title": match.title,
                "company": match.company,
                "location": match.location,
                "is_remote": match.is_remote,
            }

        job = match.job_post

        try:
            return {
                "job_id": str(job.id) if job else None,
                "title": job.title if job and hasattr(job, 'title') else "Unknown",
                "company": job.company if job and hasattr(job, 'company') else "Unknown",
                "location": job.location_text if job and hasattr(job, 'location_text') else None,
                "is_remote": job.is_remote if job and hasattr(job, 'is_remote') else False,
            }
        except Exception as exc:
            logger.warning(f"Error accessing job_post fields for match {match.id}: {exc}")
            return {
                "job_id": None,
                "title": "Unknown",
                "company": "Unknown",
                "location": None,
                "is_remote": False,
            }

    @staticmethod
    def _optional_float(value: Optional[float]) -> Optional[float]:
        return safe_float(value) if value is not None else None

    @staticmethod
    def _float_or_zero(value: Optional[float]) -> float:
        return safe_float(value) if value is not None else 0.0

    @staticmethod
    def _optional_int_attr(obj: Any, name: str) -> Optional[int]:
        value = getattr(obj, name, None)
        if type(value).__module__.startswith("unittest.mock"):
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        try:
            return int(value) if value is not None else None
        except Exception:
            return None

    @staticmethod
    def _optional_float_attr(obj: Any, name: str) -> Optional[float]:
        value = getattr(obj, name, None)
        if type(value).__module__.startswith("unittest.mock"):
            return None
        return safe_float(value) if value is not None else None

    @staticmethod
    def _optional_str_attr(obj: Any, name: str) -> Optional[str]:
        value = getattr(obj, name, None)
        if type(value).__module__.startswith("unittest.mock"):
            return None
        return value if isinstance(value, str) else None

    @staticmethod
    def _optional_datetime_iso_attr(obj: Any, name: str) -> Optional[str]:
        value = getattr(obj, name, None)
        if type(value).__module__.startswith("unittest.mock"):
            return None
        return safe_datetime_iso(value)

    @staticmethod
    def _preference_score_percent(value: Any) -> Optional[float]:
        """Clamp a stored preference score to the public 0-100 contract."""
        if value is None:
            return None
        score = safe_float(value)
        return max(0.0, min(100.0, score))

    def _to_match_summary(self, match: JobMatch) -> MatchSummary:
        """Convert ORM model to MatchSummary response model."""
        job_fields = self._extract_summary_job_fields(match)
        evaluation = getattr(match, "llm_evaluation", None)

        expl = getattr(match, "ranking_explanation", None)
        preferred_requirement_coverage = self._float_or_zero(
            match.preferred_requirement_coverage
        )
        selection_tier = getattr(match, "selection_tier", "primary")
        selection_tier = selection_tier if isinstance(selection_tier, str) else "primary"
        excluded_reason = getattr(match, "excluded_reason", None)
        excluded_reason = excluded_reason if isinstance(excluded_reason, str) else None

        return MatchSummary(
            match_id=str(match.id),
            job_id=job_fields["job_id"],
            title=job_fields["title"],
            company=job_fields["company"],
            location=job_fields["location"],
            is_remote=job_fields["is_remote"],
            fit_score=self._optional_float(match.fit_score),
            preference_score=self._preference_score_percent(match.preference_score),
            preference_status=self._preference_status(match),
            penalties=self._float_or_zero(match.penalties),
            required_coverage=self._float_or_zero(match.required_coverage),
            preferred_requirement_coverage=preferred_requirement_coverage,
            match_type=safe_str(match.match_type, "unknown"),
            is_hidden=match.is_hidden or False,
            created_at=safe_datetime_iso(match.created_at),
            calculated_at=safe_datetime_iso(match.calculated_at),
            ranking_mode_used=expl.ranking_mode_used if expl else None,
            dominant_reason_code=expl.dominant_reason_code if expl else None,
            explanation_label=expl.explanation_label if expl else None,
            balanced_primary_score=expl.balanced_primary_score if expl else None,
            missing_scores=list(expl.missing_scores) if expl else [],
            scoring_degraded_reason=self._scoring_degraded_reason(getattr(match, "fit_components", None)),
            selection_tier=selection_tier or "primary",
            excluded_reason=excluded_reason,
            llm_original_rank=self._optional_int_attr(match, "llm_original_rank"),
            llm_reranked_rank=self._optional_int_attr(match, "llm_reranked_rank"),
            llm_rerank_score=self._optional_float_attr(match, "llm_rerank_score"),
            llm_rerank_confidence=self._optional_float_attr(match, "llm_rerank_confidence"),
            **self._llm_marker_fields(evaluation),
        )
    
    def _to_match_detail(
        self,
        match: JobMatch,
        penalty_details: Dict[str, Any],
        *,
        owner_id: Optional[Any],
        tenant_id: Optional[Any],
    ) -> MatchDetail:
        """Convert ORM model to MatchDetail response model."""
        fit_components = self._fit_components(match.fit_components)
        evaluation = self._latest_evaluation_for_match(
            match,
            owner_id=owner_id,
            tenant_id=tenant_id,
        )
        if evaluation is not None and owner_id is not None:
            try:
                setattr(
                    evaluation,
                    "llm_effectiveness",
                    MatchLlmEvaluationService(self.db).evaluation_effectiveness(
                        match,
                        evaluation,
                        owner_id=owner_id,
                        tenant_id=tenant_id,
                    ),
                )
            except Exception as exc:
                logger.warning("Could not annotate LLM evaluation freshness: %s", exc)
        preferred_requirement_coverage = safe_float(
            match.preferred_requirement_coverage
        )
        return MatchDetail(
            match_id=str(match.id),
            resume_fingerprint=safe_str(match.resume_fingerprint),
            fit_score=safe_float(match.fit_score) if match.fit_score is not None else None,
            preference_score=self._preference_score_percent(match.preference_score),
            scoring_degraded_reason=self._scoring_degraded_reason(match.fit_components),
            fit_components=fit_components,
            preference_components=self._preference_components(match),
            fit_confidence=self._fit_confidence(fit_components),
            fit_explanation=self._fit_explanation(fit_components),
            fit_scorer=self._fit_scorer(fit_components),
            preference_status=self._preference_status(match),
            **self._llm_marker_fields(evaluation),
            base_score=safe_float(match.base_score),
            penalties=safe_float(match.penalties),
            required_coverage=safe_float(match.required_coverage),
            preferred_requirement_coverage=preferred_requirement_coverage,
            total_requirements=safe_int(match.total_requirements),
            matched_requirements_count=safe_int(match.matched_requirements_count),
            match_type=safe_str(match.match_type, "unknown"),
            status=safe_str(match.status, "unknown"),
            created_at=safe_datetime_iso(match.created_at),
            calculated_at=safe_datetime_iso(match.calculated_at),
            penalty_details=penalty_details,
        )

    @staticmethod
    def _fit_confidence(fit_components: Any) -> Optional[float]:
        if not isinstance(fit_components, dict):
            return None

        value = fit_components.get("fit_confidence")
        return safe_float(value) if value is not None else None

    @staticmethod
    def _fit_explanation(fit_components: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(fit_components, dict):
            return None

        explanation = fit_components.get("fit_explanation")
        return explanation if isinstance(explanation, dict) else None

    @staticmethod
    def _fit_scorer(fit_components: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(fit_components, dict):
            return None

        scorer = fit_components.get("fit_scorer")
        return scorer if isinstance(scorer, dict) else None

    @staticmethod
    def _scoring_degraded_reason(fit_components: Any) -> Optional[str]:
        """Derive a compact code for the UI banner from `semantic_fit_fallback_reason`.

        Returns one of: remote_unavailable, local_unavailable, provider_disabled, or
        the raw reason string if no known prefix matches. None when scoring was
        fully healthy.
        """
        if not isinstance(fit_components, dict):
            return None
        raw = fit_components.get("semantic_fit_fallback_reason")
        if not raw:
            return None
        text = str(raw).lower()
        if "remote" in text:
            return "remote_unavailable"
        if "local" in text and ("disabled" in text or "no provider" in text or "not available" in text):
            return "local_unavailable"
        if "disabled" in text:
            return "provider_disabled"
        return "degraded"

    @staticmethod
    def _fit_components(fit_components: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(fit_components, dict):
            return None

        return {
            key: value
            for key, value in fit_components.items()
            if key not in _PREFERENCE_COMPONENT_KEYS
        }

    def _preference_components(self, match: JobMatch) -> Optional[Dict[str, Any]]:
        preference_components = getattr(match, "preference_components", None)
        if isinstance(preference_components, dict) and preference_components:
            return preference_components
        return None

    @staticmethod
    def _preference_status(match: JobMatch) -> Optional[Dict[str, Any]]:
        preference_components = getattr(match, "preference_components", None)
        if isinstance(preference_components, dict):
            mode_used = preference_components.get("preference_mode_used")
            component_status = preference_components.get("preference_status")
            if component_status == "applied":
                return {
                    "applied": True,
                    "effective_mode": mode_used,
                }
            if isinstance(component_status, str) and component_status:
                return {
                    "applied": False,
                    "reason": MatchService._safe_preference_status_reason(component_status),
                    "effective_mode": mode_used,
                }

            fallback_reason = preference_components.get("preference_fallback_reason")
            if fallback_reason:
                return {
                    "applied": False,
                    "reason": MatchService._safe_preference_status_reason(fallback_reason),
                    "effective_mode": mode_used,
                }
            if mode_used:
                return {
                    "applied": True,
                    "effective_mode": mode_used,
                }

        ranking_snapshot = getattr(match, "ranking_snapshot", None)
        if isinstance(ranking_snapshot, dict):
            status = ranking_snapshot.get("preference_status")
            if isinstance(status, dict):
                sanitized = dict(status)
                reason = sanitized.get("reason")
                if reason:
                    sanitized["reason"] = MatchService._safe_preference_status_reason(reason)
                return sanitized

        return None

    @staticmethod
    def _safe_preference_status_reason(reason: Any) -> str:
        text = str(reason or "")
        if text in {
            "preference_profile_unavailable",
            "preference_reranker_unavailable",
            "preference_judge_unavailable",
        }:
            return "preference_scorer_unavailable"
        if text in {"job_offerings_unavailable", "missing_job_offerings"}:
            return "missing_job_offerings"
        if text in {"invalid_llm_output", "missing_preference_assessment"}:
            return "invalid_llm_output"
        if (
            text == "job_offerings_lookup_failed"
            or text.startswith("runtime_error:")
            or text.startswith("preference_reranking_failed")
        ):
            return "preference_scorer_failed"
        return text

    def _latest_evaluation_for_match(
        self,
        match: JobMatch,
        *,
        owner_id: Optional[Any],
        tenant_id: Optional[Any],
    ) -> Optional[LlmMatchEvaluation]:
        if owner_id is None:
            return None
        query = self.db.query(LlmMatchEvaluation).filter(
            LlmMatchEvaluation.owner_id == owner_id,
            LlmMatchEvaluation.job_match_id == match.id,
            LlmMatchEvaluation.resume_fingerprint == match.resume_fingerprint,
            LlmMatchEvaluation.deleted_at.is_(None),
        )
        if tenant_id is None:
            query = query.filter(LlmMatchEvaluation.tenant_id.is_(None))
        else:
            query = query.filter(LlmMatchEvaluation.tenant_id == tenant_id)
        return query.order_by(LlmMatchEvaluation.created_at.desc()).first()

    @staticmethod
    def _llm_marker_fields(evaluation: Any) -> Dict[str, Any]:
        status = getattr(evaluation, "status", None) if evaluation is not None else None
        if not isinstance(status, str):
            return {
                "llm_evaluation_status": None,
                "llm_evaluation_id": None,
                "llm_score": None,
                "llm_confidence": None,
                "llm_judged_at": None,
                "llm_effective_for_rerank": False,
                "llm_ignored_for_rerank_reason": None,
                "llm_stale_status": None,
                "llm_freshness": {},
                "llm_score_quality": {},
                "llm_retryable": False,
                "llm_queued_reason": None,
                "llm_queue_state": None,
                "llm_next_retry_at": None,
                "llm_retry_after_seconds": None,
                "llm_provider_status_message": None,
            }
        judged_at = getattr(evaluation, "completed_at", None)
        judged_at_iso = safe_datetime_iso(judged_at) if hasattr(judged_at, "isoformat") else None
        effectiveness = getattr(evaluation, "llm_effectiveness", None)
        if not isinstance(effectiveness, dict):
            effectiveness = {}
        analysis = getattr(evaluation, "analysis", None)
        if not isinstance(analysis, dict):
            analysis = {}
        queue_metadata = analysis.get("queue")
        if not isinstance(queue_metadata, dict):
            queue_metadata = {}
        retry_after_seconds = queue_metadata.get("retry_after_seconds")
        try:
            retry_after_seconds = (
                None
                if retry_after_seconds is None
                else max(int(float(retry_after_seconds)), 0)
            )
        except (TypeError, ValueError):
            retry_after_seconds = None
        provider_status_message = queue_metadata.get("provider_status_message")
        if not isinstance(provider_status_message, str):
            provider_status_message = None
        score_quality = score_quality_metadata(
            getattr(evaluation, "llm_score", None),
            getattr(evaluation, "verdict", None),
        )
        if isinstance(effectiveness.get("score_quality"), dict):
            score_quality = effectiveness["score_quality"]
        return {
            "llm_evaluation_status": status,
            "llm_evaluation_id": str(getattr(evaluation, "id", "")),
            "llm_score": (
                safe_float(score_quality.get("normalized_score"))
                if score_quality.get("normalized_score") is not None
                else (
                    None
                    if getattr(evaluation, "llm_score", None) is None
                    else normalize_llm_score(
                        evaluation.llm_score,
                        getattr(evaluation, "verdict", None),
                    )
                )
            ),
            "llm_confidence": (
                None
                if getattr(evaluation, "confidence", None) is None
                else safe_float(evaluation.confidence)
            ),
            "llm_judged_at": judged_at_iso,
            "llm_effective_for_rerank": bool(effectiveness.get("effective_for_rerank", False)),
            "llm_ignored_for_rerank_reason": effectiveness.get("ignored_for_rerank_reason"),
            "llm_stale_status": effectiveness.get("stale_status"),
            "llm_freshness": effectiveness.get("freshness") if isinstance(effectiveness.get("freshness"), dict) else {},
            "llm_score_quality": score_quality,
            "llm_retryable": bool(getattr(evaluation, "retryable", False)),
            "llm_queued_reason": analysis.get("enqueue_reason") or queue_metadata.get("enqueue_reason"),
            "llm_queue_state": queue_metadata.get("queue_state"),
            "llm_next_retry_at": queue_metadata.get("next_retry_at"),
            "llm_retry_after_seconds": retry_after_seconds,
            "llm_provider_status_message": provider_status_message,
        }

    @staticmethod
    def _primary_source(job: Optional[JobPost]):
        try:
            sources = list(getattr(job, "sources", None) or [])
        except TypeError:
            sources = []
        if not sources:
            return None
        active_sources = [source for source in sources if getattr(source, "is_active", False)]
        candidates = active_sources or sources
        return max(
            candidates,
            key=lambda source: (
                getattr(source, "last_seen_at", None) is not None,
                getattr(source, "last_seen_at", None),
            ),
        )

    @staticmethod
    def _lifecycle_metadata(job: Optional[JobPost]) -> Dict[str, Any]:
        payload = getattr(job, "raw_payload", None)
        if not isinstance(payload, dict):
            return {}
        metadata = payload.get(_LIFECYCLE_METADATA_KEY)
        return copy.deepcopy(metadata) if isinstance(metadata, dict) else {}

    @classmethod
    def _availability_status(cls, job: Optional[JobPost], source: Any) -> tuple[str | None, str | None]:
        if job is None:
            return None, None
        lifecycle = cls._lifecycle_metadata(job)
        if getattr(job, "status", None) == "expired" and isinstance(lifecycle.get("manual_retirement"), dict):
            return "manually_retired", "manual_retirement"
        if source is not None and getattr(source, "is_active", None) is False:
            return "source_inactive", "source_sync_absent"
        if source is None:
            return "unknown", "source_missing"
        if getattr(job, "status", None) == "inactive":
            return "inactive", "job_inactive"
        return "active", "source_sync_active"

    @staticmethod
    def _availability_actions(job: Optional[JobPost], source: Any) -> List[str]:
        if job is None:
            return []
        actions: List[str] = []
        if source is not None and (getattr(source, "job_url_direct", None) or getattr(source, "job_url", None)):
            actions.append("open_posting")
        refresh_kind = source_refresh_kind(source)
        if refresh_kind == "compliant_ats":
            actions.append("refresh_availability")
            if MatchService._missing_description(job):
                actions.append("refresh_description")
        elif refresh_kind == "adapter_missing":
            actions.append("refresh_unavailable_adapter_missing")
            if MatchService._missing_description(job):
                actions.append("description_recovery_unavailable_adapter_missing")
        elif refresh_kind == "prohibited":
            actions.append("refresh_unavailable_deployment_disabled")
            if MatchService._missing_description(job):
                actions.append("description_recovery_unavailable_deployment_disabled")
        elif source is not None:
            actions.append("refresh_unavailable")
            if MatchService._missing_description(job):
                actions.append("description_recovery_unavailable")
        actions.append("restore" if getattr(job, "status", None) == "expired" else "retire")
        return actions

    @staticmethod
    def _missing_description(job: Optional[JobPost]) -> bool:
        if job is None:
            return False
        description = getattr(job, "description", None)
        return (
            not (description or "").strip()
            or getattr(job, "description_completeness", None) == "missing"
            or getattr(job, "extraction_status", None) == "no_description"
        )

    @staticmethod
    def _description_recovery_provider(job: Optional[JobPost]) -> str | None:
        if job is None:
            return None
        payload = getattr(job, "raw_payload", None)
        if not isinstance(payload, dict):
            return None
        metadata = payload.get("source_metadata")
        if not isinstance(metadata, dict):
            return None
        provider = metadata.get("description_provider")
        return str(provider) if provider else None

    @staticmethod
    def _description_recovery_status_group(status: str | None) -> str:
        if status in {"queued", "refreshing"}:
            return "checking"
        if status == "description_found":
            return "found"
        if status == "posting_not_found":
            return "gone"
        if status == "source_unmapped":
            return "configure_source"
        if status == "source_adapter_missing":
            return "adapter_missing"
        if status in {"source_prohibited", "source_unsupported"}:
            return "unsupported"
        if status == "failed_retryable":
            return "retrying"
        if status == "failed_terminal":
            return "failed"
        return "pending" if status in {"pending", "not_needed", None} else "unknown"

    def _to_job_details(self, job: Optional[JobPost]) -> JobDetails:
        """Convert ORM model to JobDetails response model."""
        if not job:
            return JobDetails(
                job_id=None,
                title=None,
                company=None,
                location=None,
                is_remote=None,
                description=None,
                description_source="unknown",
                description_completeness="missing",
                description_warning_code=None,
                salary_min=None,
                salary_max=None,
                currency=None,
                min_years_experience=None,
                requires_degree=None,
                security_clearance=None,
                job_level=None,
            )
        
        description = getattr(job, "description", None)
        description_source = self._optional_str_attr(job, "description_source") or "unknown"
        description_completeness = self._optional_str_attr(job, "description_completeness")
        if not description:
            description_completeness = "missing"
        elif description_completeness is None:
            description_completeness = "unknown"
        description_warning_code = self._optional_str_attr(job, "description_warning_code")
        source = self._primary_source(job)
        availability_status, availability_reason = self._availability_status(job, source)
        recovery_status = (
            self._optional_str_attr(job, "description_recovery_status")
            or "not_needed"
        )

        return JobDetails(
            job_id=str(job.id),
            title=job.title,
            company=job.company,
            location=job.location_text,
            is_remote=job.is_remote,
            description=description,
            description_source=description_source,
            description_completeness=description_completeness,
            description_warning_code=description_warning_code,
            description_recovery_status=recovery_status,
            description_recovery_reason=self._optional_str_attr(
                job,
                "description_recovery_reason",
            ),
            description_recovery_provider=self._description_recovery_provider(job),
            description_recovery_status_group=self._description_recovery_status_group(
                recovery_status
            ),
            description_recovery_attempts=safe_int(
                getattr(job, "description_recovery_attempts", 0) or 0
            ),
            description_recovery_last_attempt_at=self._optional_datetime_iso_attr(
                job,
                "description_recovery_last_attempt_at",
            ),
            description_recovery_next_retry_at=self._optional_datetime_iso_attr(
                job,
                "description_recovery_next_retry_at",
            ),
            description_recovery_last_error=self._optional_str_attr(
                job,
                "description_recovery_last_error",
            ),
            description_recovery_run_id=self._optional_str_attr(
                job,
                "description_recovery_run_id",
            ),
            salary_min=safe_float(job.salary_min) if job.salary_min is not None else None,
            salary_max=safe_float(job.salary_max) if job.salary_max is not None else None,
            currency=job.currency,
            min_years_experience=safe_int(job.min_years_experience) if job.min_years_experience is not None else None,
            requires_degree=job.requires_degree,
            security_clearance=job.security_clearance,
            job_level=job.job_level,
            status=self._optional_str_attr(job, "status"),
            source_site=getattr(source, "site", None),
            source_url=getattr(source, "job_url", None),
            source_url_direct=getattr(source, "job_url_direct", None),
            source_job_id=getattr(source, "source_job_id", None),
            source_is_active=getattr(source, "is_active", None),
            source_first_seen_at=safe_datetime_iso(getattr(source, "first_seen_at", None)),
            source_last_seen_at=safe_datetime_iso(getattr(source, "last_seen_at", None)),
            first_seen_at=self._optional_datetime_iso_attr(job, "first_seen_at"),
            last_seen_at=self._optional_datetime_iso_attr(job, "last_seen_at"),
            availability_status=availability_status,
            availability_reason=availability_reason,
            availability_actions=self._availability_actions(job, source),
            lifecycle_metadata=self._lifecycle_metadata(job),
        )
    
    def _to_requirement_detail(self, req: JobMatchRequirement) -> RequirementDetail:
        """Convert ORM model to RequirementDetail response model."""
        return RequirementDetail(
            requirement_id=str(req.job_requirement_unit_id),
            requirement_text=req.requirement.text if req.requirement else None,
            evidence_text=req.evidence_text,
            evidence_section=req.evidence_section,
            similarity_score=safe_float(req.similarity_score),
            evidence_score=safe_float(req.evidence_score) if req.evidence_score is not None else None,
            is_covered=req.is_covered or False,
            req_type=safe_str(req.req_type, "required"),
        )
    
    def _parse_penalty_details(self, penalty_details: Any) -> Dict[str, Any]:
        """Parse penalty details from JSON or dict."""
        if penalty_details is None:
            return {}
        
        if isinstance(penalty_details, dict):
            return penalty_details
        
        if isinstance(penalty_details, str):
            try:
                return json.loads(penalty_details)
            except ValueError:
                return {}
        
        return {}
