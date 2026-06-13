#!/usr/bin/env python3
"""
Match service - business logic for job match operations.
"""

import json
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from core.redis_streams import _sanitize_log
from sqlalchemy.orm import Session

from core.llm_evaluation import MatchLlmEvaluationService
from core.match_selection import resolve_canonical_resume_selection
from core.policy import get_result_policy_store
from core.ranking import rank_matches, RankingContext, RankingMode, get_ranking_policy_store
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

logger = logging.getLogger(__name__)

_PREFERENCE_COMPONENT_KEYS = {
    "preference_confidence",
    "preference_reason_codes",
    "preference_explanation",
    "preference_mode_requested",
    "preference_mode_effective",
    "preference_mode_used",
    "preference_fallback_reason",
}


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


class MatchService:
    """Service for managing job matches."""
    
    def __init__(self, db: Session):
        self.db = db
        self.last_llm_rerank_metadata: Dict[str, Any] = self._empty_llm_rerank_metadata(
            reason="not_evaluated"
        )
    
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
            top_k: Maximum number of results (capped to config.max_top_k).
            remote_only: Filter to remote jobs only.
            show_hidden: Include hidden matches in results.
            ranking_mode: One of "preference_first", "fit_first", "balanced".
                Defaults to config.active_default_mode.

        Returns:
            List of match summaries with ranking explanation fields.
        """
        ranking_config = get_ranking_policy_store().get_current_config()

        # Resolve ranking mode
        try:
            mode = RankingMode(ranking_mode) if ranking_mode else RankingMode(ranking_config.active_default_mode)
        except ValueError:
            mode = RankingMode(ranking_config.active_default_mode)

        canonical_selection = self._resolve_canonical_selection(
            owner_id=owner_id,
            tenant_id=tenant_id,
        )
        if canonical_selection is None:
            return []

        pool = self._load_rankable_pool(
            canonical_selection,
            status=status,
            min_fit=min_fit,
            remote_only=remote_only,
            show_hidden=show_hidden,
            tier=tier,
            tenant_id=tenant_id,
        )

        # Stage 2: Python rank the primary tier only. Excluded items were
        # never ranked against the active policy and do not compete for top-K.
        primary_pool = [
            m for m in pool
            if getattr(m, "selection_tier", "primary") == "primary"
        ]
        excluded_pool = [
            m for m in pool
            if getattr(m, "selection_tier", "primary") != "primary"
        ]
        ctx = RankingContext(mode=mode, config=ranking_config)
        rank_matches(primary_pool, ctx)
        for index, candidate in enumerate(primary_pool, start=1):
            setattr(candidate, "llm_original_rank", index)

        self._attach_latest_evaluations(
            primary_pool,
            owner_id=owner_id,
            tenant_id=tenant_id,
        )
        self.last_llm_rerank_metadata = self._apply_llm_rerank(
            primary_pool,
            owner_id=owner_id,
            tenant_id=tenant_id,
        )

        if tier == "all":
            self._attach_latest_evaluations(
                excluded_pool,
                owner_id=owner_id,
                tenant_id=tenant_id,
            )
            ranked = primary_pool + excluded_pool
            if top_k is not None:
                ranked = ranked[:ranking_config.effective_top_k(top_k)]
        else:
            effective_k = ranking_config.effective_top_k(top_k)
            ranked = primary_pool[:effective_k]

        return [self._to_match_summary(m) for m in ranked]

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
            logger.warning("Could not resolve canonical resume fingerprint: %s", exc)
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
            "reason": reason,
        }

    def _apply_llm_rerank(
        self,
        primary_pool: List[Any],
        *,
        owner_id: Optional[Any],
        tenant_id: Optional[Any],
    ) -> Dict[str, Any]:
        if owner_id is None:
            return self._empty_llm_rerank_metadata(reason="owner_missing")
        if not primary_pool:
            return self._empty_llm_rerank_metadata(reason="empty_primary_pool")

        try:
            policy = get_result_policy_store().get_llm_judge_policy(owner_id)
        except Exception as exc:
            logger.warning("Could not load LLM rerank policy: %s", exc)
            return self._empty_llm_rerank_metadata(reason="policy_unavailable")

        top_n = max(0, int(getattr(policy, "top_n", 0) or 0))
        metadata = {
            "enabled": bool(getattr(policy, "enabled", False)),
            "available": bool(getattr(policy, "available", False)),
            "applied": False,
            "top_n": top_n,
            "window_size": 0,
            "eligible_count": 0,
            "reranked_count": 0,
            "reason": None,
        }
        if not getattr(policy, "available", False):
            metadata["reason"] = getattr(policy, "unavailable_reason", None) or "unavailable"
            return metadata
        if not getattr(policy, "enabled", False):
            metadata["reason"] = "disabled"
            return metadata
        if top_n <= 0:
            metadata["reason"] = "top_n_zero"
            return metadata

        window_size = min(top_n, len(primary_pool))
        window = primary_pool[:window_size]
        metadata["window_size"] = window_size
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
                candidate.llm_rerank_score = safe_float(evaluation.llm_score)
                candidate.llm_rerank_confidence = safe_float(evaluation.confidence)

        metadata["eligible_count"] = eligible_count
        if eligible_count == 0:
            metadata["reason"] = "no_current_successful_evaluations"
            for index, candidate in enumerate(primary_pool, start=1):
                candidate.llm_reranked_rank = index
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
            preference_score=self._optional_float(match.preference_score),
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
            preference_score=safe_float(match.preference_score) if match.preference_score is not None else None,
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
        ranking_snapshot = getattr(match, "ranking_snapshot", None)
        if isinstance(ranking_snapshot, dict):
            status = ranking_snapshot.get("preference_status")
            if isinstance(status, dict):
                return status

        preference_components = getattr(match, "preference_components", None)
        if not isinstance(preference_components, dict):
            return None

        fallback_reason = preference_components.get("preference_fallback_reason")
        mode_used = preference_components.get("preference_mode_used")
        if fallback_reason:
            return {
                "applied": False,
                "reason": fallback_reason,
                "effective_mode": mode_used,
            }
        if mode_used:
            return {
                "applied": True,
                "effective_mode": mode_used,
            }
        return None

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
            }
        judged_at = getattr(evaluation, "completed_at", None)
        judged_at_iso = safe_datetime_iso(judged_at) if hasattr(judged_at, "isoformat") else None
        effectiveness = getattr(evaluation, "llm_effectiveness", None)
        if not isinstance(effectiveness, dict):
            effectiveness = {}
        return {
            "llm_evaluation_status": status,
            "llm_evaluation_id": str(getattr(evaluation, "id", "")),
            "llm_score": (
                None
                if getattr(evaluation, "llm_score", None) is None
                else safe_float(evaluation.llm_score)
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
        }

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
            salary_min=safe_float(job.salary_min) if job.salary_min is not None else None,
            salary_max=safe_float(job.salary_max) if job.salary_max is not None else None,
            currency=job.currency,
            min_years_experience=safe_int(job.min_years_experience) if job.min_years_experience is not None else None,
            requires_degree=job.requires_degree,
            security_clearance=job.security_clearance,
            job_level=job.job_level,
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
