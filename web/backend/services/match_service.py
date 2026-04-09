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

from core.match_selection import resolve_canonical_resume_selection
from core.ranking import rank_matches, RankingContext, RankingMode, get_ranking_policy_store
from database.models import JobMatch, JobPost, JobMatchRequirement, StructuredResume
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
from ..exceptions import MatchNotFoundException

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


class MatchService:
    """Service for managing job matches."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_matches(
        self,
        owner_id: Optional[Any] = None,
        status: str = "active",
        min_fit: Optional[float] = None,
        top_k: Optional[int] = None,
        remote_only: bool = False,
        show_hidden: bool = False,
        ranking_mode: Optional[str] = None,
    ) -> List[MatchSummary]:
        """
        Get filtered job matches, ranked by the requested mode.

        Stage 1 — DB retrieve: scoped to the canonical resume's full persisted
            match set after request filters are applied.
        Stage 2 — Python rank: rank_matches() applies the declared mode
            with NULL-aware sort keys; attaches RankingExplanation per item.
        Stage 3 — Truncate: [:effective_top_k] applied after ranking so
            the global ordering is always correct before pagination.

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

        canonical_selection = self._resolve_canonical_selection(owner_id=owner_id)
        if canonical_selection is None:
            return []

        pool = self._load_rankable_pool(
            canonical_selection,
            status=status,
            min_fit=min_fit,
            remote_only=remote_only,
            show_hidden=show_hidden,
        )

        # Stage 2: Python rank
        ctx = RankingContext(mode=mode, config=ranking_config)
        rank_matches(pool, ctx)

        # Stage 3: Truncate
        effective_k = ranking_config.effective_top_k(top_k)
        ranked = pool[:effective_k]

        return [self._to_match_summary(m) for m in ranked]

    def _resolve_canonical_selection(
        self,
        owner_id: Optional[Any] = None,
    ):
        try:
            with job_uow() as repo:
                return resolve_canonical_resume_selection(repo, owner_id)
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
    ) -> List[Any]:
        with job_uow() as repo:
            items = repo.match_selection.get_items_for_run(
                canonical_selection.selection_run_id
            )
            pool: List[Any] = []
            for item in items:
                match = item.job_match
                job = getattr(match, "job_post", None)
                fit_score = (
                    None
                    if item.fit_score_at_selection is None
                    else float(item.fit_score_at_selection)
                )
                if status != "all" and match.status != status:
                    continue
                if min_fit is not None and (fit_score is None or fit_score < min_fit):
                    continue
                if not show_hidden and bool(match.is_hidden):
                    continue
                if remote_only and not bool(getattr(job, "is_remote", False)):
                    continue
                pool.append(
                    MatchSummaryCandidate(
                        id=str(match.id),
                        job_id=str(match.job_post_id),
                        title=job.title if job and hasattr(job, "title") else "Unknown",
                        company=job.company if job and hasattr(job, "company") else "Unknown",
                        location=(
                            job.location_text
                            if job and hasattr(job, "location_text")
                            else None
                        ),
                        is_remote=bool(getattr(job, "is_remote", False)),
                        fit_score=fit_score,
                        preference_score=(
                            None
                            if item.preference_score_at_selection is None
                            else float(item.preference_score_at_selection)
                        ),
                        job_similarity=float(item.job_similarity_at_selection),
                        penalties=(
                            None if match.penalties is None else safe_float(match.penalties)
                        ),
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
                    )
                )
            return pool
    
    def _get_match_for_owner(
        self,
        match_id: str,
        owner_id: Optional[Any] = None,
    ) -> JobMatch:
        query = self.db.query(JobMatch)
        if owner_id is not None:
            query = query.join(
                StructuredResume,
                StructuredResume.resume_fingerprint == JobMatch.resume_fingerprint,
            ).filter(StructuredResume.owner_id == owner_id)

        match = query.filter(JobMatch.id == match_id).one_or_none()
        if not match:
            raise MatchNotFoundException(f"Match {match_id} not found")
        return match

    def get_match_detail(
        self,
        match_id: str,
        owner_id: Optional[Any] = None,
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
        match = self._get_match_for_owner(match_id, owner_id=owner_id)

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
                match=self._to_match_detail(match, penalty_details),
                job=self._to_job_details(job),
                requirements=requirements
            )
        except Exception as e:
            logger.error("Database error fetching match details for %s: %s", _sanitize_log(match_id), e, exc_info=True)
            raise
    
    def toggle_hidden(self, match_id: str, owner_id: Optional[Any] = None) -> bool:
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

        is_currently_hidden = match.is_hidden or False
        new_status = not is_currently_hidden
        match.is_hidden = new_status
        self.db.commit()
        
        return new_status
    
    def get_match_explanation(
        self,
        match_id: str,
        owner_id: Optional[Any] = None,
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
        match = self._get_match_for_owner(match_id, owner_id=owner_id)

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

    def _to_match_summary(self, match: JobMatch) -> MatchSummary:
        """Convert ORM model to MatchSummary response model."""
        job_fields = self._extract_summary_job_fields(match)

        expl = getattr(match, "ranking_explanation", None)
        preferred_requirement_coverage = self._float_or_zero(
            match.preferred_requirement_coverage
        )

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
        )
    
    def _to_match_detail(self, match: JobMatch, penalty_details: Dict[str, Any]) -> MatchDetail:
        """Convert ORM model to MatchDetail response model."""
        fit_components = self._fit_components(match.fit_components)
        preferred_requirement_coverage = safe_float(
            match.preferred_requirement_coverage
        )
        return MatchDetail(
            match_id=str(match.id),
            resume_fingerprint=safe_str(match.resume_fingerprint),
            fit_score=safe_float(match.fit_score) if match.fit_score is not None else None,
            preference_score=safe_float(match.preference_score) if match.preference_score is not None else None,
            fit_components=fit_components,
            preference_components=self._preference_components(match),
            fit_confidence=self._fit_confidence(fit_components),
            fit_explanation=self._fit_explanation(fit_components),
            fit_scorer=self._fit_scorer(fit_components),
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
                salary_min=None,
                salary_max=None,
                currency=None,
                min_years_experience=None,
                requires_degree=None,
                security_clearance=None,
                job_level=None,
            )
        
        return JobDetails(
            job_id=str(job.id),
            title=job.title,
            company=job.company,
            location=job.location_text,
            is_remote=job.is_remote,
            description=job.description,
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
