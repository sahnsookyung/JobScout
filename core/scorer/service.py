from typing import List, Optional, Dict, Any
import logging
import threading
import numpy as np
from sqlalchemy import select

from database.repository import JobRepository
from database.models import StructuredResume  # keep ResumeSectionEmbedding only if you truly need it
from core.config_loader import ScorerConfig, ResultPolicy
from core.matcher import JobMatchPreliminary
from core.scorer.models import ScoredJobMatch
from core.scorer import penalties as penalty_calculations
from core.scorer import fit_score, want_score

logger = logging.getLogger(__name__)


def _prefetch_total_years(preliminary_matches: List[JobMatchPreliminary], db) -> Dict[str, Optional[float]]:
    fps = {pm.resume_fingerprint for pm in preliminary_matches if pm.resume_fingerprint}
    if not fps:
        return {}

    stmt = select(StructuredResume.resume_fingerprint, StructuredResume.total_experience_years).where(
        StructuredResume.resume_fingerprint.in_(fps)
    )
    rows = db.execute(stmt).fetchall()
    return {fp: (float(years) if years is not None else None) for fp, years in rows}


def _blend_overall(config: ScorerConfig, fit: float, want: Optional[float]) -> float:
    if want is None:
        return min(100.0, fit)
    return min(100.0, config.fit_weight * fit + config.want_weight * want)


class ScoringService:
    def __init__(self, repo: JobRepository, config: ScorerConfig):
        self.repo = repo
        self.config = config

    def score_matches(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        result_policy: Optional[ResultPolicy] = None,
        user_want_embeddings: Optional[List[np.ndarray]] = None,
        job_facet_embeddings_map: Optional[Dict[str, Dict[str, np.ndarray]]] = None,
        match_type: str = "requirements_only",
        stop_event: Optional[threading.Event] = None,
    ) -> List[ScoredJobMatch]:

        years_by_fp = _prefetch_total_years(preliminary_matches, self.repo.db)

        scored = []
        for pm in preliminary_matches:
            if stop_event and stop_event.is_set():
                logger.info("ScoringService interrupted")
                return []
            
            scored.append(self.score_preliminary_match(
                preliminary=pm,
                match_type=match_type,
                candidate_total_years=years_by_fp.get(pm.resume_fingerprint),
                user_want_embeddings=user_want_embeddings,
                job_facet_embeddings=(job_facet_embeddings_map or {}).get(str(pm.job.id)),
            ))

        scored.sort(key=lambda x: x.overall_score, reverse=True)

        if result_policy:
            if result_policy.min_fit > 0:
                scored = [r for r in scored if r.fit_score >= result_policy.min_fit]
            if result_policy.min_jd_required_coverage is not None:
                scored = [r for r in scored if r.jd_required_coverage >= result_policy.min_jd_required_coverage]
            scored = scored[: result_policy.top_k]

        return scored

    def score_preliminary_match(
        self,
        preliminary: JobMatchPreliminary,
        match_type: str = "requirements_only",
        candidate_total_years: Optional[float] = None,
        user_want_embeddings: Optional[List[np.ndarray]] = None,
        job_facet_embeddings: Optional[Dict[str, np.ndarray]] = None,
    ) -> ScoredJobMatch:

        job = preliminary.job

        fit_penalties, penalty_details = penalty_calculations.calculate_fit_penalties(
            job=job,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            config=self.config,
            candidate_total_years=candidate_total_years,
            experience_sections=None,  # make this opt-in if needed
        )

        fit_value, fit_components = fit_score.calculate_fit_score(
            job_similarity=preliminary.job_similarity,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            fit_penalties=fit_penalties,
            config=self.config,
        )

        want_value = None
        want_components = {}
        if user_want_embeddings and job_facet_embeddings:
            want_value, want_components = want_score.calculate_want_score(
                user_want_embeddings=user_want_embeddings,
                job_facet_embeddings=job_facet_embeddings,
                facet_weights=self.config.facet_weights,
            )

        overall = _blend_overall(self.config, fit_value, want_value)

        return ScoredJobMatch(
            job=job,
            fit_score=fit_value,
            want_score=want_value or 0.0,
            overall_score=overall,
            fit_components=fit_components,
            want_components=want_components or {},
            base_score=fit_components.get("blended", 0.0) * 100.0,
            preferences_boost=0.0,
            penalties=fit_penalties,
            jd_required_coverage=fit_components["required_coverage"],
            jd_preferences_coverage=fit_components["preferred_coverage"],
            job_similarity=preliminary.job_similarity,
            penalty_details=penalty_details,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type,
            policy_applied=None,
        )
