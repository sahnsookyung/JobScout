from typing import List, Optional, Dict
import logging
import threading
from sqlalchemy import select

from database.repository import JobRepository
from database.models import StructuredResume  # keep ResumeSectionEmbedding only if you truly need it
from core.config_loader import ScorerConfig, ResultPolicy
from core.matcher import JobMatchPreliminary
from core.scorer.models import ScoredJobMatch
from core.scorer import penalties as penalty_calculations
from core.scorer.semantic_fit import SemanticFitScorer, ThresholdSemanticFitScorer

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


def _blend_overall(fit: float) -> float:
    return min(100.0, fit)


class ScoringService:
    def __init__(
        self,
        repo: JobRepository,
        config: ScorerConfig,
        semantic_fit_scorer: Optional[SemanticFitScorer] = None,
    ):
        self.repo = repo
        self.config = config
        self.semantic_fit_scorer = semantic_fit_scorer or ThresholdSemanticFitScorer()

    def score_matches(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        result_policy: Optional[ResultPolicy] = None,
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
    ) -> ScoredJobMatch:
        del candidate_total_years  # Deprecated no-op retained for backwards compatibility.

        job = preliminary.job

        fit_penalties, penalty_details = penalty_calculations.calculate_fit_penalties(
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            config=self.config,
            experience_sections=None,  # make this opt-in if needed
        )

        semantic_fit = self.semantic_fit_scorer.score(
            preliminary=preliminary,
            fit_penalties=fit_penalties,
            config=self.config,
        )

        overall = _blend_overall(semantic_fit.fit_score)

        return ScoredJobMatch(
            job=job,
            fit_score=semantic_fit.fit_score,
            overall_score=overall,
            fit_components=semantic_fit.fit_components,
            fit_confidence=semantic_fit.fit_confidence,
            fit_explanation=semantic_fit.fit_explanation,
            fit_scorer={
                "name": semantic_fit.scorer_name,
                "version": semantic_fit.scorer_version,
            },
            base_score=semantic_fit.fit_components.get("core", 0.0) * 100.0,
            penalties=fit_penalties,
            jd_required_coverage=semantic_fit.fit_components["required_coverage"],
            jd_preferences_coverage=semantic_fit.fit_components["preferred_coverage"],
            job_similarity=preliminary.job_similarity,
            penalty_details=penalty_details,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type,
            policy_applied=None,
        )
