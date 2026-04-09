from typing import List, Optional, Dict, Any
import logging
import threading
from sqlalchemy import select

from database.repository import JobRepository
from database.models import StructuredResume  # keep ResumeSectionEmbedding only if you truly need it
from core.config_loader import ScorerConfig, ResultPolicy
from core.llm.interfaces import LLMProvider
from core.llm.provider_factory import build_llm_provider, runtime_llm_config_from_fit
from core.matcher import JobMatchPreliminary
from core.scorer.coverage import calculate_requirement_coverage
from core.scorer.models import ScoredJobMatch
from core.scorer import penalties as penalty_calculations
from core.scorer.semantic_fit import (
    CrossEncoderSemanticFitScorer,
    LLMSemanticFitScorer,
    LocalCrossEncoderProvider,
    RemoteCrossEncoderProvider,
    SemanticFitScorer,
    ThresholdSemanticFitScorer,
    resolve_effective_fit_mode,
)

logger = logging.getLogger(__name__)


def _prefetch_resume_metadata(preliminary_matches: List[JobMatchPreliminary], db) -> Dict[str, Dict[str, Any]]:
    fps = {pm.resume_fingerprint for pm in preliminary_matches if pm.resume_fingerprint}
    if not fps:
        return {}

    stmt = select(
        StructuredResume.resume_fingerprint,
        StructuredResume.total_experience_years,
        StructuredResume.owner_id,
    ).where(
        StructuredResume.resume_fingerprint.in_(fps)
    )
    rows = db.execute(stmt).fetchall()
    metadata: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if len(row) == 3:
            fp, years, owner_id = row
        else:
            fp, years = row
            owner_id = None
        metadata[fp] = {
            "total_years": float(years) if years is not None else None,
            "owner_id": owner_id,
        }
    return metadata


def _prefetch_total_years(preliminary_matches: List[JobMatchPreliminary], db) -> Dict[str, Optional[float]]:
    metadata_by_fp = _prefetch_resume_metadata(preliminary_matches, db)
    return {
        fingerprint: values.get("total_years")
        for fingerprint, values in metadata_by_fp.items()
    }


class ScoringService:
    def __init__(
        self,
        repo: JobRepository,
        config: ScorerConfig,
        ai_service: Optional[LLMProvider] = None,
        semantic_fit_scorer: Optional[SemanticFitScorer] = None,
    ):
        self.repo = repo
        self.config = config
        self.semantic_fit_scorer = semantic_fit_scorer or self._build_semantic_fit_scorer(ai_service)

    def _build_semantic_fit_scorer(
        self,
        ai_service: Optional[LLMProvider],
    ) -> SemanticFitScorer:
        threshold_scorer = ThresholdSemanticFitScorer()
        semantic_fit_config = getattr(self.config, "semantic_fit", None)
        local_provider = None
        if semantic_fit_config.cross_encoder.local.enabled:
            local_provider = LocalCrossEncoderProvider(
                model_name=semantic_fit_config.cross_encoder.local.model_name,
                cache_path=semantic_fit_config.cross_encoder.local.model_cache_path,
                runtime=semantic_fit_config.cross_encoder.local.runtime,
                max_batch_size=semantic_fit_config.cross_encoder.local.max_batch_size,
                trust_remote_code=semantic_fit_config.cross_encoder.local.trust_remote_code,
            )
        remote_provider = None
        if semantic_fit_config.cross_encoder.remote.enabled and semantic_fit_config.cross_encoder.remote.base_url:
            remote_provider = RemoteCrossEncoderProvider(
                base_url=semantic_fit_config.cross_encoder.remote.base_url,
                api_key=semantic_fit_config.cross_encoder.remote.api_key,
                model=semantic_fit_config.cross_encoder.remote.model,
                timeout_ms=semantic_fit_config.cross_encoder.remote.timeout_ms,
            )
        llm_scorer = None
        llm_provider = self._resolve_llm_provider(ai_service)
        if llm_provider:
            llm_scorer = LLMSemanticFitScorer(
                ai_service=llm_provider,
                fallback_scorer=threshold_scorer,
            )
        cross_encoder_scorer = CrossEncoderSemanticFitScorer(
            local_provider=local_provider,
            remote_provider=remote_provider,
            fallback_scorer=threshold_scorer,
        )
        return SemanticFitRouter(
            repo=self.repo,
            config=self.config,
            threshold_scorer=threshold_scorer,
            cross_encoder_scorer=cross_encoder_scorer,
            llm_scorer=llm_scorer,
        )

    def _resolve_llm_provider(self, _ai_service: Optional[LLMProvider]) -> Optional[LLMProvider]:
        llm_config = getattr(getattr(self.config, "semantic_fit", None), "llm", None)
        if not llm_config or not getattr(llm_config, "enabled", False):
            return None
        return build_llm_provider(runtime_llm_config_from_fit(llm_config))

    def score_matches(
        self,
        preliminary_matches: List[JobMatchPreliminary],
        result_policy: Optional[ResultPolicy] = None,
        match_type: str = "requirements_only",
        stop_event: Optional[threading.Event] = None,
    ) -> List[ScoredJobMatch]:

        metadata_by_fp = _prefetch_resume_metadata(preliminary_matches, self.repo.db)

        scored = []
        for pm in preliminary_matches:
            if stop_event and stop_event.is_set():
                logger.info("ScoringService interrupted")
                return []
            
            scored.append(self.score_preliminary_match(
                preliminary=pm,
                match_type=match_type,
                candidate_total_years=(metadata_by_fp.get(pm.resume_fingerprint) or {}).get("total_years"),
                owner_id=pm.owner_id or (metadata_by_fp.get(pm.resume_fingerprint) or {}).get("owner_id"),
            ))

        scored.sort(key=lambda x: x.fit_score or 0.0, reverse=True)

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
        owner_id: Optional[Any] = None,
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
            owner_id=owner_id,
        )
        fit_components = dict(semantic_fit.fit_components)
        preferred_requirement_coverage = calculate_requirement_coverage(
            semantic_fit.matched_requirements,
            semantic_fit.missing_requirements,
            req_type="preferred",
            threshold=float(fit_components.get("threshold", 0.0)),
            clamp_similarity=bool(fit_components.get("similarity_clamp", True)),
        )["coverage"]

        return ScoredJobMatch(
            job=job,
            fit_score=semantic_fit.fit_score,
            fit_components=fit_components,
            preference_components={},
            fit_confidence=semantic_fit.fit_confidence,
            fit_explanation=semantic_fit.fit_explanation,
            fit_scorer={
                "name": semantic_fit.scorer_name,
                "version": semantic_fit.scorer_version,
            },
            base_score=fit_components.get("core", 0.0) * 100.0,
            penalties=fit_penalties,
            jd_required_coverage=fit_components["required_coverage"],
            jd_preferred_requirement_coverage=preferred_requirement_coverage,
            job_similarity=preliminary.job_similarity,
            penalty_details=penalty_details,
            matched_requirements=semantic_fit.matched_requirements,
            missing_requirements=semantic_fit.missing_requirements,
            resume_fingerprint=preliminary.resume_fingerprint,
            match_type=match_type,
            policy_applied=None,
        )


class SemanticFitRouter:
    def __init__(
        self,
        *,
        repo: JobRepository,
        config: ScorerConfig,
        threshold_scorer: ThresholdSemanticFitScorer,
        cross_encoder_scorer: CrossEncoderSemanticFitScorer,
        llm_scorer: Optional[LLMSemanticFitScorer],
    ):
        self.repo = repo
        self.config = config
        self.threshold_scorer = threshold_scorer
        self.cross_encoder_scorer = cross_encoder_scorer
        self.llm_scorer = llm_scorer

    def score(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
        owner_id: Optional[Any] = None,
    ):
        semantic_fit_config = getattr(config, "semantic_fit", None)
        if not semantic_fit_config or not semantic_fit_config.enabled:
            return self.threshold_scorer.score(
                preliminary,
                fit_penalties=fit_penalties,
                config=config,
            )

        resolved_mode, effective_allowed = resolve_effective_fit_mode(
            self.repo,
            config,
            owner_id or preliminary.owner_id,
        )
        if resolved_mode not in effective_allowed:
            resolved_mode = effective_allowed[0]

        if resolved_mode == "llm" and self.llm_scorer is not None and semantic_fit_config.llm.enabled:
            return self.llm_scorer.score(
                preliminary,
                fit_penalties=fit_penalties,
                config=config,
            )
        if resolved_mode == "llm" and "cross_encoder" in effective_allowed:
            return self.cross_encoder_scorer.score(
                preliminary,
                fit_penalties=fit_penalties,
                config=config,
            )
        if resolved_mode == "llm":
            raise RuntimeError(
                "Semantic fit mode resolved to 'llm', but no LLM scorer is configured and "
                "cross-encoder fallback is not allowed."
            )
        if resolved_mode == "cross_encoder":
            return self.cross_encoder_scorer.score(
                preliminary,
                fit_penalties=fit_penalties,
                config=config,
            )
        return self.threshold_scorer.score(
            preliminary,
            fit_penalties=fit_penalties,
            config=config,
        )
