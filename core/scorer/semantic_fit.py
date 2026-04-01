"""
Semantic fit scoring contracts and implementations.

This module provides:
- a default threshold-based scorer used as a safe fallback
- an LLM-backed semantic scorer that judges requirement/evidence pairs
  and then aggregates fit from those semantic verdicts
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

from pydantic import BaseModel, ConfigDict, Field

from core.config_loader import ScorerConfig
from core.llm.interfaces import LLMProvider
from core.matcher import JobMatchPreliminary, RequirementMatchResult
from core.scorer import fit_score

logger = logging.getLogger(__name__)

DEFAULT_SCORER_NAME = "threshold_semantic_fit"
DEFAULT_SCORER_VERSION = "1"
LLM_SCORER_NAME = "llm_semantic_fit"
LLM_SCORER_VERSION = "1"
SEMANTIC_FIT_SCHEMA_NAME = "semantic_fit_score_v1"

SEMANTIC_FIT_SYSTEM_PROMPT = """
You are a resume-to-job fit evaluator.

Task
- Evaluate whether each provided resume evidence item satisfies the paired job requirement.
- Judge semantic fit, not just lexical overlap.
- Be strict about technology mismatches: Java evidence does not satisfy Python requirements, and vice versa.
- Be strict about domain mismatches unless the evidence clearly shows the same capability.

Coverage levels
- covered: the evidence clearly satisfies the requirement
- partial: the evidence is related but incomplete or weaker than the requirement
- missing: the evidence does not support the requirement

Rules
- Use only the requirement text and paired evidence text provided in the payload.
- Do not infer additional resume evidence that is not present.
- For missing evidence, return `missing` with low semantic score.
- Keep reasons short and user-safe.
"""


class SemanticRequirementJudgment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requirement_id: str
    coverage_level: str = Field(pattern="^(covered|partial|missing)$")
    semantic_score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str


class SemanticJobFitAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str
    requirement_judgments: List[SemanticRequirementJudgment]


SEMANTIC_FIT_SCHEMA_SPEC = {
    "name": SEMANTIC_FIT_SCHEMA_NAME,
    "strict": True,
    "schema": SemanticJobFitAssessment.model_json_schema(),
}


@dataclass(frozen=True)
class SemanticFitScoreResult:
    """Structured fit score output from a semantic fit scorer."""

    fit_score: float
    fit_components: Dict[str, Any]
    fit_confidence: float
    fit_explanation: Dict[str, Any]
    scorer_name: str
    scorer_version: str
    matched_requirements: List[RequirementMatchResult]
    missing_requirements: List[RequirementMatchResult]


class SemanticFitScorer(Protocol):
    """Contract for shortlist-level semantic fit scorers."""

    def score(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
    ) -> SemanticFitScoreResult:
        """Score a preliminary match and return structured semantic outputs."""


def _requirement_text(requirement_match: RequirementMatchResult) -> str:
    return getattr(requirement_match.requirement, "text", "") or ""


def _requirement_id(requirement_match: RequirementMatchResult) -> str:
    requirement_id = getattr(requirement_match.requirement, "id", "")
    return str(requirement_id) if requirement_id is not None else ""


def _requirement_weight(requirement_match: RequirementMatchResult) -> float:
    weight = getattr(requirement_match.requirement, "weight", 1.0)
    try:
        return float(weight)
    except Exception:
        return 1.0


def _evidence_text(requirement_match: RequirementMatchResult) -> str:
    evidence = requirement_match.evidence
    return getattr(evidence, "text", "") if evidence else ""


def _evidence_section(requirement_match: RequirementMatchResult) -> str | None:
    evidence = requirement_match.evidence
    return getattr(evidence, "source_section", None) if evidence else None


def _string_or_empty(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _fit_confidence(required_coverage: float, job_similarity: float) -> float:
    return round(max(0.0, min(1.0, (float(required_coverage) + float(job_similarity)) / 2.0)), 4)


def _count_percent(count: int, total: int) -> float:
    return (count / total) if total else 0.0


def _verdicts_to_summary(verdicts: List[Dict[str, Any]]) -> str:
    required_total = len([verdict for verdict in verdicts if verdict["req_type"] == "required"])
    required_covered = len(
        [
            verdict
            for verdict in verdicts
            if verdict["req_type"] == "required" and verdict["verdict"] == "covered"
        ]
    )
    preferred_total = len([verdict for verdict in verdicts if verdict["req_type"] == "preferred"])
    preferred_covered = len(
        [
            verdict
            for verdict in verdicts
            if verdict["req_type"] == "preferred" and verdict["verdict"] == "covered"
        ]
    )
    required_percent = _count_percent(required_covered, required_total)
    preferred_percent = _count_percent(preferred_covered, preferred_total)
    return (
        f"Covered {required_covered} of {required_total} required requirements "
        f"({required_percent:.0%}) and {preferred_covered} of {preferred_total} preferred "
        f"requirements ({preferred_percent:.0%})."
    )


def _build_retrieval_diagnostics(preliminary: JobMatchPreliminary) -> Dict[str, Any]:
    lexical_score = preliminary.lexical_score
    retrieval_mode = "hybrid" if lexical_score is not None else "dense"
    retrieval_sources = ["dense", "lexical"] if lexical_score is not None else ["dense"]
    diagnostics = {
        "mode": retrieval_mode,
        "sources": retrieval_sources,
        "retrieval_score": float(preliminary.retrieval_score or 0.0),
        "job_similarity": float(preliminary.job_similarity or 0.0),
    }
    if lexical_score is not None:
        diagnostics["lexical_score"] = float(lexical_score)
    return diagnostics


def _build_scorer_diagnostics(
    *,
    scorer_name: str,
    scorer_version: str,
    latency_ms: float,
    fallback_used: bool = False,
    fallback_reason: str | None = None,
    judged_requirements: int | None = None,
) -> Dict[str, Any]:
    diagnostics = {
        "name": scorer_name,
        "version": scorer_version,
        "latency_ms": latency_ms,
        "fallback_used": fallback_used,
    }
    if fallback_reason:
        diagnostics["fallback_reason"] = fallback_reason
    if judged_requirements is not None:
        diagnostics["judged_requirements"] = judged_requirements
    return diagnostics


def _top_strengths(verdicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    strengths = [verdict for verdict in verdicts if verdict["verdict"] == "covered"]
    strengths.sort(
        key=lambda verdict: (verdict.get("semantic_score", 0.0), verdict.get("similarity", 0.0)),
        reverse=True,
    )
    return strengths[:3]


def _top_gaps(verdicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    gaps = [verdict for verdict in verdicts if verdict["req_type"] == "required" and verdict["verdict"] != "covered"]
    gaps.sort(key=lambda verdict: (verdict["verdict"] != "missing", verdict.get("semantic_score", 0.0)))
    return gaps[:3]


def _clone_match(
    requirement_match: RequirementMatchResult,
    *,
    similarity: float,
    is_covered: bool,
) -> RequirementMatchResult:
    return RequirementMatchResult(
        requirement=requirement_match.requirement,
        evidence=requirement_match.evidence,
        similarity=similarity,
        is_covered=is_covered,
    )


def _semantic_similarity(
    semantic_score: float,
    coverage_level: str,
    threshold: float,
) -> float:
    if coverage_level == "covered":
        return max(threshold, semantic_score)
    if coverage_level == "partial":
        return min(max(semantic_score, 0.0), max(threshold - 0.01, 0.0))
    return 0.0


def _fallback_coverage_level(requirement_match: RequirementMatchResult) -> str:
    if requirement_match.is_covered:
        return "covered"
    if requirement_match.similarity and requirement_match.similarity > 0:
        return "partial"
    return "missing"


def _base_verdict(
    requirement_match: RequirementMatchResult,
    *,
    coverage_level: str,
    semantic_score: float,
    confidence: float,
    reason: str,
) -> Dict[str, Any]:
    return {
        "requirement_id": _requirement_id(requirement_match),
        "requirement_text": _requirement_text(requirement_match),
        "req_type": getattr(requirement_match.requirement, "req_type", "required"),
        "weight": _requirement_weight(requirement_match),
        "similarity": float(requirement_match.similarity or 0.0),
        "semantic_score": semantic_score,
        "confidence": confidence,
        "is_covered": coverage_level == "covered",
        "verdict": coverage_level,
        "reason": reason,
        "evidence_text": _evidence_text(requirement_match),
        "evidence_section": _evidence_section(requirement_match),
    }


def _threshold_verdict(requirement_match: RequirementMatchResult, *, threshold: float) -> Dict[str, Any]:
    coverage_level = _fallback_coverage_level(requirement_match)
    semantic_score = _semantic_similarity(
        float(requirement_match.similarity or 0.0),
        coverage_level,
        threshold,
    )
    if coverage_level == "covered":
        reason = "Resume evidence cleared the fit threshold for this requirement."
    elif coverage_level == "partial":
        reason = "Resume evidence was related but did not clear the fit threshold."
    else:
        reason = "No supporting resume evidence was found for this requirement."
    return _base_verdict(
        requirement_match,
        coverage_level=coverage_level,
        semantic_score=semantic_score,
        confidence=_fit_confidence(float(requirement_match.similarity or 0.0), float(requirement_match.similarity or 0.0)),
        reason=reason,
    )


def _build_fit_explanation(
    verdicts: List[Dict[str, Any]],
    *,
    required_coverage: float,
    preferred_coverage: float,
    fit_confidence: float,
    job_similarity: float,
    scorer_name: str,
    scorer_version: str,
    retrieval_diagnostics: Dict[str, Any],
    scorer_diagnostics: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "summary": _verdicts_to_summary(verdicts),
        "strengths": _top_strengths(verdicts),
        "gaps": _top_gaps(verdicts),
        "requirement_verdicts": verdicts,
        "required_coverage": required_coverage,
        "preferred_coverage": preferred_coverage,
        "fit_confidence": fit_confidence,
        "job_similarity": float(job_similarity or 0.0),
        "fit_scorer": {
            "name": scorer_name,
            "version": scorer_version,
        },
        "retrieval": retrieval_diagnostics,
        "diagnostics": scorer_diagnostics,
    }


class ThresholdSemanticFitScorer:
    """Default semantic fit scorer backed by thresholded requirement evidence."""

    scorer_name = DEFAULT_SCORER_NAME
    scorer_version = DEFAULT_SCORER_VERSION

    def score(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
    ) -> SemanticFitScoreResult:
        started_at = time.perf_counter()
        fit_value, fit_components = fit_score.calculate_fit_score(
            job_similarity=preliminary.job_similarity,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            fit_penalties=fit_penalties,
            config=config,
        )

        threshold = float(fit_components.get("threshold", 0.0))
        matched_requirements = list(preliminary.requirement_matches)
        missing_requirements = list(preliminary.missing_requirements)
        verdicts = [
            _threshold_verdict(requirement_match, threshold=threshold)
            for requirement_match in matched_requirements + missing_requirements
        ]
        required_coverage = float(fit_components.get("required_coverage", 0.0))
        preferred_coverage = float(fit_components.get("preferred_coverage", 0.0))
        fit_confidence = _fit_confidence(required_coverage, preliminary.job_similarity)
        retrieval_diagnostics = _build_retrieval_diagnostics(preliminary)
        scorer_diagnostics = _build_scorer_diagnostics(
            scorer_name=self.scorer_name,
            scorer_version=self.scorer_version,
            latency_ms=round((time.perf_counter() - started_at) * 1000.0, 3),
        )
        fit_explanation = _build_fit_explanation(
            verdicts,
            required_coverage=required_coverage,
            preferred_coverage=preferred_coverage,
            fit_confidence=fit_confidence,
            job_similarity=preliminary.job_similarity,
            scorer_name=self.scorer_name,
            scorer_version=self.scorer_version,
            retrieval_diagnostics=retrieval_diagnostics,
            scorer_diagnostics=scorer_diagnostics,
        )

        enriched_components = dict(fit_components)
        enriched_components["fit_confidence"] = fit_confidence
        enriched_components["fit_scorer"] = {
            "name": self.scorer_name,
            "version": self.scorer_version,
        }
        enriched_components["retrieval"] = retrieval_diagnostics
        enriched_components["semantic_fit_diagnostics"] = scorer_diagnostics
        enriched_components["fit_explanation"] = fit_explanation

        return SemanticFitScoreResult(
            fit_score=fit_value,
            fit_components=enriched_components,
            fit_confidence=fit_confidence,
            fit_explanation=fit_explanation,
            scorer_name=self.scorer_name,
            scorer_version=self.scorer_version,
            matched_requirements=matched_requirements,
            missing_requirements=missing_requirements,
        )


class LLMSemanticFitScorer:
    """LLM-backed semantic fit scorer with threshold fallback."""

    scorer_name = LLM_SCORER_NAME
    scorer_version = LLM_SCORER_VERSION

    def __init__(
        self,
        ai_service: LLMProvider,
        *,
        fallback_scorer: Optional[ThresholdSemanticFitScorer] = None,
    ):
        self.ai_service = ai_service
        self.fallback_scorer = fallback_scorer or ThresholdSemanticFitScorer()

    def score(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
    ) -> SemanticFitScoreResult:
        started_at = time.perf_counter()
        if not getattr(config, "semantic_fit_enabled", True):
            return self.fallback_scorer.score(
                preliminary,
                fit_penalties=fit_penalties,
                config=config,
            )

        try:
            return self._score_with_llm(
                preliminary,
                fit_penalties=fit_penalties,
                config=config,
            )
        except Exception as exc:
            if not getattr(config, "semantic_fit_fallback_to_threshold", True):
                raise
            logger.warning("Semantic fit scoring failed; falling back to threshold scorer: %s", exc, exc_info=True)
            fallback = self.fallback_scorer.score(
                preliminary,
                fit_penalties=fit_penalties,
                config=config,
            )
            enriched_components = dict(fallback.fit_components)
            fallback_explanation = dict(fallback.fit_explanation)
            fallback_explanation["message"] = "Semantic fit scorer unavailable; using threshold fallback."
            fallback_reason = str(exc)
            scorer_diagnostics = _build_scorer_diagnostics(
                scorer_name=fallback.scorer_name,
                scorer_version=fallback.scorer_version,
                latency_ms=round((time.perf_counter() - started_at) * 1000.0, 3),
                fallback_used=True,
                fallback_reason=fallback_reason,
            )
            enriched_components["semantic_fit_fallback_reason"] = fallback_reason
            enriched_components["semantic_fit_diagnostics"] = scorer_diagnostics
            fallback_explanation["diagnostics"] = scorer_diagnostics
            enriched_components["fit_explanation"] = fallback_explanation
            return SemanticFitScoreResult(
                fit_score=fallback.fit_score,
                fit_components=enriched_components,
                fit_confidence=fallback.fit_confidence,
                fit_explanation=fallback_explanation,
                scorer_name=fallback.scorer_name,
                scorer_version=fallback.scorer_version,
                matched_requirements=fallback.matched_requirements,
                missing_requirements=fallback.missing_requirements,
            )

    def _score_with_llm(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
    ) -> SemanticFitScoreResult:
        started_at = time.perf_counter()
        payload = self._build_payload(preliminary)
        assessment = self.ai_service.extract_structured_data(
            text=json.dumps(payload),
            schema_spec=SEMANTIC_FIT_SCHEMA_SPEC,
            system_prompt=SEMANTIC_FIT_SYSTEM_PROMPT,
            user_message=(
                "Evaluate the semantic fit between each job requirement and the paired resume evidence.\n\n"
                f"{json.dumps(payload)}"
            ),
        )
        parsed = SemanticJobFitAssessment.model_validate(assessment)
        threshold = float(getattr(config, "req_similarity_threshold", fit_score.DEFAULT_REQ_SIMILARITY_THRESHOLD))
        judgments = {judgment.requirement_id: judgment for judgment in parsed.requirement_judgments}

        adjusted_matched: List[RequirementMatchResult] = []
        adjusted_missing: List[RequirementMatchResult] = []
        verdicts: List[Dict[str, Any]] = []

        for requirement_match in preliminary.requirement_matches + preliminary.missing_requirements:
            requirement_id = _requirement_id(requirement_match)
            judgment = judgments.get(requirement_id)
            if judgment is None:
                coverage_level = _fallback_coverage_level(requirement_match)
                semantic_score = _semantic_similarity(float(requirement_match.similarity or 0.0), coverage_level, threshold)
                confidence = 0.0
                reason = "Requirement was not judged by the semantic scorer; preserved fallback classification."
            else:
                coverage_level = judgment.coverage_level
                semantic_score = max(0.0, min(1.0, float(judgment.semantic_score)))
                confidence = max(0.0, min(1.0, float(judgment.confidence)))
                reason = judgment.reason

            similarity = _semantic_similarity(semantic_score, coverage_level, threshold)
            adjusted = _clone_match(
                requirement_match,
                similarity=similarity,
                is_covered=coverage_level == "covered",
            )
            if adjusted.is_covered:
                adjusted_matched.append(adjusted)
            else:
                adjusted_missing.append(adjusted)

            verdicts.append(
                _base_verdict(
                    requirement_match,
                    coverage_level=coverage_level,
                    semantic_score=semantic_score,
                    confidence=confidence,
                    reason=reason,
                )
            )

        fit_value, fit_components = fit_score.calculate_fit_score(
            job_similarity=preliminary.job_similarity,
            matched_requirements=adjusted_matched,
            missing_requirements=adjusted_missing,
            fit_penalties=fit_penalties,
            config=config,
        )
        required_coverage = float(fit_components.get("required_coverage", 0.0))
        preferred_coverage = float(fit_components.get("preferred_coverage", 0.0))
        confidence_values = [verdict["confidence"] for verdict in verdicts if verdict["confidence"] > 0]
        base_confidence = (sum(confidence_values) / len(confidence_values)) if confidence_values else 0.0
        fit_confidence = round(max(base_confidence, _fit_confidence(required_coverage, preliminary.job_similarity)), 4)
        retrieval_diagnostics = _build_retrieval_diagnostics(preliminary)
        scorer_diagnostics = _build_scorer_diagnostics(
            scorer_name=self.scorer_name,
            scorer_version=self.scorer_version,
            latency_ms=round((time.perf_counter() - started_at) * 1000.0, 3),
            judged_requirements=len(verdicts),
        )
        fit_explanation = _build_fit_explanation(
            verdicts,
            required_coverage=required_coverage,
            preferred_coverage=preferred_coverage,
            fit_confidence=fit_confidence,
            job_similarity=preliminary.job_similarity,
            scorer_name=self.scorer_name,
            scorer_version=self.scorer_version,
            retrieval_diagnostics=retrieval_diagnostics,
            scorer_diagnostics=scorer_diagnostics,
        )

        enriched_components = dict(fit_components)
        enriched_components["fit_confidence"] = fit_confidence
        enriched_components["fit_scorer"] = {
            "name": self.scorer_name,
            "version": self.scorer_version,
        }
        enriched_components["retrieval"] = retrieval_diagnostics
        enriched_components["semantic_fit_diagnostics"] = scorer_diagnostics
        enriched_components["fit_explanation"] = fit_explanation
        enriched_components["semantic_fit_judged_requirements"] = len(verdicts)
        enriched_components["semantic_fit_summary"] = parsed.summary

        return SemanticFitScoreResult(
            fit_score=fit_value,
            fit_components=enriched_components,
            fit_confidence=fit_confidence,
            fit_explanation=fit_explanation,
            scorer_name=self.scorer_name,
            scorer_version=self.scorer_version,
            matched_requirements=adjusted_matched,
            missing_requirements=adjusted_missing,
        )

    @staticmethod
    def _build_payload(preliminary: JobMatchPreliminary) -> Dict[str, Any]:
        return {
            "job_id": str(getattr(preliminary.job, "id", "")),
            "job_title": _string_or_empty(getattr(preliminary.job, "title", "")),
            "job_company": _string_or_empty(getattr(preliminary.job, "company", "")),
            "job_summary": _string_or_empty(getattr(preliminary.job, "canonical_job_summary", ""))
            or _string_or_empty(getattr(preliminary.job, "description", "")),
            "requirements": [
                {
                    "requirement_id": _requirement_id(requirement_match),
                    "requirement_text": _requirement_text(requirement_match),
                    "req_type": getattr(requirement_match.requirement, "req_type", "required"),
                    "evidence_text": _evidence_text(requirement_match),
                    "evidence_section": _evidence_section(requirement_match),
                    "original_similarity": float(requirement_match.similarity or 0.0),
                }
                for requirement_match in preliminary.requirement_matches + preliminary.missing_requirements
            ],
        }
