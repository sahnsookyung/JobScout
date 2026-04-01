"""
Semantic fit scoring contracts and default implementation.

The default scorer still relies on the existing thresholded requirement matches
for its semantic evidence, but it centralizes fit aggregation and explanation
generation behind a dedicated scorer interface so stronger models can replace it
later without rewriting the scoring pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol

from core.config_loader import ScorerConfig
from core.matcher import JobMatchPreliminary, RequirementMatchResult
from core.scorer import fit_score

DEFAULT_SCORER_NAME = "threshold_semantic_fit"
DEFAULT_SCORER_VERSION = "1"


@dataclass(frozen=True)
class SemanticFitScoreResult:
    """Structured fit score output from a semantic fit scorer."""

    fit_score: float
    fit_components: Dict[str, Any]
    fit_confidence: float
    fit_explanation: Dict[str, Any]
    scorer_name: str
    scorer_version: str


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


def _build_requirement_verdict(
    requirement_match: RequirementMatchResult,
    *,
    threshold: float,
) -> Dict[str, Any]:
    similarity = float(requirement_match.similarity or 0.0)
    covered = bool(requirement_match.is_covered or similarity >= threshold)

    if covered:
        verdict = "covered"
        reason = "Resume evidence cleared the fit threshold for this requirement."
    elif similarity > 0:
        verdict = "partial"
        reason = "Resume evidence was related but did not clear the fit threshold."
    else:
        verdict = "missing"
        reason = "No supporting resume evidence was found for this requirement."

    return {
        "requirement_id": _requirement_id(requirement_match),
        "requirement_text": _requirement_text(requirement_match),
        "req_type": getattr(requirement_match.requirement, "req_type", "required"),
        "weight": _requirement_weight(requirement_match),
        "similarity": similarity,
        "is_covered": covered,
        "verdict": verdict,
        "reason": reason,
        "evidence_text": _evidence_text(requirement_match),
        "evidence_section": _evidence_section(requirement_match),
    }


def _top_strengths(verdicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    strengths = [verdict for verdict in verdicts if verdict["verdict"] == "covered"]
    strengths.sort(key=lambda verdict: verdict["similarity"], reverse=True)
    return strengths[:3]


def _top_gaps(verdicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    gaps = [verdict for verdict in verdicts if verdict["req_type"] == "required" and verdict["verdict"] != "covered"]
    gaps.sort(key=lambda verdict: (verdict["verdict"] != "missing", verdict["similarity"]))
    return gaps[:3]


def _build_summary(
    verdicts: List[Dict[str, Any]],
    *,
    required_coverage: float,
    preferred_coverage: float,
) -> str:
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
    required_percent = (required_covered / required_total) if required_total else 0.0
    preferred_percent = (preferred_covered / preferred_total) if preferred_total else 0.0
    return (
        f"Covered {required_covered} of {required_total} required requirements "
        f"({required_percent:.0%}) and {preferred_covered} of {preferred_total} preferred "
        f"requirements ({preferred_percent:.0%})."
    )


def _fit_confidence(required_coverage: float, job_similarity: float) -> float:
    return round(max(0.0, min(1.0, (float(required_coverage) + float(job_similarity)) / 2.0)), 4)


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
        fit_value, fit_components = fit_score.calculate_fit_score(
            job_similarity=preliminary.job_similarity,
            matched_requirements=preliminary.requirement_matches,
            missing_requirements=preliminary.missing_requirements,
            fit_penalties=fit_penalties,
            config=config,
        )

        threshold = float(fit_components.get("threshold", 0.0))
        verdicts = [
            _build_requirement_verdict(requirement_match, threshold=threshold)
            for requirement_match in preliminary.requirement_matches + preliminary.missing_requirements
        ]
        required_coverage = float(fit_components.get("required_coverage", 0.0))
        preferred_coverage = float(fit_components.get("preferred_coverage", 0.0))
        fit_confidence = _fit_confidence(required_coverage, preliminary.job_similarity)
        fit_explanation = {
            "summary": _build_summary(
                verdicts,
                required_coverage=required_coverage,
                preferred_coverage=preferred_coverage,
            ),
            "strengths": _top_strengths(verdicts),
            "gaps": _top_gaps(verdicts),
            "requirement_verdicts": verdicts,
            "required_coverage": required_coverage,
            "preferred_coverage": preferred_coverage,
            "fit_confidence": fit_confidence,
            "job_similarity": float(preliminary.job_similarity or 0.0),
        }

        enriched_components = dict(fit_components)
        enriched_components["fit_confidence"] = fit_confidence
        enriched_components["fit_scorer"] = {
            "name": self.scorer_name,
            "version": self.scorer_version,
        }
        enriched_components["fit_explanation"] = fit_explanation

        return SemanticFitScoreResult(
            fit_score=fit_value,
            fit_components=enriched_components,
            fit_confidence=fit_confidence,
            fit_explanation=fit_explanation,
            scorer_name=self.scorer_name,
            scorer_version=self.scorer_version,
        )
