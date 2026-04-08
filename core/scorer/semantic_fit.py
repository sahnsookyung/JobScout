"""
Semantic fit scoring contracts and implementations.

This module provides:
- threshold fallback scoring
- routed semantic fit scoring across cross-encoder and LLM providers
- per-pair truncation diagnostics for later observability
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Protocol

import requests
from pydantic import BaseModel, ConfigDict, Field

from core.config_loader import ScorerConfig, SemanticFitConfig
from core.llm.interfaces import LLMProvider
from core.matcher import (
    JobMatchPreliminary,
    RequirementEvidenceCandidate,
    RequirementMatchResult,
)
from core.scorer import fit_score

logger = logging.getLogger(__name__)

THRESHOLD_SCORER_NAME = "threshold_semantic_fit"
THRESHOLD_SCORER_VERSION = "2"
CROSS_ENCODER_SCORER_NAME = "cross_encoder_semantic_fit"
CROSS_ENCODER_SCORER_VERSION = "1"
LLM_SCORER_NAME = "llm_semantic_fit"
LLM_SCORER_VERSION = "2"
SEMANTIC_PAIR_SCHEMA_NAME = "semantic_fit_pairs_v1"

FEATURE_ALLOWED_MODES = "fit.semantic.allowed_modes"
FEATURE_PREFERRED_MODE = "fit.semantic.preferred_mode"
REASON_COVERED = "Evidence strongly matches the requirement."
REASON_PARTIAL = "Evidence is related but does not fully satisfy the requirement."
REASON_MISSING = "Evidence does not support the requirement."
REASON_NO_EVIDENCE = "No supporting resume evidence was recalled for this requirement."
REASON_PRESERVED_MATCHER = (
    "No supporting resume evidence payload was available; preserved matcher classification."
)
REASON_UNJUDGED = (
    "Requirement was not judged by the semantic scorer; preserved fallback classification."
)
THRESHOLD_FALLBACK_MESSAGE = "Semantic fit scorer unavailable; using threshold fallback."

_GENERIC_TOKENS = {
    "a", "an", "and", "api", "apis", "as", "at", "be", "build", "building",
    "developer", "development", "engineer", "engineering", "experience", "for",
    "from", "hands", "in", "into", "is", "it", "knowledge", "of", "on", "or",
    "plus", "required", "service", "services", "skill", "skills", "that", "the",
    "to", "using", "with", "year", "years",
}
_TECH_MISMATCH_GROUPS = [
    {"java", "python"},
    {"react", "angular", "vue"},
    {"aws", "gcp", "azure"},
]

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
""".strip()


class SemanticPairJudgment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pair_id: str
    requirement_id: str
    coverage_level: str = Field(pattern="^(covered|partial|missing)$")
    semantic_score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str


class SemanticPairAssessmentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str
    pair_judgments: List[SemanticPairJudgment]


SEMANTIC_FIT_SCHEMA_SPEC = {
    "name": SEMANTIC_PAIR_SCHEMA_NAME,
    "strict": True,
    "schema": SemanticPairAssessmentResponse.model_json_schema(),
}


@dataclass(frozen=True)
class SemanticFitScoreResult:
    fit_score: float
    fit_components: Dict[str, Any]
    fit_confidence: float
    fit_explanation: Dict[str, Any]
    scorer_name: str
    scorer_version: str
    matched_requirements: List[RequirementMatchResult]
    missing_requirements: List[RequirementMatchResult]


class SemanticFitScorer(Protocol):
    def score(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
        owner_id: Any | None = None,
    ) -> SemanticFitScoreResult:
        """Score a preliminary match and return structured semantic outputs."""


@dataclass(frozen=True)
class SerializedPair:
    pair_id: str
    requirement_id: str
    requirement_match: RequirementMatchResult
    candidate: RequirementEvidenceCandidate
    fields: Dict[str, str]
    truncation: Dict[str, Any]


@dataclass(frozen=True)
class PairAssessment:
    pair_id: str
    requirement_id: str
    coverage_level: str
    semantic_score: float
    confidence: float
    reason: str


@dataclass(frozen=True)
class AssessmentScoringMetadata:
    scorer_name: str
    scorer_version: str
    provider_route: str
    provider_id: str
    threshold: float
    latency_ms: float
    model_summary: Optional[str]
    truncation_aggregate: Dict[str, Any]
    fallback_used: bool = False
    fallback_reason: Optional[str] = None


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9_+#.-]+", (text or "").lower())


def _meaningful_overlap(left: str, right: str) -> set[str]:
    left_tokens = {token for token in _tokenize(left) if token not in _GENERIC_TOKENS}
    right_tokens = {token for token in _tokenize(right) if token not in _GENERIC_TOKENS}
    return left_tokens & right_tokens


def _explicit_tech_mismatch(left: str, right: str) -> bool:
    left_tokens = set(_tokenize(left))
    right_tokens = set(_tokenize(right))
    for group in _TECH_MISMATCH_GROUPS:
        if left_tokens & group and right_tokens & group and not ((left_tokens & group) & (right_tokens & group)):
            return True
    return False


def _string_or_empty(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _requirement_id(requirement_match: RequirementMatchResult) -> str:
    value = getattr(requirement_match.requirement, "id", "")
    return str(value) if value is not None else ""


def _requirement_text(requirement_match: RequirementMatchResult) -> str:
    return _string_or_empty(getattr(requirement_match.requirement, "text", ""))


def _requirement_weight(requirement_match: RequirementMatchResult) -> float:
    weight = getattr(requirement_match.requirement, "weight", 1.0)
    try:
        return float(weight)
    except Exception:
        return 1.0


def _requirement_type(requirement_match: RequirementMatchResult) -> str:
    return _string_or_empty(getattr(requirement_match.requirement, "req_type", "required")) or "required"


def _fit_confidence(required_coverage: float, job_similarity: float) -> float:
    return round(max(0.0, min(1.0, (float(required_coverage) + float(job_similarity)) / 2.0)), 4)


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


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _normalize_semantic_score(raw_score: float) -> float:
    value = float(raw_score)
    if 0.0 <= value <= 1.0:
        return _clamp01(value)
    return 1.0 / (1.0 + math.exp(-value))


def _count_percent(count: int, total: int) -> float:
    return (count / total) if total else 0.0


def _verdicts_to_summary(verdicts: List[Dict[str, Any]]) -> str:
    required_total = len([v for v in verdicts if v["req_type"] == "required"])
    required_covered = len([v for v in verdicts if v["req_type"] == "required" and v["verdict"] == "covered"])
    preferred_total = len([v for v in verdicts if v["req_type"] == "preferred"])
    preferred_covered = len([v for v in verdicts if v["req_type"] == "preferred" and v["verdict"] == "covered"])
    return (
        f"Covered {required_covered} of {required_total} required requirements "
        f"({_count_percent(required_covered, required_total):.0%}) and "
        f"{preferred_covered} of {preferred_total} preferred requirements "
        f"({_count_percent(preferred_covered, preferred_total):.0%})."
    )


def _top_strengths(verdicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    strengths = [verdict for verdict in verdicts if verdict["verdict"] == "covered"]
    strengths.sort(key=lambda verdict: (verdict.get("semantic_score", 0.0), verdict.get("similarity", 0.0)), reverse=True)
    return strengths[:3]


def _top_gaps(verdicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    gaps = [verdict for verdict in verdicts if verdict["req_type"] == "required" and verdict["verdict"] != "covered"]
    gaps.sort(key=lambda verdict: (verdict["verdict"] != "missing", verdict.get("semantic_score", 0.0)))
    return gaps[:3]


def _build_fit_explanation(
    verdicts: List[Dict[str, Any]],
    *,
    required_coverage: float,
    preferred_requirement_coverage: float,
    fit_confidence: float,
    job_similarity: float,
    scorer_name: str,
    scorer_version: str,
    retrieval_diagnostics: Dict[str, Any],
    scorer_diagnostics: Dict[str, Any],
    fallback_message: str | None = None,
) -> Dict[str, Any]:
    explanation = {
        "summary": _verdicts_to_summary(verdicts),
        "strengths": _top_strengths(verdicts),
        "gaps": _top_gaps(verdicts),
        "requirement_verdicts": verdicts,
        "required_coverage": required_coverage,
        "preferred_requirement_coverage": preferred_requirement_coverage,
        "fit_confidence": fit_confidence,
        "job_similarity": float(job_similarity or 0.0),
        "fit_scorer": {"name": scorer_name, "version": scorer_version},
        "retrieval": retrieval_diagnostics,
        "diagnostics": scorer_diagnostics,
    }
    if fallback_message:
        explanation["message"] = fallback_message
    return explanation


def _clone_match(
    requirement_match: RequirementMatchResult,
    *,
    evidence: Optional[Any],
    similarity: float,
    is_covered: bool,
) -> RequirementMatchResult:
    return RequirementMatchResult(
        requirement=requirement_match.requirement,
        evidence=evidence,
        similarity=similarity,
        is_covered=is_covered,
        evidence_candidates=list(requirement_match.evidence_candidates),
    )


def _semantic_similarity(semantic_score: float, coverage_level: str, threshold: float) -> float:
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
    evidence: Optional[Any],
    coverage_level: str,
    semantic_score: float,
    confidence: float,
    reason: str,
    provider_route: Optional[str] = None,
    truncation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    evidence_text = getattr(evidence, "text", "") if evidence else ""
    evidence_section = getattr(evidence, "source_section", None) if evidence else None
    verdict = {
        "requirement_id": _requirement_id(requirement_match),
        "requirement_text": _requirement_text(requirement_match),
        "req_type": _requirement_type(requirement_match),
        "weight": _requirement_weight(requirement_match),
        "similarity": float(getattr(requirement_match, "similarity", 0.0) or 0.0),
        "semantic_score": semantic_score,
        "confidence": confidence,
        "is_covered": coverage_level == "covered",
        "verdict": coverage_level,
        "reason": reason,
        "evidence_text": evidence_text,
        "evidence_section": evidence_section,
    }
    if provider_route:
        verdict["provider_route"] = provider_route
    if truncation:
        verdict["truncation"] = truncation
    return verdict


def _threshold_verdict(requirement_match: RequirementMatchResult, *, threshold: float) -> Dict[str, Any]:
    coverage_level = _fallback_coverage_level(requirement_match)
    semantic_score = _semantic_similarity(float(requirement_match.similarity or 0.0), coverage_level, threshold)
    if coverage_level == "covered":
        reason = "Resume evidence cleared the fit threshold for this requirement."
    elif coverage_level == "partial":
        reason = "Resume evidence was related but did not clear the fit threshold."
    else:
        reason = "No supporting resume evidence was found for this requirement."
    return _base_verdict(
        requirement_match,
        evidence=requirement_match.evidence,
        coverage_level=coverage_level,
        semantic_score=semantic_score,
        confidence=_fit_confidence(float(requirement_match.similarity or 0.0), float(requirement_match.similarity or 0.0)),
        reason=reason,
    )


def _build_threshold_result(
    preliminary: JobMatchPreliminary,
    *,
    fit_penalties: float,
    config: ScorerConfig,
    scorer_name: str,
    scorer_version: str,
    fallback_used: bool = False,
    fallback_reason: str | None = None,
    fallback_message: str | None = None,
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
    verdicts = [
        _threshold_verdict(requirement_match, threshold=threshold)
        for requirement_match in preliminary.requirement_matches + preliminary.missing_requirements
    ]
    required_coverage = float(fit_components.get("required_coverage", 0.0))
    preferred_requirement_coverage = float(
        fit_components.get("preferred_requirement_coverage", 0.0)
    )
    fit_confidence = _fit_confidence(required_coverage, preliminary.job_similarity)
    retrieval_diagnostics = _build_retrieval_diagnostics(preliminary)
    scorer_diagnostics = {
        "name": scorer_name,
        "version": scorer_version,
        "effective_fit_mode": "threshold",
        "provider_route": "threshold",
        "latency_ms": round((time.perf_counter() - started_at) * 1000.0, 3),
        "fallback_used": fallback_used,
        "judged_requirements": len(verdicts),
        "truncation": {
            "any_truncated": False,
            "pair_count": 0,
            "truncated_pair_count": 0,
            "total_truncated_chars": 0,
            "emergency_ceiling_hits": 0,
        },
    }
    if fallback_reason:
        scorer_diagnostics["fallback_reason"] = fallback_reason
    fit_explanation = _build_fit_explanation(
        verdicts,
        required_coverage=required_coverage,
        preferred_requirement_coverage=preferred_requirement_coverage,
        fit_confidence=fit_confidence,
        job_similarity=preliminary.job_similarity,
        scorer_name=scorer_name,
        scorer_version=scorer_version,
        retrieval_diagnostics=retrieval_diagnostics,
        scorer_diagnostics=scorer_diagnostics,
        fallback_message=fallback_message,
    )
    enriched_components = dict(fit_components)
    enriched_components["fit_confidence"] = fit_confidence
    enriched_components["fit_scorer"] = {"name": scorer_name, "version": scorer_version}
    enriched_components["effective_fit_mode"] = "threshold"
    enriched_components["provider_route"] = "threshold"
    enriched_components["retrieval"] = retrieval_diagnostics
    enriched_components["semantic_fit_diagnostics"] = scorer_diagnostics
    if fallback_reason:
        enriched_components["semantic_fit_fallback_reason"] = fallback_reason
    enriched_components["fit_explanation"] = fit_explanation
    return SemanticFitScoreResult(
        fit_score=fit_value,
        fit_components=enriched_components,
        fit_confidence=fit_confidence,
        fit_explanation=fit_explanation,
        scorer_name=scorer_name,
        scorer_version=scorer_version,
        matched_requirements=list(preliminary.requirement_matches),
        missing_requirements=list(preliminary.missing_requirements),
    )


class ThresholdSemanticFitScorer:
    scorer_name = THRESHOLD_SCORER_NAME
    scorer_version = THRESHOLD_SCORER_VERSION

    def score(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
        owner_id: Any | None = None,
    ) -> SemanticFitScoreResult:
        del owner_id
        return _build_threshold_result(
            preliminary,
            fit_penalties=fit_penalties,
            config=config,
            scorer_name=self.scorer_name,
            scorer_version=self.scorer_version,
        )


def _serialization_config(config: ScorerConfig):
    return getattr(getattr(config, "semantic_fit", None), "serialization", None)


def _truncate_field(value: str, max_chars: int, ceiling_chars: int) -> tuple[str, Dict[str, Any]]:
    original = len(value)
    submitted = value
    truncated = False
    if original > max_chars:
        submitted = value[:max_chars]
        truncated = True
    emergency_ceiling_hit = False
    if len(submitted) > ceiling_chars:
        submitted = submitted[:ceiling_chars]
        truncated = True
        emergency_ceiling_hit = True
    return submitted, {
        "original_length": original,
        "submitted_length": len(submitted),
        "truncated": truncated,
        "discarded_chars": max(0, original - len(submitted)),
        "emergency_ceiling_hit": emergency_ceiling_hit,
    }


def _serialize_pair(
    preliminary: JobMatchPreliminary,
    requirement_match: RequirementMatchResult,
    candidate: RequirementEvidenceCandidate,
    *,
    config: ScorerConfig,
) -> SerializedPair:
    serialization = _serialization_config(config)
    budgets = {
        "requirement_text": getattr(serialization, "requirement_text_max_chars", 500),
        "evidence_text": getattr(serialization, "evidence_text_max_chars", 2500),
        "evidence_section": getattr(serialization, "evidence_section_max_chars", 64),
        "job_title": getattr(serialization, "job_title_max_chars", 200),
        "job_company": getattr(serialization, "job_company_max_chars", 200),
        "job_summary": getattr(serialization, "job_summary_max_chars", 1800),
        "req_type": 32,
    }
    emergency_ceiling = 8000
    raw_fields = {
        "requirement_text": _requirement_text(requirement_match),
        "req_type": _requirement_type(requirement_match),
        "evidence_text": getattr(candidate.evidence, "text", "") or "",
        "evidence_section": getattr(candidate.evidence, "source_section", "") or "",
        "job_title": _string_or_empty(getattr(preliminary.job, "title", "")),
        "job_company": _string_or_empty(getattr(preliminary.job, "company", "")),
        "job_summary": _string_or_empty(getattr(preliminary.job, "canonical_job_summary", ""))
        or _string_or_empty(getattr(preliminary.job, "description", "")),
    }
    fields: Dict[str, str] = {}
    per_field: Dict[str, Dict[str, Any]] = {}
    truncated_fields: List[str] = []
    total_truncated_chars = 0
    emergency_hits = 0
    for key, raw_value in raw_fields.items():
        truncated, diagnostics = _truncate_field(raw_value, budgets[key], emergency_ceiling)
        fields[key] = truncated
        per_field[key] = diagnostics
        total_truncated_chars += int(diagnostics["discarded_chars"])
        if diagnostics["truncated"]:
            truncated_fields.append(key)
        if diagnostics["emergency_ceiling_hit"]:
            emergency_hits += 1

    pair_id_source = (
        f"{getattr(preliminary.job, 'id', '')}|{_requirement_id(requirement_match)}|"
        f"{candidate.rank}|{fields['evidence_section']}|{fields['evidence_text']}"
    )
    pair_id = hashlib.sha256(pair_id_source.encode("utf-8")).hexdigest()[:32]
    truncation = {
        "truncated": bool(truncated_fields),
        "truncated_fields": truncated_fields,
        "total_truncated_chars": total_truncated_chars,
        "emergency_ceiling_hit": emergency_hits > 0,
        "field_lengths": per_field,
    }
    return SerializedPair(
        pair_id=pair_id,
        requirement_id=_requirement_id(requirement_match),
        requirement_match=requirement_match,
        candidate=candidate,
        fields=fields,
        truncation=truncation,
    )


def _pair_assessment_from_heuristic(pair: SerializedPair) -> PairAssessment:
    requirement_text = pair.fields["requirement_text"]
    evidence_text = pair.fields["evidence_text"]
    original_similarity = float(pair.candidate.similarity or 0.0)
    overlap = _meaningful_overlap(requirement_text, evidence_text)
    if not evidence_text:
        return PairAssessment(
            pair_id=pair.pair_id,
            requirement_id=pair.requirement_id,
            coverage_level="missing",
            semantic_score=0.0,
            confidence=1.0,
            reason=REASON_NO_EVIDENCE,
        )
    if _explicit_tech_mismatch(requirement_text, evidence_text):
        return PairAssessment(
            pair_id=pair.pair_id,
            requirement_id=pair.requirement_id,
            coverage_level="missing",
            semantic_score=0.0,
            confidence=0.92,
            reason="Evidence references different technologies than the requirement.",
        )
    if overlap:
        score = min(0.97, 0.75 + 0.05 * len(overlap) + 0.1 * original_similarity)
        return PairAssessment(
            pair_id=pair.pair_id,
            requirement_id=pair.requirement_id,
            coverage_level="covered",
            semantic_score=round(score, 4),
            confidence=min(0.98, 0.72 + 0.06 * len(overlap)),
            reason=REASON_COVERED,
        )
    if original_similarity >= 0.45:
        return PairAssessment(
            pair_id=pair.pair_id,
            requirement_id=pair.requirement_id,
            coverage_level="partial",
            semantic_score=min(0.79, max(0.55, original_similarity)),
            confidence=0.6,
            reason=REASON_PARTIAL,
        )
    return PairAssessment(
        pair_id=pair.pair_id,
        requirement_id=pair.requirement_id,
        coverage_level="missing",
        semantic_score=0.0,
        confidence=0.72,
        reason=REASON_MISSING,
    )


def _coverage_level_and_reason(semantic_score: float) -> tuple[str, str]:
    if semantic_score >= 0.80:
        return "covered", REASON_COVERED
    if semantic_score >= 0.55:
        return "partial", REASON_PARTIAL
    return "missing", REASON_MISSING


class LocalCrossEncoderProvider:
    route_name = "local"

    def __init__(
        self,
        model_name: str,
        *,
        cache_path: Optional[str] = None,
        runtime: str = "auto",
        max_batch_size: int = 32,
        trust_remote_code: bool = False,
    ):
        self.model_name = model_name
        self.cache_path = cache_path
        self.runtime = runtime
        self.max_batch_size = max(1, int(max_batch_size))
        self.trust_remote_code = trust_remote_code
        self._model: Any = None
        self._provider_id = "heuristic-local"
        self._effective_route_name = self.route_name

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def effective_route_name(self) -> str:
        return self._effective_route_name

    def _candidate_runtimes(self) -> List[str]:
        if self.runtime == "auto":
            return ["flag_embedding", "sentence_transformers", "heuristic"]
        if self.runtime == "heuristic":
            return ["heuristic"]
        return [self.runtime, "heuristic"]

    @staticmethod
    def _filter_supported_kwargs(factory: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        try:
            signature = inspect.signature(factory)
        except (TypeError, ValueError):
            return kwargs

        accepts_var_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        if accepts_var_kwargs:
            return kwargs
        supported = set(signature.parameters.keys())
        return {
            key: value
            for key, value in kwargs.items()
            if key in supported
        }

    @staticmethod
    def _normalize_runtime_item(item: Any) -> float:
        if isinstance(item, dict):
            if "score" not in item:
                raise TypeError(f"Unsupported local reranker item payload: {item!r}")
            return float(item["score"])
        if isinstance(item, (list, tuple)) and item:
            return float(item[0])
        return float(item)

    @staticmethod
    def _normalize_runtime_scores(result: Any) -> List[float]:
        if result is None:
            return []
        if isinstance(result, (int, float)):
            return [float(result)]
        if isinstance(result, dict):
            if "scores" in result:
                return LocalCrossEncoderProvider._normalize_runtime_scores(result["scores"])
            if "score" in result:
                return [float(result["score"])]
            raise TypeError(f"Unsupported local reranker result payload: {type(result)!r}")

        normalized: List[float] = []
        for item in result:
            normalized.append(LocalCrossEncoderProvider._normalize_runtime_item(item))
        return normalized

    def _load_flag_embedding_runtime(self) -> Any:
        module = importlib.import_module("FlagEmbedding")
        class_names = (
            "FlagAutoReranker",
            "FlagReranker",
            "LayerWiseFlagLLMReranker",
            "FlagLLMReranker",
        )
        last_error: Optional[Exception] = None
        for class_name in class_names:
            cls = getattr(module, class_name, None)
            if cls is None:
                continue
            try:
                return self._instantiate_flag_embedding_runtime(cls)
            except Exception as exc:  # noqa: BLE001 - keep trying supported runtime adapters
                last_error = exc
                logger.debug(
                    "FlagEmbedding runtime class %s was unavailable for model %s: %s",
                    class_name,
                    self.model_name,
                    exc,
                )
        raise RuntimeError(
            f"No compatible FlagEmbedding runtime could be initialized for {self.model_name}"
        ) from last_error

    def _instantiate_flag_embedding_runtime(self, cls: Any) -> Any:
        common_kwargs: Dict[str, Any] = {
            "use_fp16": False,
            "trust_remote_code": self.trust_remote_code,
        }
        if self.cache_path:
            common_kwargs["cache_dir"] = self.cache_path

        factory = getattr(cls, "from_finetuned", None)
        if callable(factory):
            filtered_kwargs = self._filter_supported_kwargs(factory, common_kwargs)
            model = factory(self.model_name, **filtered_kwargs)
        else:
            filtered_kwargs = self._filter_supported_kwargs(cls, common_kwargs)
            model = cls(self.model_name, **filtered_kwargs)

        self._provider_id = f"flag_embedding:{self.model_name}"
        self._effective_route_name = self.route_name
        return model

    def _load_sentence_transformers_runtime(self) -> Any:
        from sentence_transformers import CrossEncoder  # type: ignore

        model_kwargs: Dict[str, Any] = {}
        if self.cache_path:
            model_kwargs["cache_folder"] = self.cache_path
        model = CrossEncoder(self.model_name, **model_kwargs)
        self._provider_id = f"sentence_transformers:{self.model_name}"
        self._effective_route_name = self.route_name
        return model

    def _load_model(self):
        if self._model is not None:
            return self._model
        last_error: Optional[Exception] = None
        for runtime_name in self._candidate_runtimes():
            if runtime_name == "heuristic":
                self._provider_id = "heuristic-local"
                self._effective_route_name = "local_heuristic"
                self._model = False
                return self._model
            try:
                loader = getattr(self, f"_load_{runtime_name}_runtime")
            except AttributeError:
                logger.warning("Unknown local cross-encoder runtime '%s'; falling back.", runtime_name)
                continue
            try:
                self._model = loader()
                return self._model
            except Exception as exc:  # noqa: BLE001 - try the next supported local runtime
                last_error = exc
                logger.warning(
                    "Local semantic fit runtime %s unavailable for model %s; trying next runtime: %s",
                    runtime_name,
                    self.model_name,
                    exc,
                )
        self._provider_id = "heuristic-local"
        self._effective_route_name = "local_heuristic"
        self._model = False
        if last_error is not None:
            logger.warning(
                "Falling back to heuristic local semantic scoring after runtime initialization failures: %s",
                last_error,
            )
        return self._model

    def score_pairs(self, pairs: List[SerializedPair]) -> tuple[List[PairAssessment], Dict[str, Any]]:
        started_at = time.perf_counter()
        model = self._load_model()
        assessments: List[PairAssessment] = []
        if model is False:
            assessments = [_pair_assessment_from_heuristic(pair) for pair in pairs]
        else:
            left_right = [
                (
                    "\n".join(
                        [
                            f"Requirement: {pair.fields['requirement_text']}",
                            f"Requirement Type: {pair.fields['req_type']}",
                            f"Job Title: {pair.fields['job_title']}",
                            f"Company: {pair.fields['job_company']}",
                            f"Job Summary: {pair.fields['job_summary']}",
                        ]
                    ),
                    "\n".join(
                        [
                            f"Evidence: {pair.fields['evidence_text']}",
                            f"Evidence Section: {pair.fields['evidence_section']}",
                        ]
                    ),
                )
                for pair in pairs
            ]
            if hasattr(model, "compute_score"):
                compute_score = getattr(model, "compute_score")
                raw_scores = compute_score(
                    left_right,
                    **self._filter_supported_kwargs(
                        compute_score,
                        {"batch_size": self.max_batch_size},
                    ),
                )
            elif hasattr(model, "predict"):
                predict = getattr(model, "predict")
                raw_scores = predict(
                    left_right,
                    **self._filter_supported_kwargs(
                        predict,
                        {
                            "batch_size": self.max_batch_size,
                            "show_progress_bar": False,
                        },
                    ),
                )
            else:
                raise TypeError(
                    f"Unsupported local cross-encoder runtime for {self.model_name}: {type(model)!r}"
                )
            raw_scores = self._normalize_runtime_scores(raw_scores)
            for pair, raw_score in zip(pairs, raw_scores):
                semantic_score = _normalize_semantic_score(raw_score)
                coverage_level, reason = _coverage_level_and_reason(semantic_score)
                confidence = _clamp01(abs(semantic_score - 0.55) / 0.45)
                assessments.append(
                    PairAssessment(
                        pair_id=pair.pair_id,
                        requirement_id=pair.requirement_id,
                        coverage_level=coverage_level,
                        semantic_score=round(semantic_score, 4),
                        confidence=round(confidence, 4),
                        reason=reason,
                    )
                )
        diagnostics = {
            "provider_id": self.provider_id,
            "provider_route": self.effective_route_name,
            "latency_ms": round((time.perf_counter() - started_at) * 1000.0, 3),
        }
        return assessments, diagnostics

    def score_text_pairs(self, pairs: List[tuple[str, str]]) -> List[float]:
        """Score raw (left, right) string pairs and return one normalized [0, 1] score per pair."""
        if not pairs:
            return []
        model = self._load_model()
        if model is False:
            # Heuristic fallback when model load fails; warning already logged by _load_model().
            # Mirrors _pair_assessment_from_heuristic: 0.0 = no overlap, > 0 = overlap exists.
            scores = []
            for left, right in pairs:
                overlap = _meaningful_overlap(left, right)
                scores.append(min(0.85, 0.45 + 0.1 * len(overlap)) if overlap else 0.0)
            return scores
        if hasattr(model, "compute_score"):
            compute_score = getattr(model, "compute_score")
            raw_scores = compute_score(
                pairs,
                **self._filter_supported_kwargs(compute_score, {"batch_size": self.max_batch_size}),
            )
        elif hasattr(model, "predict"):
            predict = getattr(model, "predict")
            raw_scores = predict(
                pairs,
                **self._filter_supported_kwargs(
                    predict,
                    {"batch_size": self.max_batch_size, "show_progress_bar": False},
                ),
            )
        else:
            raise TypeError(
                f"Unsupported local cross-encoder runtime for {self.model_name}: {type(model)!r}"
            )
        raw_scores = self._normalize_runtime_scores(raw_scores)
        return [_normalize_semantic_score(s) for s in raw_scores]


class RemoteCrossEncoderProvider:
    route_name = "remote"

    def __init__(self, *, base_url: str, api_key: Optional[str], model: str, timeout_ms: int):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout_ms = timeout_ms

    @property
    def provider_id(self) -> str:
        return self.model

    def score_pairs(self, pairs: List[SerializedPair]) -> tuple[List[PairAssessment], Dict[str, Any]]:
        started_at = time.perf_counter()
        response = requests.post(
            f"{self.base_url}/v1/fit/score",
            headers={
                **({"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}),
                "Content-Type": "application/json",
            },
            json={
                "model": self.model,
                "pairs": [
                    {
                        "pair_id": pair.pair_id,
                        "requirement_id": pair.requirement_id,
                        "requirement_text": pair.fields["requirement_text"],
                        "req_type": pair.fields["req_type"],
                        "evidence_text": pair.fields["evidence_text"],
                        "evidence_section": pair.fields["evidence_section"],
                        "original_similarity": float(pair.candidate.similarity or 0.0),
                        "job_title": pair.fields["job_title"],
                        "job_company": pair.fields["job_company"],
                        "job_summary": pair.fields["job_summary"],
                    }
                    for pair in pairs
                ],
                "timeout_ms": self.timeout_ms,
            },
            timeout=max(1, self.timeout_ms / 1000.0),
        )
        response.raise_for_status()
        payload = response.json()
        scores_by_pair = {item["pair_id"]: float(item["raw_logit"]) for item in payload.get("scores", [])}
        assessments: List[PairAssessment] = []
        for pair in pairs:
            if pair.pair_id not in scores_by_pair:
                assessments.append(_pair_assessment_from_heuristic(pair))
                continue
            semantic_score = _normalize_semantic_score(scores_by_pair[pair.pair_id])
            coverage_level, reason = _coverage_level_and_reason(semantic_score)
            assessments.append(
                PairAssessment(
                    pair_id=pair.pair_id,
                    requirement_id=pair.requirement_id,
                    coverage_level=coverage_level,
                    semantic_score=round(semantic_score, 4),
                    confidence=round(_clamp01(abs(semantic_score - 0.55) / 0.45), 4),
                    reason=reason,
                )
            )
        diagnostics = {
            "provider_id": self.provider_id,
            "provider_route": self.route_name,
            "latency_ms": round((time.perf_counter() - started_at) * 1000.0, 3),
        }
        return assessments, diagnostics


class LLMSemanticFitScorer:
    scorer_name = LLM_SCORER_NAME
    scorer_version = LLM_SCORER_VERSION

    def __init__(self, ai_service: LLMProvider, *, fallback_scorer: Optional[ThresholdSemanticFitScorer] = None):
        self.ai_service = ai_service
        self.fallback_scorer = fallback_scorer or ThresholdSemanticFitScorer()

    def score(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
        owner_id: Any | None = None,
    ) -> SemanticFitScoreResult:
        del owner_id
        if not getattr(config.semantic_fit, "enabled", True):
            return self.fallback_scorer.score(preliminary, fit_penalties=fit_penalties, config=config)

        try:
            return self._score_with_llm(preliminary, fit_penalties=fit_penalties, config=config)
        except Exception as exc:
            if not getattr(config.semantic_fit, "threshold_fallback_enabled", True):
                raise
            logger.warning("Semantic fit scoring failed; falling back to threshold scorer: %s", exc, exc_info=True)
            return _build_threshold_result(
                preliminary,
                fit_penalties=fit_penalties,
                config=config,
                scorer_name=THRESHOLD_SCORER_NAME,
                scorer_version=THRESHOLD_SCORER_VERSION,
                fallback_used=True,
                fallback_reason=str(exc),
                fallback_message=THRESHOLD_FALLBACK_MESSAGE,
            )

    def _score_with_llm(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
    ) -> SemanticFitScoreResult:
        started_at = time.perf_counter()
        threshold = float(
            getattr(config, "req_similarity_threshold", fit_score.DEFAULT_REQ_SIMILARITY_THRESHOLD)
        )
        serialized_pairs, zero_evidence_verdicts, truncation_aggregate = _build_serialized_pairs(preliminary, config=config)
        payload = {
            "job_id": str(getattr(preliminary.job, "id", "")),
            "job_title": _string_or_empty(getattr(preliminary.job, "title", "")),
            "job_company": _string_or_empty(getattr(preliminary.job, "company", "")),
            "job_summary": _string_or_empty(getattr(preliminary.job, "canonical_job_summary", ""))
            or _string_or_empty(getattr(preliminary.job, "description", "")),
            "pairs": [
                {
                    "pair_id": pair.pair_id,
                    "requirement_id": pair.requirement_id,
                    **pair.fields,
                    "original_similarity": float(pair.candidate.similarity or 0.0),
                }
                for pair in serialized_pairs
            ],
        }
        if serialized_pairs:
            assessment = self.ai_service.extract_structured_data(
                text=json.dumps(payload),
                schema_spec=SEMANTIC_FIT_SCHEMA_SPEC,
                system_prompt=SEMANTIC_FIT_SYSTEM_PROMPT,
                user_message=(
                    "Evaluate the semantic fit between each job requirement and the paired resume evidence.\n\n"
                    f"{json.dumps(payload)}"
                ),
            )
            parsed = SemanticPairAssessmentResponse.model_validate(assessment)
            assessments = [
                PairAssessment(
                    pair_id=item.pair_id,
                    requirement_id=item.requirement_id,
                    coverage_level=item.coverage_level,
                    semantic_score=item.semantic_score,
                    confidence=item.confidence,
                    reason=item.reason,
                )
                for item in parsed.pair_judgments
            ]
            summary = parsed.summary
        else:
            assessments = []
            summary = "No recalled resume evidence supported the evaluated requirements."
        return _score_with_assessments(
            preliminary,
            assessments=assessments,
            serialized_pairs=serialized_pairs,
            zero_evidence_verdicts=zero_evidence_verdicts,
            fit_penalties=fit_penalties,
            config=config,
            metadata=AssessmentScoringMetadata(
                scorer_name=self.scorer_name,
                scorer_version=self.scorer_version,
                provider_route="llm",
                provider_id="llm",
                threshold=threshold,
                latency_ms=round((time.perf_counter() - started_at) * 1000.0, 3),
                model_summary=summary,
                truncation_aggregate=truncation_aggregate,
            ),
        )


def _build_serialized_pairs(
    preliminary: JobMatchPreliminary,
    *,
    config: ScorerConfig,
) -> tuple[List[SerializedPair], List[Dict[str, Any]], Dict[str, Any]]:
    threshold = float(
        getattr(config, "req_similarity_threshold", fit_score.DEFAULT_REQ_SIMILARITY_THRESHOLD)
    )
    pairs: List[SerializedPair] = []
    zero_evidence_verdicts: List[Dict[str, Any]] = []
    for requirement_match in preliminary.requirement_matches + preliminary.missing_requirements:
        candidates = _candidate_evidence_candidates(requirement_match)
        if not candidates:
            zero_evidence_verdicts.append(_zero_evidence_verdict(requirement_match, threshold))
            continue
        for candidate in candidates:
            pair = _serialize_pair(preliminary, requirement_match, candidate, config=config)
            pairs.append(pair)
    aggregate = _truncation_aggregate(pairs)
    _log_truncation_aggregate(aggregate)
    return pairs, zero_evidence_verdicts, aggregate


def _candidate_evidence_candidates(requirement_match: RequirementMatchResult) -> List[RequirementEvidenceCandidate]:
    candidates = list(requirement_match.evidence_candidates)
    if candidates or requirement_match.evidence is None:
        return candidates
    return [
        RequirementEvidenceCandidate(
            evidence=requirement_match.evidence,
            similarity=float(requirement_match.similarity or 0.0),
            rank=1,
        )
    ]


def _zero_evidence_verdict(requirement_match: RequirementMatchResult, threshold: float) -> Dict[str, Any]:
    preserved_coverage = _fallback_coverage_level(requirement_match)
    preserved_score = _semantic_similarity(
        float(requirement_match.similarity or 0.0),
        preserved_coverage,
        threshold,
    )
    return _base_verdict(
        requirement_match,
        evidence=None,
        coverage_level=preserved_coverage,
        semantic_score=preserved_score,
        confidence=1.0 if preserved_coverage == "missing" else 0.0,
        reason=REASON_NO_EVIDENCE if preserved_coverage == "missing" else REASON_PRESERVED_MATCHER,
    )


def _truncation_aggregate(pairs: List[SerializedPair]) -> Dict[str, Any]:
    truncated_pair_count = sum(1 for pair in pairs if pair.truncation["truncated"])
    total_truncated_chars = sum(int(pair.truncation["total_truncated_chars"]) for pair in pairs)
    emergency_ceiling_hits = sum(1 for pair in pairs if pair.truncation["emergency_ceiling_hit"])
    return {
        "any_truncated": truncated_pair_count > 0,
        "pair_count": len(pairs),
        "truncated_pair_count": truncated_pair_count,
        "total_truncated_chars": total_truncated_chars,
        "emergency_ceiling_hits": emergency_ceiling_hits,
    }


def _log_truncation_aggregate(aggregate: Dict[str, Any]) -> None:
    if not aggregate["any_truncated"]:
        return
    logger.warning(
        "Semantic fit serialization truncated %d/%d pairs (%d chars discarded, emergency ceiling hits=%d)",
        aggregate["truncated_pair_count"],
        aggregate["pair_count"],
        aggregate["total_truncated_chars"],
        aggregate["emergency_ceiling_hits"],
    )


def _select_best_assessment(
    candidate_pairs: List[SerializedPair],
    assessments_by_pair: Dict[str, PairAssessment],
) -> tuple[Optional[SerializedPair], Optional[PairAssessment]]:
    best_pair: Optional[SerializedPair] = None
    best_assessment: Optional[PairAssessment] = None
    for pair in candidate_pairs:
        assessment = assessments_by_pair.get(pair.pair_id)
        if assessment is None:
            continue
        if best_assessment is None:
            best_pair = pair
            best_assessment = assessment
            continue
        candidate_key = (
            assessment.semantic_score,
            assessment.confidence,
            float(pair.candidate.similarity or 0.0),
        )
        best_key = (
            best_assessment.semantic_score,
            best_assessment.confidence,
            float(best_pair.candidate.similarity or 0.0),
        )
        if candidate_key > best_key:
            best_pair = pair
            best_assessment = assessment
    return best_pair, best_assessment


def _score_with_assessments(
    preliminary: JobMatchPreliminary,
    *,
    assessments: List[PairAssessment],
    serialized_pairs: List[SerializedPair],
    zero_evidence_verdicts: List[Dict[str, Any]],
    fit_penalties: float,
    config: ScorerConfig,
    metadata: AssessmentScoringMetadata,
) -> SemanticFitScoreResult:
    assessments_by_pair = {assessment.pair_id: assessment for assessment in assessments}
    pairs_by_requirement = _pairs_by_requirement(serialized_pairs)
    adjusted_matched: List[RequirementMatchResult] = []
    adjusted_missing: List[RequirementMatchResult] = []
    verdicts: List[Dict[str, Any]] = list(zero_evidence_verdicts)

    for requirement_match in preliminary.requirement_matches + preliminary.missing_requirements:
        adjusted, verdict = _score_requirement_match(
            requirement_match=requirement_match,
            candidate_pairs=pairs_by_requirement.get(_requirement_id(requirement_match), []),
            assessments_by_pair=assessments_by_pair,
            threshold=metadata.threshold,
            provider_route=metadata.provider_route,
        )
        if adjusted.is_covered:
            adjusted_matched.append(adjusted)
        else:
            adjusted_missing.append(adjusted)
        if verdict is not None:
            verdicts.append(verdict)

    fit_value, fit_components = fit_score.calculate_fit_score(
        job_similarity=preliminary.job_similarity,
        matched_requirements=adjusted_matched,
        missing_requirements=adjusted_missing,
        fit_penalties=fit_penalties,
        config=config,
    )
    required_coverage = float(fit_components.get("required_coverage", 0.0))
    preferred_requirement_coverage = float(
        fit_components.get("preferred_requirement_coverage", 0.0)
    )
    confidence_values = [verdict["confidence"] for verdict in verdicts if verdict["confidence"] > 0]
    base_confidence = (sum(confidence_values) / len(confidence_values)) if confidence_values else 0.0
    fit_confidence = round(max(base_confidence, _fit_confidence(required_coverage, preliminary.job_similarity)), 4)
    retrieval_diagnostics = _build_retrieval_diagnostics(preliminary)
    scorer_diagnostics = {
        "name": metadata.scorer_name,
        "version": metadata.scorer_version,
        "effective_fit_mode": _effective_fit_mode(metadata.provider_route, metadata.provider_id),
        "provider_route": metadata.provider_route,
        "provider_id": metadata.provider_id,
        "latency_ms": metadata.latency_ms,
        "fallback_used": metadata.fallback_used,
        "judged_requirements": len(verdicts),
        "truncation": metadata.truncation_aggregate,
    }
    if metadata.fallback_reason:
        scorer_diagnostics["fallback_reason"] = metadata.fallback_reason
    fit_explanation = _build_fit_explanation(
        verdicts,
        required_coverage=required_coverage,
        preferred_requirement_coverage=preferred_requirement_coverage,
        fit_confidence=fit_confidence,
        job_similarity=preliminary.job_similarity,
        scorer_name=metadata.scorer_name,
        scorer_version=metadata.scorer_version,
        retrieval_diagnostics=retrieval_diagnostics,
        scorer_diagnostics=scorer_diagnostics,
        fallback_message=THRESHOLD_FALLBACK_MESSAGE if metadata.fallback_used else None,
    )
    enriched_components = dict(fit_components)
    enriched_components["fit_confidence"] = fit_confidence
    enriched_components["fit_scorer"] = {"name": metadata.scorer_name, "version": metadata.scorer_version}
    enriched_components["effective_fit_mode"] = scorer_diagnostics["effective_fit_mode"]
    enriched_components["provider_route"] = metadata.provider_route
    enriched_components["retrieval"] = retrieval_diagnostics
    enriched_components["semantic_fit_diagnostics"] = scorer_diagnostics
    enriched_components["semantic_fit_truncation"] = metadata.truncation_aggregate
    if metadata.fallback_reason:
        enriched_components["semantic_fit_fallback_reason"] = metadata.fallback_reason
    if metadata.model_summary:
        enriched_components["semantic_fit_summary"] = metadata.model_summary
    enriched_components["fit_explanation"] = fit_explanation
    return SemanticFitScoreResult(
        fit_score=fit_value,
        fit_components=enriched_components,
        fit_confidence=fit_confidence,
        fit_explanation=fit_explanation,
        scorer_name=metadata.scorer_name,
        scorer_version=metadata.scorer_version,
        matched_requirements=adjusted_matched,
        missing_requirements=adjusted_missing,
    )


def _pairs_by_requirement(serialized_pairs: List[SerializedPair]) -> Dict[str, List[SerializedPair]]:
    pairs_by_requirement: Dict[str, List[SerializedPair]] = {}
    for pair in serialized_pairs:
        pairs_by_requirement.setdefault(pair.requirement_id, []).append(pair)
    return pairs_by_requirement


def _fallback_adjusted_match(requirement_match: RequirementMatchResult, threshold: float) -> RequirementMatchResult:
    preserved_coverage = _fallback_coverage_level(requirement_match)
    return _clone_match(
        requirement_match,
        evidence=requirement_match.evidence,
        similarity=_semantic_similarity(
            float(requirement_match.similarity or 0.0),
            preserved_coverage,
            threshold,
        ),
        is_covered=preserved_coverage == "covered",
    )


def _missing_assessment_verdict(
    requirement_match: RequirementMatchResult,
    provider_route: str,
) -> Dict[str, Any]:
    return _base_verdict(
        requirement_match,
        evidence=requirement_match.evidence,
        coverage_level="missing",
        semantic_score=0.0,
        confidence=0.0,
        reason=REASON_UNJUDGED,
        provider_route=provider_route,
    )


def _scored_requirement_verdict(
    requirement_match: RequirementMatchResult,
    best_pair: SerializedPair,
    best_assessment: PairAssessment,
    provider_route: str,
) -> Dict[str, Any]:
    return _base_verdict(
        requirement_match,
        evidence=best_pair.candidate.evidence,
        coverage_level=best_assessment.coverage_level,
        semantic_score=round(_clamp01(best_assessment.semantic_score), 4),
        confidence=round(_clamp01(best_assessment.confidence), 4),
        reason=best_assessment.reason,
        provider_route=provider_route,
        truncation=best_pair.truncation,
    )


def _score_requirement_match(
    *,
    requirement_match: RequirementMatchResult,
    candidate_pairs: List[SerializedPair],
    assessments_by_pair: Dict[str, PairAssessment],
    threshold: float,
    provider_route: str,
) -> tuple[RequirementMatchResult, Optional[Dict[str, Any]]]:
    if not candidate_pairs:
        return _fallback_adjusted_match(requirement_match, threshold), None

    best_pair, best_assessment = _select_best_assessment(candidate_pairs, assessments_by_pair)
    if best_pair is None or best_assessment is None:
        adjusted = _clone_match(
            requirement_match,
            evidence=requirement_match.evidence,
            similarity=0.0,
            is_covered=False,
        )
        return adjusted, _missing_assessment_verdict(requirement_match, provider_route)

    similarity = _semantic_similarity(best_assessment.semantic_score, best_assessment.coverage_level, threshold)
    adjusted = _clone_match(
        requirement_match,
        evidence=best_pair.candidate.evidence,
        similarity=similarity,
        is_covered=best_assessment.coverage_level == "covered",
    )
    return adjusted, _scored_requirement_verdict(
        requirement_match,
        best_pair,
        best_assessment,
        provider_route,
    )


def _effective_fit_mode(provider_route: str, provider_id: str) -> str:
    if provider_route in {"local_heuristic", "threshold"} or provider_id == "heuristic-local":
        return "threshold"
    return "llm" if provider_route == "llm" else "cross_encoder"


class CrossEncoderSemanticFitScorer:
    scorer_name = CROSS_ENCODER_SCORER_NAME
    scorer_version = CROSS_ENCODER_SCORER_VERSION

    def __init__(
        self,
        *,
        local_provider: Optional[LocalCrossEncoderProvider],
        remote_provider: Optional[RemoteCrossEncoderProvider],
        fallback_scorer: Optional[ThresholdSemanticFitScorer] = None,
    ):
        self.local_provider = local_provider
        self.remote_provider = remote_provider
        self.fallback_scorer = fallback_scorer or ThresholdSemanticFitScorer()

    @staticmethod
    def _cross_encoder_threshold(config: ScorerConfig) -> float:
        return float(
            getattr(config, "req_similarity_threshold", fit_score.DEFAULT_REQ_SIMILARITY_THRESHOLD)
        )

    @staticmethod
    def _is_production_environment() -> bool:
        environment = (
            os.getenv("JOBSCOUT_ENV")
            or os.getenv("APP_ENV")
            or os.getenv("ENVIRONMENT")
            or "development"
        )
        return environment.strip().lower() == "production"

    @staticmethod
    def _available_providers(*providers: Any) -> List[Any]:
        return [provider for provider in providers if provider is not None]

    def _providers_for_route(
        self,
        *,
        route_policy: str,
        pair_count: int,
        config: ScorerConfig,
    ) -> tuple[List[Any], Optional[Exception]]:
        if route_policy == "remote":
            if self.remote_provider is not None:
                return [self.remote_provider], None
            return [], RuntimeError("Remote cross-encoder route requested but no remote provider is configured")

        if route_policy == "auto":
            promote_threshold = int(getattr(config.semantic_fit.cross_encoder, "remote_promote_pair_count", 40))
            prefer_remote = (
                self.remote_provider is not None
                and self._is_production_environment()
                and pair_count > promote_threshold
            )
            if prefer_remote:
                return self._available_providers(self.remote_provider, self.local_provider), None
            return self._available_providers(self.local_provider, self.remote_provider), None

        providers: List[Any] = []
        last_error: Optional[Exception] = None
        if self.local_provider is not None:
            providers.append(self.local_provider)
        else:
            last_error = RuntimeError("Local cross-encoder route requested but local provider is disabled")
        return providers, last_error

    def _fallback_result(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
        last_error: Optional[Exception],
    ) -> SemanticFitScoreResult:
        if not getattr(config.semantic_fit, "threshold_fallback_enabled", True):
            if last_error:
                raise last_error
            raise RuntimeError("No cross-encoder provider was available")

        return _build_threshold_result(
            preliminary,
            fit_penalties=fit_penalties,
            config=config,
            scorer_name=THRESHOLD_SCORER_NAME,
            scorer_version=THRESHOLD_SCORER_VERSION,
            fallback_used=True,
            fallback_reason=str(last_error) if last_error else "no_provider_available",
            fallback_message=THRESHOLD_FALLBACK_MESSAGE,
        )

    def score(
        self,
        preliminary: JobMatchPreliminary,
        *,
        fit_penalties: float,
        config: ScorerConfig,
        owner_id: Any | None = None,
    ) -> SemanticFitScoreResult:
        del owner_id
        if not getattr(config.semantic_fit, "enabled", True):
            return self.fallback_scorer.score(preliminary, fit_penalties=fit_penalties, config=config)

        threshold = self._cross_encoder_threshold(config)
        serialized_pairs, zero_evidence_verdicts, truncation_aggregate = _build_serialized_pairs(preliminary, config=config)
        pair_count = len(serialized_pairs)
        route_policy = getattr(config.semantic_fit.cross_encoder, "route_policy", "local")
        providers, last_error = self._providers_for_route(
            route_policy=route_policy,
            pair_count=pair_count,
            config=config,
        )

        for provider in providers:
            try:
                assessments, provider_diagnostics = provider.score_pairs(serialized_pairs)
                return _score_with_assessments(
                    preliminary,
                    assessments=assessments,
                    serialized_pairs=serialized_pairs,
                    zero_evidence_verdicts=zero_evidence_verdicts,
                    fit_penalties=fit_penalties,
                    config=config,
                    metadata=AssessmentScoringMetadata(
                        scorer_name=self.scorer_name,
                        scorer_version=self.scorer_version,
                        provider_route=provider_diagnostics.get("provider_route", provider.route_name),
                        provider_id=provider_diagnostics["provider_id"],
                        threshold=threshold,
                        latency_ms=provider_diagnostics["latency_ms"],
                        model_summary=None,
                        truncation_aggregate=truncation_aggregate,
                    ),
                )
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Cross-encoder provider route %s failed; trying next route if available: %s",
                    getattr(provider, "route_name", "unknown"),
                    exc,
                    exc_info=True,
                )

        return self._fallback_result(
            preliminary,
            fit_penalties=fit_penalties,
            config=config,
            last_error=last_error,
        )


def _normalize_modes(raw_modes: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    for mode in raw_modes:
        if mode in {"cross_encoder", "llm"} and mode not in normalized:
            normalized.append(mode)
    return normalized


def _default_effective_allowed(semantic_fit: SemanticFitConfig) -> List[str]:
    deploy_allowed = _normalize_modes(getattr(semantic_fit, "deploy_allowed_modes", []))
    baseline_allowed = _normalize_modes(getattr(semantic_fit, "baseline_allowed_modes", []))
    if not baseline_allowed:
        baseline_allowed = [semantic_fit.default_mode]
    effective_allowed = [mode for mode in baseline_allowed if mode in deploy_allowed]
    if effective_allowed:
        return effective_allowed
    return [mode for mode in ["cross_encoder", "llm"] if mode in deploy_allowed] or ["cross_encoder"]


def _capability_allowed_modes(repo: Any, owner_id: Any, deploy_allowed: List[str]) -> List[str]:
    allowed_row = repo.get_capability(owner_id, FEATURE_ALLOWED_MODES)
    if not allowed_row or not getattr(allowed_row, "enabled", True):
        return []

    value_json = getattr(allowed_row, "value_json", None) or {}
    if not isinstance(value_json, dict):
        logger.warning("Ignoring invalid capability payload for %s", FEATURE_ALLOWED_MODES)
        return []

    capability_modes = _normalize_modes(value_json.get("modes", []))
    return [mode for mode in capability_modes if mode in deploy_allowed]


def _preferred_capability_mode(repo: Any, owner_id: Any, effective_allowed: List[str]) -> Optional[str]:
    preferred_row = repo.get_capability(owner_id, FEATURE_PREFERRED_MODE)
    if not preferred_row or not getattr(preferred_row, "enabled", True):
        return None

    value_json = getattr(preferred_row, "value_json", None) or {}
    if not isinstance(value_json, dict):
        logger.warning("Ignoring invalid capability payload for %s", FEATURE_PREFERRED_MODE)
        return None

    preferred_mode = value_json.get("mode")
    return preferred_mode if preferred_mode in effective_allowed else None


def resolve_effective_fit_mode(repo, config: ScorerConfig, owner_id: Any) -> tuple[str, List[str]]:
    semantic_fit = config.semantic_fit
    deploy_allowed = _normalize_modes(getattr(semantic_fit, "deploy_allowed_modes", []))
    effective_allowed = _default_effective_allowed(semantic_fit)

    if owner_id:
        capability_allowed = _capability_allowed_modes(repo, owner_id, deploy_allowed)
        if capability_allowed:
            effective_allowed = capability_allowed

    resolved_mode = semantic_fit.default_mode if semantic_fit.default_mode in effective_allowed else effective_allowed[0]
    if owner_id:
        preferred_mode = _preferred_capability_mode(repo, owner_id, effective_allowed)
        if preferred_mode:
            resolved_mode = preferred_mode
    return resolved_mode, effective_allowed
