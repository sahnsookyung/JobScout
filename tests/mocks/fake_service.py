"""
Deterministic fake LLM service for integration and end-to-end tests.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
from typing import Any, Dict, List, Optional

from core.llm.interfaces import LLMProvider

logger = logging.getLogger(__name__)

EMBEDDING_DIMENSIONS = 1024
FAIL_EXTRACTION_MARKER = "FAIL_EXTRACTION"
FAIL_EMBEDDING_MARKER = "FAIL_EMBEDDING"

_KEYWORD_DIMENSIONS = {
    "python": 0,
    "fastapi": 1,
    "aws": 2,
    "docker": 3,
    "microservices": 4,
    "kubernetes": 5,
    "postgresql": 6,
    "redis": 7,
    "java": 8,
    "spring": 9,
    "salesforce": 10,
    "react": 11,
    "remote": 12,
    "backend": 13,
    "api": 14,
}

_GENERIC_TOKENS = {
    "experience", "years", "year", "developer", "development", "building", "build",
    "engineer", "engineering", "services", "service", "knowledge", "skill", "skills",
    "required", "preferred", "with", "using", "plus", "have", "hands", "on",
}

# Mirrors PREFERENCE_PROFILE_VERSION in services/scorer_matcher/preference_semantics.py.
# Keep in sync when the real parser bumps its version string.
_FAKE_PREFERENCE_PROFILE_VERSION = "2026-04-01.v1"

# Exact single-token matching only — multi-word phrases like "React Native" won't match.
# Extend when test fixtures need additional signals; don't rely on this for coverage of
# the real parser's full vocabulary.
_PREFERENCE_SIGNALS = {
    "remote": ("work_style", "Remote-friendly"),
    "hybrid": ("work_style", "Hybrid collaboration"),
    "mentorship": ("team_culture", "Mentorship"),
    "backend": ("tech_stack", "Backend systems"),
    "python": ("tech_stack", "Python"),
    "fastapi": ("tech_stack", "FastAPI"),
    "microservices": ("tech_stack", "Microservices"),
    "product": ("mission_domain", "Product-minded"),
    "mission": ("mission_domain", "Mission-driven"),
    "climate": ("mission_domain", "Climate"),
    "growth": ("growth_preferences", "Growth opportunities"),
    "modern": ("team_culture", "Modern engineering"),
    "teams": ("team_culture", "Collaborative teams"),
}


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9_+-]+", text.lower())


def _unit_normalize(vector: List[float]) -> List[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        if vector:
            vector[-1] = 1.0
        return vector
    return [value / norm for value in vector]


def _meaningful_overlap(requirement_text: str, evidence_text: str) -> set[str]:
    req_tokens = {token for token in _tokenize(requirement_text) if token not in _GENERIC_TOKENS}
    evidence_tokens = {token for token in _tokenize(evidence_text) if token not in _GENERIC_TOKENS}
    return req_tokens & evidence_tokens


def _fake_semantic_fit_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    judgments: List[Dict[str, Any]] = []
    for pair in payload.get("pairs", []):
        requirement_text = pair.get("requirement_text", "")
        evidence_text = pair.get("evidence_text", "")
        original_similarity = float(pair.get("original_similarity", 0.0) or 0.0)
        overlap = _meaningful_overlap(requirement_text, evidence_text)
        req_keywords = set(_tokenize(requirement_text)) & set(_KEYWORD_DIMENSIONS.keys())
        evidence_keywords = set(_tokenize(evidence_text)) & set(_KEYWORD_DIMENSIONS.keys())
        explicit_keyword_mismatch = bool(req_keywords and evidence_keywords and not (req_keywords & evidence_keywords))

        if not evidence_text:
            coverage = "missing"
            semantic_score = 0.0
            confidence = 0.4
            reason = "No matching resume evidence was available for this requirement."
        elif explicit_keyword_mismatch:
            coverage = "missing"
            semantic_score = 0.0
            confidence = 0.85
            reason = "Evidence references different technologies than the requirement."
        elif overlap:
            coverage = "covered"
            semantic_score = min(0.95, 0.72 + 0.12 * len(overlap))
            confidence = min(0.95, 0.7 + 0.1 * len(overlap))
            reason = "Evidence mentions the core requirement directly."
        elif original_similarity >= 0.45:
            coverage = "partial"
            semantic_score = 0.35
            confidence = 0.55
            reason = "Evidence is related but does not clearly satisfy the requirement."
        else:
            coverage = "missing"
            semantic_score = 0.0
            confidence = 0.65
            reason = "Evidence does not support the specific requirement."

        judgments.append(
            {
                "pair_id": pair.get("pair_id", ""),
                "requirement_id": pair.get("requirement_id", ""),
                "coverage_level": coverage,
                "semantic_score": semantic_score,
                "confidence": confidence,
                "reason": reason,
            }
        )

    required_total = sum(1 for judgment in judgments if any(
        req.get("requirement_id") == judgment["requirement_id"] and req.get("req_type") == "required"
        for req in payload.get("pairs", [])
    ))
    required_covered = sum(
        1
        for judgment in judgments
        if judgment["coverage_level"] == "covered" and any(
            req.get("requirement_id") == judgment["requirement_id"] and req.get("req_type") == "required"
            for req in payload.get("pairs", [])
        )
    )
    summary = f"Covered {required_covered} of {required_total} required requirements."
    return {
        "summary": summary,
        "pair_judgments": judgments,
    }


def _fake_preference_profile_response(text: str) -> Dict[str, Any]:
    normalized = " ".join(text.split())
    lowered_tokens = _tokenize(normalized)
    grouped: Dict[str, List[Dict[str, Any]]] = {
        "work_style": [],
        "team_culture": [],
        "tech_stack": [],
        "mission_domain": [],
        "growth_preferences": [],
        "negative_preferences": [],
    }
    seen: set[tuple[str, str]] = set()
    for token in lowered_tokens:
        signal = _PREFERENCE_SIGNALS.get(token)
        if not signal:
            continue
        field_name, label = signal
        key = (field_name, label.lower())
        if key in seen:
            continue
        seen.add(key)
        grouped[field_name].append(
            {
                "label": label,
                "weight": 0.8 if field_name != "growth_preferences" else 0.7,
                "confidence": 0.85,
            }
        )

    negative_phrases = []
    lowered = normalized.lower()
    for phrase in ("avoid consulting", "avoid salesforce", "no startup chaos"):
        if phrase in lowered:
            negative_phrases.append(
                {
                    "label": phrase,
                    "weight": 0.8,
                    "confidence": 0.8,
                }
            )
    grouped["negative_preferences"] = negative_phrases

    return {
        "raw_text": normalized,
        "parse_version": _FAKE_PREFERENCE_PROFILE_VERSION,
        "parser_confidence": 0.82 if seen or negative_phrases else 0.45,
        **grouped,
    }


def _preference_label_tokens(label: str) -> set[str]:
    return {
        token
        for token in _tokenize(label)
        if token not in _GENERIC_TOKENS and len(token) >= 4
    }


def _job_preference_haystack(job_payload: Dict[str, Any]) -> str:
    return " ".join(
        [
            str(job_payload.get("title", "")),
            str(job_payload.get("company", "")),
            str(job_payload.get("location_text", "")),
            str(job_payload.get("work_mode", "")),
            str(job_payload.get("employment_type", "")),
            str(job_payload.get("summary", "")),
            str(job_payload.get("company_description", "")),
            " ".join(str(skill) for skill in (job_payload.get("skills") or [])),
            " ".join(str(requirement) for requirement in (job_payload.get("requirements") or [])),
            " ".join(str(benefit) for benefit in (job_payload.get("benefits") or [])),
        ]
    ).lower()


def _score_job_against_profile(
    job: Dict[str, Any],
    profile: Dict[str, Any],
    positive_fields: tuple,
) -> tuple:
    """Return (positive_score, possible_score, negative_score, reason_codes, matched_labels)."""
    haystack = _job_preference_haystack(job)
    possible_score = 0.0
    positive_score = 0.0
    negative_score = 0.0
    reason_codes: List[str] = []
    matched_labels: List[str] = []

    for field_name in positive_fields:
        for item in profile.get(field_name, []) or []:
            item_weight = float(item.get("weight", 0.0) or 0.0) * float(
                item.get("confidence", 0.0) or 0.0
            )
            possible_score += item_weight
            label = str(item.get("label", ""))
            label_tokens = _preference_label_tokens(label)
            if label_tokens and any(token in haystack for token in label_tokens):
                positive_score += item_weight
                matched_labels.append(label)
                reason_codes.append(f"{field_name}_match")

    for item in profile.get("negative_preferences", []) or []:
        label = str(item.get("label", ""))
        label_tokens = _preference_label_tokens(label)
        if label_tokens and any(token in haystack for token in label_tokens):
            negative_score += float(item.get("weight", 0.0) or 0.0) * float(
                item.get("confidence", 0.0) or 0.0
            )
            reason_codes.append("negative_preference_match")

    return positive_score, possible_score, negative_score, reason_codes, matched_labels


def _fake_preference_rerank_response(payload: Dict[str, Any], *, judge_mode: bool) -> Dict[str, Any]:
    profile = payload.get("profile", {}) or {}
    jobs = payload.get("jobs", []) or []
    results: List[Dict[str, Any]] = []
    positive_fields = (
        "work_style",
        "team_culture",
        "tech_stack",
        "mission_domain",
        "growth_preferences",
    )
    for job in jobs:
        positive_score, possible_score, negative_score, reason_codes, matched_labels = (
            _score_job_against_profile(job, profile, positive_fields)
        )

        base_score = positive_score / max(possible_score, 1.0)
        score = max(0.0, min(1.0, base_score - min(0.75, negative_score)))
        confidence = max(0.3, min(0.95, 0.45 + (0.12 * len(set(reason_codes)))))

        if matched_labels:
            explanation = (
                "Matches preferences for " + ", ".join(matched_labels[:3]) + "."
            )
        elif negative_score > 0:
            explanation = "Conflicts with at least one saved negative preference."
        else:
            explanation = "The job description does not strongly reflect the saved preferences."

        if judge_mode and score > 0:
            score = min(1.0, score + 0.05)
            confidence = min(0.98, confidence + 0.05)

        results.append(
            {
                "job_id": str(job.get("job_id", "")),
                "preference_score": round(score, 4),
                "preference_confidence": round(confidence, 4),
                "preference_reason_codes": sorted(set(reason_codes))[:4] or ["no_preference_signal"],
                "preference_explanation": explanation,
            }
        )

    return {"results": results}


class FakeLLMService(LLMProvider):
    """Deterministic fake provider that never calls external services."""

    def __init__(self, embedding_dimensions: int = EMBEDDING_DIMENSIONS):
        self.embedding_dimensions = embedding_dimensions
        self.extraction_model = "fake-extraction"
        self.embedding_model = "fake-embedding"

    @staticmethod
    def _global_failure_mode() -> str:
        return os.getenv("JOBSCOUT_FAKE_AI_FAILURE_MODE", "").strip().lower()

    def _maybe_fail_extraction(self, text: str) -> None:
        failure_mode = self._global_failure_mode()
        if failure_mode == "extraction" or FAIL_EXTRACTION_MARKER.lower() in text.lower():
            raise ValueError("Fake extraction failure")

    def _maybe_fail_embedding(self, text: str) -> None:
        failure_mode = self._global_failure_mode()
        if failure_mode == "embedding" or FAIL_EMBEDDING_MARKER.lower() in text.lower():
            raise ValueError("Fake embedding failure")

    def extract_structured_data(
        self,
        text: str,
        schema_spec: Dict,
        system_prompt: Optional[str] = None,
        user_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        del system_prompt, user_message
        self._maybe_fail_extraction(text)
        schema_name = schema_spec.get("name") if isinstance(schema_spec, dict) else None
        if schema_name == "semantic_fit_pairs_v1":
            parsed_payload = json.loads(text)
            if not isinstance(parsed_payload, dict):
                raise ValueError("Fake semantic fit extraction expected a JSON payload object")
            return _fake_semantic_fit_response(parsed_payload)
        if schema_name == "preference_profile_schema":
            return _fake_preference_profile_response(text)
        if schema_name == "preference_semantic_rerank_v1":
            parsed_payload = json.loads(text)
            if not isinstance(parsed_payload, dict):
                raise ValueError("Fake preference rerank extraction expected a JSON payload object")
            return _fake_preference_rerank_response(parsed_payload, judge_mode=False)
        if schema_name == "preference_llm_judge_v1":
            parsed_payload = json.loads(text)
            if not isinstance(parsed_payload, dict):
                raise ValueError("Fake preference judge extraction expected a JSON payload object")
            return _fake_preference_rerank_response(parsed_payload, judge_mode=True)
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        return {}

    def extract_resume_data(self, text: str) -> Dict[str, Any]:
        self._maybe_fail_extraction(text)
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f"Fake resume extraction expected JSON fixture input: {exc}") from exc

        if not isinstance(parsed, dict) or "profile" not in parsed or "extraction" not in parsed:
            raise ValueError("Fake resume extraction expected a structured resume fixture")
        return parsed

    def extract_requirements_data(self, text: str) -> Dict[str, Any]:
        self._maybe_fail_extraction(text)
        tokens = set(_tokenize(text))
        requirements: List[Dict[str, Any]] = []
        for keyword, category in (
            ("python", "technical"),
            ("fastapi", "technical"),
            ("aws", "technical"),
            ("docker", "technical"),
            ("microservices", "technical"),
            ("java", "technical"),
            ("spring", "technical"),
            ("salesforce", "domain_knowledge"),
        ):
            if keyword in tokens:
                requirements.append(
                    {
                        "req_type": "must_have",
                        "category": category,
                        "text": f"Experience with {keyword}",
                        "related_skills": [keyword],
                        "proficiency": "proficient",
                    }
                )

        return {
            "thought_process": "Fake deterministic extraction.",
            "job_summary": text[:200],
            "seniority_level": "Senior" if "senior" in tokens else "Mid-Level",
            "remote_policy": "Remote (Global)" if "remote" in tokens else "On-site",
            "visa_sponsorship_available": False,
            "min_years_experience": 5 if "senior" in tokens else 2,
            "requires_degree": False,
            "security_clearance": False,
            "salary_min": None,
            "salary_max": None,
            "currency": None,
            "tech_stack": sorted(tokens & set(_KEYWORD_DIMENSIONS.keys())),
            "requirements": requirements,
            "benefits": [],
        }

    def generate_embedding(self, text: str) -> List[float]:
        self._maybe_fail_embedding(text)
        vector = [0.0] * self.embedding_dimensions
        lowered = text.lower()
        for keyword, idx in _KEYWORD_DIMENSIONS.items():
            if idx >= self.embedding_dimensions:
                continue
            count = lowered.count(keyword)
            if count:
                vector[idx] = float(count)

        hash_offset = max(_KEYWORD_DIMENSIONS.values(), default=-1) + 1
        for token in _tokenize(text):
            if self.embedding_dimensions <= hash_offset:
                break
            digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
            slot = hash_offset + (int(digest[:8], 16) % (self.embedding_dimensions - hash_offset))
            vector[slot] += 0.15

        return _unit_normalize(vector)

    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        return [self.generate_embedding(text) for text in texts]

    def unload_model(self, model_name: str) -> None:
        logger.debug("Fake provider ignoring unload_model(%s)", model_name)
