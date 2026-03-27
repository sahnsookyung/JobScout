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


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9_+-]+", text.lower())


def _unit_normalize(vector: List[float]) -> List[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        if vector:
            vector[-1] = 1.0
        return vector
    return [value / norm for value in vector]


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
        del schema_spec, system_prompt, user_message
        self._maybe_fail_extraction(text)
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

    def extract_facet_data(self, text: str) -> Dict[str, str]:
        self._maybe_fail_extraction(text)
        return {
            "remote_flexibility": "Remote-friendly" if "remote" in text.lower() else "On-site",
            "compensation": "Competitive compensation",
            "learning_growth": "Learning budget and mentorship",
            "company_culture": "Collaborative engineering culture",
            "work_life_balance": "Flexible work practices",
            "tech_stack": text[:200],
            "visa_sponsorship": "Not specified",
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
