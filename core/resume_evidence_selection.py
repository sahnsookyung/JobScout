"""Utilities for selecting job-relevant resume evidence."""

from __future__ import annotations

import json
import re
from typing import Any, Iterable

TOKEN_RE = re.compile(r"c\+\+|c#|[A-Za-z][A-Za-z0-9]*(?:[.+#-][A-Za-z0-9]+)*")

STOP_TERMS = {
    "a",
    "able",
    "about",
    "across",
    "all",
    "also",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "business",
    "can",
    "candidate",
    "client",
    "company",
    "development",
    "engineering",
    "experience",
    "for",
    "from",
    "good",
    "have",
    "in",
    "including",
    "is",
    "it",
    "job",
    "knowledge",
    "must",
    "of",
    "on",
    "or",
    "our",
    "role",
    "skills",
    "team",
    "that",
    "the",
    "this",
    "to",
    "using",
    "we",
    "with",
    "work",
    "you",
}

ALIASES = {
    "js": "javascript",
    "node.js": "node",
    "react.js": "react",
    "vue.js": "vue",
    "typescript": "typescript",
}


def build_job_relevance_terms(
    requirements: Iterable[Any],
    *,
    job_texts: Iterable[Any] = (),
    max_terms: int = 80,
) -> set[str]:
    """Extract stable, public keyword terms from requirements before fallback text."""
    terms: list[str] = []
    seen: set[str] = set()

    for requirement in requirements:
        for value in (
            getattr(requirement, "text", None),
            getattr(requirement, "tags", None),
            getattr(requirement, "years_context", None),
        ):
            _extend_terms(terms, seen, value)
            if len(terms) >= max_terms:
                return set(terms[:max_terms])

    for value in job_texts:
        _extend_terms(terms, seen, value)
        if len(terms) >= max_terms:
            break

    return set(terms[:max_terms])


def select_relevant_resume_evidence_units(
    units: list[Any],
    requirements: Iterable[Any],
    *,
    max_count: int,
    extra_count: int = 0,
    job_texts: Iterable[Any] = (),
) -> list[Any]:
    """Return the most requirement-relevant evidence units, preserving stable ties."""
    include_count = max(1, int(max_count)) + max(0, int(extra_count))
    if len(units) <= include_count:
        return list(units)

    terms = build_job_relevance_terms(requirements, job_texts=job_texts)
    if not terms:
        return list(units[:include_count])

    scored: list[tuple[int, int, Any]] = [
        (_score_evidence_unit(unit, terms), index, unit)
        for index, unit in enumerate(units)
    ]
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [unit for _score, _index, unit in scored[:include_count]]


def _extend_terms(terms: list[str], seen: set[str], value: Any) -> None:
    for text in _iter_public_strings(value):
        for token in TOKEN_RE.findall(text):
            for normalized in _normalize_token(token):
                if normalized in seen or normalized in STOP_TERMS or len(normalized) < 2:
                    continue
                seen.add(normalized)
                terms.append(normalized)


def _score_evidence_unit(unit: Any, terms: set[str]) -> int:
    haystack = " ".join(
        _iter_public_strings(
            {
                "source_text": getattr(unit, "source_text", None),
                "source_section": getattr(unit, "source_section", None),
                "tags": getattr(unit, "tags", None),
                "years_context": getattr(unit, "years_context", None),
            }
        )
    ).lower()
    if not haystack:
        return 0

    unit_terms = set()
    for token in TOKEN_RE.findall(haystack):
        unit_terms.update(_normalize_token(token))

    exact_hits = terms & unit_terms
    substring_hits = {
        term
        for term in terms - exact_hits
        if len(term) >= 4 and term in haystack
    }
    score = len(exact_hits) * 10 + len(substring_hits) * 3
    section = str(getattr(unit, "source_section", "") or "").lower()
    if section in {"skills", "projects", "experience"}:
        score += 1
    return score


def _normalize_token(token: str) -> list[str]:
    lowered = token.lower().strip(".,:;()[]{}")
    if not lowered:
        return []
    normalized = ALIASES.get(lowered, lowered)
    values = [normalized]
    for separator in (".", "-", "+"):
        if separator in normalized:
            values.extend(part for part in normalized.split(separator) if part)
    return values


def _iter_public_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        text = value.replace("\x00", "").strip()
        if text:
            yield text
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_public_strings(item)
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_public_strings(item)
        return
    if isinstance(value, (int, float, bool)) or value is None:
        return
    try:
        encoded = json.dumps(value, default=str)
    except Exception:
        return
    if encoded:
        yield encoded
