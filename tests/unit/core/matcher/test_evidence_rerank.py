"""Tests for the evidence cross-encoder reranker.

Covers the critical behavior: with vector-similarity retrieval, the "best"
candidate is often the wrong one because cosine over generic tokens
confuses requirements like "Bachelor's degree" with tooling chunks. Rerank
picks the semantically correct candidate.
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

import pytest

from core.matcher.evidence_reranker import rerank_requirement_evidence
from core.matcher.models import RequirementEvidenceCandidate, RequirementMatchResult


@dataclass
class _FakeRequirement:
    id: str
    text: str
    req_type: str = "required"


@dataclass
class _FakeEvidence:
    text: str


class _StubProvider:
    def __init__(self, score_map: dict[Tuple[str, str], float]):
        self.score_map = score_map
        self.calls: List[List[Tuple[str, str]]] = []

    def score_text_pairs(self, pairs: List[Tuple[str, str]]) -> List[float]:
        self.calls.append(list(pairs))
        return [self.score_map.get(pair, 0.0) for pair in pairs]


class _RaisingProvider:
    def score_text_pairs(self, pairs):
        raise RuntimeError("provider offline")


def _candidate(text: str, similarity: float, rank: int) -> RequirementEvidenceCandidate:
    return RequirementEvidenceCandidate(
        evidence=_FakeEvidence(text=text),
        similarity=similarity,
        rank=rank,
    )


def _match(req_text: str, candidates: List[RequirementEvidenceCandidate]) -> RequirementMatchResult:
    # Seed the initial best as the highest-similarity (vector) candidate.
    initial = max(candidates, key=lambda c: c.similarity) if candidates else None
    return RequirementMatchResult(
        requirement=_FakeRequirement(id=req_text, text=req_text),
        evidence=initial.evidence if initial else None,
        similarity=initial.similarity if initial else 0.0,
        is_covered=False,
        evidence_candidates=list(candidates),
    )


def test_rerank_picks_semantic_winner_over_vector_winner():
    degree_chunk = "BSc Computer Science, University of X"
    tool_chunk = "Built a semantic matching engine using vector similarity"

    req = "Bachelor's degree"
    match = _match(
        req,
        [
            _candidate(text=tool_chunk, similarity=0.82, rank=1),
            _candidate(text=degree_chunk, similarity=0.61, rank=2),
        ],
    )

    provider = _StubProvider(
        score_map={(req, tool_chunk): 0.10, (req, degree_chunk): 0.93},
    )

    rerank_requirement_evidence(provider=provider, requirement_matches=[match])

    assert match.evidence.text == degree_chunk
    assert match.similarity == pytest.approx(0.61)
    assert match.evidence_score == pytest.approx(0.93)
    assert len(provider.calls) == 1


def test_rerank_batches_all_pairs_into_one_provider_call():
    req_a = "4+ years frontend"
    req_b = "SQL proficiency"
    match_a = _match(
        req_a,
        [
            _candidate(text="Used Vite for bundling", similarity=0.70, rank=1),
            _candidate(text="5 years React and TypeScript at Acme", similarity=0.60, rank=2),
        ],
    )
    match_b = _match(
        req_b,
        [
            _candidate(text="Wrote complex SQL queries and stored procedures", similarity=0.55, rank=1),
        ],
    )

    provider = _StubProvider(
        score_map={
            (req_a, "Used Vite for bundling"): 0.15,
            (req_a, "5 years React and TypeScript at Acme"): 0.88,
            (req_b, "Wrote complex SQL queries and stored procedures"): 0.91,
        },
    )

    rerank_requirement_evidence(provider=provider, requirement_matches=[match_a, match_b])

    assert len(provider.calls) == 1, "All pairs must be batched into one score call per job"
    assert len(provider.calls[0]) == 3

    assert match_a.evidence.text == "5 years React and TypeScript at Acme"
    assert match_a.evidence_score == pytest.approx(0.88)
    assert match_b.evidence_score == pytest.approx(0.91)


def test_rerank_leaves_results_untouched_when_provider_raises():
    req = "Python"
    match = _match(
        req,
        [
            _candidate(text="Python and Django", similarity=0.80, rank=1),
            _candidate(text="C++ expert", similarity=0.40, rank=2),
        ],
    )
    original_evidence = match.evidence
    original_similarity = match.similarity

    rerank_requirement_evidence(
        provider=_RaisingProvider(), requirement_matches=[match]
    )

    assert match.evidence is original_evidence
    assert match.similarity == original_similarity
    assert match.evidence_score is None


def test_rerank_skips_results_without_candidates():
    req = "GraphQL"
    match = RequirementMatchResult(
        requirement=_FakeRequirement(id=req, text=req),
        evidence=None,
        similarity=0.0,
        is_covered=False,
        evidence_candidates=[],
    )
    provider = _StubProvider(score_map={})

    rerank_requirement_evidence(provider=provider, requirement_matches=[match])

    assert provider.calls == []
    assert match.evidence_score is None


def test_rerank_skips_candidates_with_empty_evidence_text():
    req = "Go experience"
    match = _match(
        req,
        [
            _candidate(text="", similarity=0.90, rank=1),
            _candidate(text="Wrote production Go services", similarity=0.55, rank=2),
        ],
    )
    provider = _StubProvider(
        score_map={(req, "Wrote production Go services"): 0.77},
    )

    rerank_requirement_evidence(provider=provider, requirement_matches=[match])

    # Only the non-empty candidate was scored; it becomes the pick.
    assert match.evidence.text == "Wrote production Go services"
    assert match.evidence_score == pytest.approx(0.77)
