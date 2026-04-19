"""Cross-encoder rerank of requirement evidence candidates.

Stage 1 retrieval picks evidence chunks by cosine similarity against
the requirement embedding. That routinely pairs "Bachelor's degree" with
chunks like "Semantic matching engine using vector similarity…" because
the two share generic technical tokens. Reranking with a cross-encoder
that sees both texts jointly fixes those pairings — its evidence_score
is a much stronger signal than cosine for relevance.

Contract:
- Accept an injected cross-encoder provider (must expose `score_text_pairs`).
  Do NOT instantiate a second model — callers should hand in the shared
  provider from `get_shared_local_cross_encoder_provider`.
- Batch all (requirement, candidate) pairs for a job into one score call
  so we pay the model's per-call overhead once per job instead of per
  requirement.
- Mutate each RequirementMatchResult in place: pick the highest-scoring
  candidate's evidence, write the new similarity, and set evidence_score.
- If a provider call fails, leave results untouched — vector-similarity
  evidence still populates the UI, just without the rerank score.
"""

from __future__ import annotations

import logging
from typing import Iterable, List, Protocol

from core.matcher.models import RequirementMatchResult

logger = logging.getLogger(__name__)


class CrossEncoderLike(Protocol):
    def score_text_pairs(self, pairs: List[tuple[str, str]]) -> List[float]:
        """Score (query, doc) text pairs and return a float per pair."""


def rerank_requirement_evidence(
    *,
    provider: CrossEncoderLike,
    requirement_matches: Iterable[RequirementMatchResult],
) -> None:
    """Rerank evidence candidates per requirement; mutate results in place."""
    materialized = [r for r in requirement_matches if r.evidence_candidates]
    if not materialized:
        return

    flat_pairs, offsets = _flatten_pairs(materialized)
    if not flat_pairs:
        return

    scores = _score_or_none(provider, flat_pairs)
    if scores is None:
        return

    _apply_scores(materialized, offsets, scores, total=len(flat_pairs))


def _flatten_pairs(
    materialized: List[RequirementMatchResult],
) -> tuple[List[tuple[str, str]], List[tuple[int, int]]]:
    flat_pairs: List[tuple[str, str]] = []
    offsets: List[tuple[int, int]] = []
    for idx, result in enumerate(materialized):
        req_text = _requirement_text(result) or ""
        if not req_text:
            continue
        start = len(flat_pairs)
        for candidate in result.evidence_candidates:
            cand_text = _evidence_text(candidate)
            if cand_text:
                flat_pairs.append((req_text, cand_text))
        offsets.append((idx, start))
    return flat_pairs, offsets


def _score_or_none(
    provider: CrossEncoderLike, pairs: List[tuple[str, str]]
) -> List[float] | None:
    try:
        return provider.score_text_pairs(pairs)
    except Exception as exc:  # noqa: BLE001 — reranker degrades gracefully
        logger.warning(
            "Evidence rerank failed; keeping vector-similarity evidence: %s", exc
        )
        return None


def _apply_scores(
    materialized: List[RequirementMatchResult],
    offsets: List[tuple[int, int]],
    scores: List[float],
    *,
    total: int,
) -> None:
    for position, (result_idx, start) in enumerate(offsets):
        end = offsets[position + 1][1] if position + 1 < len(offsets) else total
        window = scores[start:end]
        if not window:
            continue
        _update_best_evidence(materialized[result_idx], window, result_idx)


def _update_best_evidence(
    result: RequirementMatchResult, window_scores: List[float], result_idx: int
) -> None:
    candidates = [c for c in result.evidence_candidates if _evidence_text(c)]
    if len(candidates) != len(window_scores):
        logger.debug(
            "Rerank skipped requirement idx=%d: candidate/score mismatch (%d vs %d)",
            result_idx, len(candidates), len(window_scores),
        )
        return
    best_index = max(range(len(window_scores)), key=window_scores.__getitem__)
    best_candidate = candidates[best_index]
    result.evidence = best_candidate.evidence
    result.similarity = float(best_candidate.similarity or 0.0)
    result.evidence_score = float(window_scores[best_index])


def _requirement_text(result: RequirementMatchResult) -> str:
    req = result.requirement
    for attr in ("text", "requirement_text", "description"):
        value = getattr(req, attr, None)
        if value:
            return str(value)
    return str(req) if req is not None else ""


def _evidence_text(candidate) -> str:
    evidence = getattr(candidate, "evidence", None)
    if evidence is None:
        return ""
    return getattr(evidence, "text", "") or ""
