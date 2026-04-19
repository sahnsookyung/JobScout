"""Tests for the LLM-escalation candidate detector (§B.4)."""

from dataclasses import dataclass
from typing import Optional

from core.matcher.models import RequirementMatchResult
from core.scorer.semantic_fit import identify_evidence_escalation_candidates


@dataclass
class _Req:
    id: str
    req_type: str = "required"


def _rm(
    req_id: str,
    *,
    original_covered: bool,
    adjusted_covered: bool,
    evidence_score: Optional[float],
) -> tuple[RequirementMatchResult, RequirementMatchResult]:
    req = _Req(id=req_id)
    original = RequirementMatchResult(
        requirement=req,
        evidence=None,
        similarity=0.5,
        is_covered=original_covered,
    )
    adjusted = RequirementMatchResult(
        requirement=req,
        evidence=None,
        similarity=0.5,
        is_covered=adjusted_covered,
        evidence_score=evidence_score,
    )
    return original, adjusted


def test_selects_borderline_disagreements():
    o_a, a_a = _rm("r1", original_covered=True, adjusted_covered=False, evidence_score=0.50)
    o_b, a_b = _rm("r2", original_covered=False, adjusted_covered=True, evidence_score=0.55)

    ids = identify_evidence_escalation_candidates(
        original_matches=[o_a, o_b],
        adjusted_matches=[a_b],
        adjusted_missing=[a_a],
        borderline_band=(0.40, 0.65),
    )
    assert sorted(ids) == ["r1", "r2"]


def test_skips_when_verdicts_agree():
    o, a = _rm("r1", original_covered=True, adjusted_covered=True, evidence_score=0.55)
    assert (
        identify_evidence_escalation_candidates(
            original_matches=[o],
            adjusted_matches=[a],
            adjusted_missing=[],
            borderline_band=(0.40, 0.65),
        )
        == []
    )


def test_skips_when_outside_band():
    o_high, a_high = _rm("r1", original_covered=False, adjusted_covered=True, evidence_score=0.90)
    o_low, a_low = _rm("r2", original_covered=True, adjusted_covered=False, evidence_score=0.10)
    assert (
        identify_evidence_escalation_candidates(
            original_matches=[o_high, o_low],
            adjusted_matches=[a_high],
            adjusted_missing=[a_low],
            borderline_band=(0.40, 0.65),
        )
        == []
    )


def test_skips_when_no_evidence_score():
    o, a = _rm("r1", original_covered=True, adjusted_covered=False, evidence_score=None)
    assert (
        identify_evidence_escalation_candidates(
            original_matches=[o],
            adjusted_matches=[],
            adjusted_missing=[a],
            borderline_band=(0.40, 0.65),
        )
        == []
    )
