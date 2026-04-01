"""Unit tests for semantic fit scoring contracts."""

from unittest.mock import MagicMock

from core.config_loader import ScorerConfig
from core.matcher import JobMatchPreliminary, RequirementMatchResult
from core.scorer.semantic_fit import ThresholdSemanticFitScorer


def _make_requirement(*, req_id: str, req_type: str, text: str, weight: float = 1.0):
    requirement = MagicMock()
    requirement.id = req_id
    requirement.req_type = req_type
    requirement.text = text
    requirement.weight = weight
    return requirement


def _make_evidence(text: str, source_section: str):
    evidence = MagicMock()
    evidence.text = text
    evidence.source_section = source_section
    return evidence


def test_threshold_semantic_fit_adds_structured_explanation():
    job = MagicMock()
    job.id = "job-1"

    python_match = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-1",
            req_type="required",
            text="Strong Python experience",
        ),
        evidence=_make_evidence("Built Python services", "experience"),
        similarity=0.92,
        is_covered=True,
    )
    aws_gap = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-2",
            req_type="required",
            text="AWS production experience",
        ),
        evidence=None,
        similarity=0.0,
        is_covered=False,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.8,
        requirement_matches=[python_match],
        missing_requirements=[aws_gap],
        resume_fingerprint="fp-1",
    )

    result = ThresholdSemanticFitScorer().score(
        preliminary,
        fit_penalties=0.0,
        config=ScorerConfig(),
    )

    assert result.fit_score > 0
    assert result.fit_confidence == 0.86
    assert result.fit_components["fit_scorer"]["name"] == "threshold_semantic_fit"
    assert "fit_explanation" in result.fit_components
    assert result.fit_explanation["summary"] == (
        "Covered 1 of 2 required requirements (50%) and 0 of 0 preferred requirements (0%)."
    )
    assert result.fit_explanation["strengths"][0]["requirement_id"] == "req-1"
    assert result.fit_explanation["gaps"][0]["requirement_id"] == "req-2"
    assert result.fit_explanation["requirement_verdicts"][0]["evidence_section"] == "experience"


def test_threshold_semantic_fit_marks_partial_requirement_when_similarity_below_threshold():
    job = MagicMock()
    partial_match = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-3",
            req_type="required",
            text="GraphQL API experience",
        ),
        evidence=_make_evidence("Read GraphQL schemas", "projects"),
        similarity=0.4,
        is_covered=False,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.4,
        requirement_matches=[],
        missing_requirements=[partial_match],
        resume_fingerprint="fp-2",
    )

    result = ThresholdSemanticFitScorer().score(
        preliminary,
        fit_penalties=0.0,
        config=ScorerConfig(),
    )

    verdict = result.fit_explanation["requirement_verdicts"][0]
    assert verdict["verdict"] == "partial"
    assert "did not clear the fit threshold" in verdict["reason"]
