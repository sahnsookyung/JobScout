"""Final scores and explanations must use the same evidence judgments."""

from types import SimpleNamespace

import pytest

from core.config_loader import ScorerConfig
from core.matcher.models import JobMatchPreliminary, RequirementMatchResult
from core.scorer.semantic_fit import CrossEncoderSemanticFitScorer, PairAssessment, ThresholdSemanticFitScorer
from core.scorer.service import ScoringService


class JudgmentProvider:
    route_name = "local"

    def __init__(self, covered: bool) -> None:
        self.covered = covered

    def score_pairs(self, pairs):
        return [PairAssessment(
            pair_id=p.pair_id, requirement_id=p.requirement_id,
            coverage_level="covered" if self.covered else "missing",
            semantic_score=0.95 if self.covered else 0.05,
            confidence=0.95, reason="Fixture judgment",
        ) for p in pairs], {"provider_id": "fixture", "provider_route": "local", "latency_ms": 0}


def preliminary(covered: bool) -> JobMatchPreliminary:
    requirement = SimpleNamespace(id="python", text="Python development", req_type="required", weight=1.0)
    evidence = SimpleNamespace(text="Built Python APIs", source_section="experience", years_value=4)
    match = RequirementMatchResult(requirement, evidence, 0.9 if covered else 0.3, covered)
    return JobMatchPreliminary(
        job=SimpleNamespace(id="job", title="Engineer", company="Fixture", canonical_job_summary="Python", description="Python"),
        job_similarity=0.8, requirement_matches=[match] if covered else [],
        missing_requirements=[] if covered else [match], resume_fingerprint="fixture",
    )


@pytest.mark.parametrize("retrieved_covered,judged_covered", [(False, True), (True, False)])
def test_penalties_follow_semantic_coverage_in_both_directions(retrieved_covered, judged_covered):
    config = ScorerConfig(penalty_missing_required=100.0)
    scorer = ScoringService(None, config, semantic_fit_scorer=CrossEncoderSemanticFitScorer(
        local_provider=JudgmentProvider(judged_covered), remote_provider=None,
    ))
    result = scorer.score_preliminary_match(preliminary(retrieved_covered))
    assert result.penalties == (0 if judged_covered else 100)
    assert result.fit_components["fit_penalties"] == result.penalties
    assert result.fit_components["fit_score"] == result.fit_score
    assert result.fit_components["semantic_fit_diagnostics"]["provider_id"] == "fixture"
    if judged_covered:
        # Recompute from the unclipped components; adding back a penalty to a
        # score already clamped to zero would incorrectly produce 100 here.
        assert 80 < result.fit_score < 100
        assert result.penalty_details == []
        assert len(result.matched_requirements) == 1
    else:
        assert result.fit_score == 0
        assert result.penalty_details[0]["amount"] == 100
        assert len(result.missing_requirements) == 1


@pytest.mark.parametrize("covered", [True, False])
def test_threshold_scoring_preserves_existing_score(covered, monkeypatch):
    monkeypatch.setattr("core.scorer.semantic_fit.time.perf_counter", lambda: 1.0)
    config = ScorerConfig(penalty_missing_required=15)
    item = preliminary(covered)
    threshold = ThresholdSemanticFitScorer()
    expected = threshold.score(item, fit_penalties=0 if covered else 15, config=config)
    actual = ScoringService(None, config, semantic_fit_scorer=threshold).score_preliminary_match(item)
    assert actual.fit_score == expected.fit_score
    assert actual.fit_components == expected.fit_components
