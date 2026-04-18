"""Unit tests for semantic fit scoring contracts."""

import sys
import types
from unittest.mock import MagicMock

import pytest

from core.config_loader import ScorerConfig
from tests.mocks.fake_service import FakeLLMService
from core.matcher import JobMatchPreliminary, RequirementMatchResult
from core.matcher.models import RequirementEvidenceCandidate
from core.scorer.semantic_fit import (
    CrossEncoderSemanticFitScorer,
    FEATURE_ALLOWED_MODES,
    FEATURE_PREFERRED_MODE,
    LLMSemanticFitScorer,
    LocalCrossEncoderProvider,
    PairAssessment,
    RemoteCrossEncoderProvider,
    _build_retrieval_diagnostics,
    _build_serialized_pairs,
    _candidate_evidence_candidates,
    _coverage_level_and_reason,
    _default_effective_allowed,
    _effective_fit_mode,
    _fallback_adjusted_match,
    _missing_assessment_verdict,
    _normalize_modes,
    _preferred_capability_mode,
    _pair_assessment_from_heuristic,
    _scored_requirement_verdict,
    _score_requirement_match,
    _select_best_assessment,
    _serialize_pair,
    _truncation_aggregate,
    _zero_evidence_verdict,
    ThresholdSemanticFitScorer,
    resolve_effective_fit_mode,
)


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


def _make_requirement_match(
    *,
    req_id: str = "req-1",
    req_type: str = "required",
    text: str = "Python backend API development",
    evidence_text: str | None = "Built Python backend APIs for internal services",
    evidence_section: str = "experience",
    similarity: float = 0.72,
    is_covered: bool = True,
    rank: int = 1,
):
    evidence = _make_evidence(evidence_text, evidence_section) if evidence_text is not None else None
    evidence_candidates = []
    if evidence is not None:
        evidence_candidates = [
            RequirementEvidenceCandidate(
                evidence=evidence,
                similarity=similarity,
                rank=rank,
            )
        ]
    return RequirementMatchResult(
        requirement=_make_requirement(
            req_id=req_id,
            req_type=req_type,
            text=text,
        ),
        evidence=evidence,
        similarity=similarity,
        is_covered=is_covered,
        evidence_candidates=evidence_candidates,
    )


def _make_preliminary(requirement_matches=None, missing_requirements=None):
    job = MagicMock()
    job.id = "job-1"
    job.title = "Python Engineer"
    job.company = "Acme"
    job.canonical_job_summary = "Python backend APIs, services, and platform ownership."
    job.description = "Python backend APIs, services, and platform ownership."
    return JobMatchPreliminary(
        job=job,
        job_similarity=0.75,
        requirement_matches=requirement_matches or [],
        missing_requirements=missing_requirements or [],
        resume_fingerprint="fp-1",
        retrieval_score=0.83,
        lexical_score=0.55,
    )


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
    assert result.fit_confidence == pytest.approx((0.46 + 0.8) / 2.0, abs=1e-4)
    assert result.fit_components["fit_scorer"]["name"] == "threshold_semantic_fit"
    assert result.fit_components["retrieval"] == {
        "mode": "dense",
        "sources": ["dense"],
        "retrieval_score": 0.0,
        "job_similarity": 0.8,
    }
    assert result.fit_components["semantic_fit_diagnostics"]["fallback_used"] is False
    assert "fit_explanation" in result.fit_components
    assert result.fit_explanation["summary"] == (
        "Covered 1 of 2 required requirements (50%) and 0 of 0 preferred requirements (0%)."
    )
    assert result.fit_explanation["retrieval"]["mode"] == "dense"
    assert result.fit_explanation["diagnostics"]["name"] == "threshold_semantic_fit"
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


def test_llm_semantic_fit_demotes_false_positive_skill_match():
    job = MagicMock()
    job.id = "job-java"
    job.title = "Java Engineer"
    job.company = "Acme"
    job.description = "Backend Java role"

    java_requirement = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-java",
            req_type="required",
            text="Strong Java programming experience",
        ),
        evidence=_make_evidence("Built Python services and FastAPI APIs", "experience"),
        similarity=0.82,
        is_covered=True,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.8,
        requirement_matches=[java_requirement],
        missing_requirements=[],
        resume_fingerprint="fp-java",
    )

    result = LLMSemanticFitScorer(FakeLLMService()).score(
        preliminary,
        fit_penalties=0.0,
        config=ScorerConfig(),
    )

    assert result.scorer_name == "llm_semantic_fit"
    assert result.matched_requirements == []
    assert len(result.missing_requirements) == 1
    verdict = result.fit_explanation["requirement_verdicts"][0]
    assert verdict["verdict"] == "missing"
    assert verdict["semantic_score"] == 0.0


def test_llm_semantic_fit_promotes_related_missing_requirement():
    job = MagicMock()
    job.id = "job-python"
    job.title = "Python Engineer"
    job.company = "Acme"
    job.description = "Backend Python role"

    python_requirement = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-python",
            req_type="required",
            text="Python backend API development",
        ),
        evidence=_make_evidence("Built Python backend APIs for internal services", "experience"),
        similarity=0.31,
        is_covered=False,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.7,
        requirement_matches=[],
        missing_requirements=[python_requirement],
        resume_fingerprint="fp-python",
        retrieval_score=0.92,
        lexical_score=0.61,
    )

    result = LLMSemanticFitScorer(FakeLLMService()).score(
        preliminary,
        fit_penalties=0.0,
        config=ScorerConfig(),
    )

    assert len(result.matched_requirements) == 1
    assert result.missing_requirements == []
    verdict = result.fit_explanation["requirement_verdicts"][0]
    assert verdict["verdict"] == "covered"
    assert verdict["semantic_score"] > 0.7
    assert result.fit_explanation["summary"] == (
        "Covered 1 of 1 required requirements (100%) and 0 of 0 preferred requirements (0%)."
    )
    assert result.fit_components["retrieval"] == {
        "mode": "hybrid",
        "sources": ["dense", "lexical"],
        "retrieval_score": 0.92,
        "job_similarity": 0.7,
        "lexical_score": 0.61,
    }
    assert result.fit_explanation["retrieval"]["mode"] == "hybrid"
    assert result.fit_explanation["diagnostics"]["judged_requirements"] == 1
    assert "model_summary" not in result.fit_explanation
    assert result.fit_components["semantic_fit_summary"] == "Covered 1 of 1 required requirements."


def test_llm_semantic_fit_respects_fallback_disable_flag():
    job = MagicMock()
    job.id = "job-fallback"
    job.title = "Python Engineer FAIL_EXTRACTION"
    job.company = "Acme"
    job.description = "Backend Python role"

    requirement = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-fallback",
            req_type="required",
            text="Python backend API development",
        ),
        evidence=_make_evidence("Built Python backend APIs for internal services", "experience"),
        similarity=0.61,
        is_covered=True,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.7,
        requirement_matches=[requirement],
        missing_requirements=[],
        resume_fingerprint="fp-fallback",
    )

    with pytest.raises(ValueError, match="Fake extraction failure"):
        LLMSemanticFitScorer(FakeLLMService()).score(
            preliminary,
            fit_penalties=0.0,
            config=ScorerConfig(semantic_fit_fallback_to_threshold=False),
        )


def test_llm_semantic_fit_fallback_records_diagnostics():
    job = MagicMock()
    job.id = "job-fallback"
    job.title = "Python Engineer FAIL_EXTRACTION"
    job.company = "Acme"
    job.description = "Backend Python role"

    requirement = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-fallback",
            req_type="required",
            text="Python backend API development",
        ),
        evidence=_make_evidence("Built Python backend APIs for internal services", "experience"),
        similarity=0.61,
        is_covered=True,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.7,
        requirement_matches=[requirement],
        missing_requirements=[],
        resume_fingerprint="fp-fallback",
        retrieval_score=0.81,
        lexical_score=0.52,
    )

    result = LLMSemanticFitScorer(FakeLLMService()).score(
        preliminary,
        fit_penalties=0.0,
        config=ScorerConfig(),
    )

    assert result.fit_components["semantic_fit_fallback_reason"] == "Fake extraction failure"
    assert result.fit_components["semantic_fit_diagnostics"]["fallback_used"] is True
    assert result.fit_components["semantic_fit_diagnostics"]["fallback_reason"] == "Fake extraction failure"
    assert result.fit_explanation["message"] == "Semantic fit scorer unavailable; using threshold fallback."
    assert result.fit_explanation["diagnostics"]["fallback_used"] is True
    assert result.fit_explanation["retrieval"]["mode"] == "hybrid"


def test_resolve_effective_fit_mode_uses_capability_preference_when_allowed():
    repo = MagicMock()
    repo.get_capability.side_effect = [
        MagicMock(enabled=True, value_json={"modes": ["cross_encoder", "llm"]}),
        MagicMock(enabled=True, value_json={"mode": "llm"}),
    ]
    config = ScorerConfig()
    config.semantic_fit.deploy_allowed_modes = ["cross_encoder", "llm"]

    resolved_mode, allowed = resolve_effective_fit_mode(repo, config, owner_id="user-1")

    assert resolved_mode == "llm"
    assert allowed == ["cross_encoder", "llm"]
    repo.get_capability.assert_any_call("user-1", FEATURE_ALLOWED_MODES)
    repo.get_capability.assert_any_call("user-1", FEATURE_PREFERRED_MODE)


def test_resolve_effective_fit_mode_ignores_invalid_capability_payloads():
    repo = MagicMock()
    repo.get_capability.side_effect = [
        MagicMock(enabled=True, value_json="invalid"),
        MagicMock(enabled=True, value_json={"mode": "llm"}),
    ]
    config = ScorerConfig()
    config.semantic_fit.deploy_allowed_modes = ["cross_encoder", "llm"]

    resolved_mode, allowed = resolve_effective_fit_mode(repo, config, owner_id="user-1")

    assert resolved_mode == "cross_encoder"
    assert allowed == ["cross_encoder"]


def test_local_cross_encoder_provider_auto_prefers_flag_embedding_runtime():
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="auto",
    )
    fake_runtime = MagicMock()

    provider._load_flag_embedding_runtime = MagicMock(return_value=fake_runtime)
    provider._load_sentence_transformers_runtime = MagicMock()

    result = provider._load_model()

    assert result is fake_runtime
    provider._load_flag_embedding_runtime.assert_called_once()
    provider._load_sentence_transformers_runtime.assert_not_called()


def test_local_cross_encoder_provider_auto_falls_back_to_sentence_transformers():
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="auto",
    )
    fake_runtime = MagicMock()

    provider._load_flag_embedding_runtime = MagicMock(side_effect=ImportError("no flag embedding"))
    provider._load_sentence_transformers_runtime = MagicMock(return_value=fake_runtime)

    result = provider._load_model()

    assert result is fake_runtime
    provider._load_flag_embedding_runtime.assert_called_once()
    provider._load_sentence_transformers_runtime.assert_called_once()


def test_local_cross_encoder_provider_heuristic_fallback_when_no_runtime_available():
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="auto",
        allow_heuristic=True,
    )

    provider._load_flag_embedding_runtime = MagicMock(side_effect=ImportError("no flag embedding"))
    provider._load_sentence_transformers_runtime = MagicMock(side_effect=ImportError("no sentence transformers"))

    result = provider._load_model()

    assert result is False
    assert provider.provider_id == "heuristic-local"
    assert provider.effective_route_name == "local_heuristic"


def test_local_cross_encoder_provider_raises_when_runtimes_fail_and_heuristic_disabled():
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="auto",
    )

    provider._load_flag_embedding_runtime = MagicMock(side_effect=ImportError("no flag embedding"))
    provider._load_sentence_transformers_runtime = MagicMock(side_effect=ImportError("no sentence transformers"))

    with pytest.raises(RuntimeError, match="Local cross-encoder could not be loaded"):
        provider._load_model()


def test_serialize_pair_records_truncation_details():
    preliminary = _make_preliminary(
        requirement_matches=[
            _make_requirement_match(
                text="R" * 700,
                evidence_text="E" * 3000,
            )
        ]
    )
    requirement_match = preliminary.requirement_matches[0]
    candidate = requirement_match.evidence_candidates[0]
    config = ScorerConfig()

    pair = _serialize_pair(preliminary, requirement_match, candidate, config=config)

    assert pair.truncation["truncated"] is True
    assert "requirement_text" in pair.truncation["truncated_fields"]
    assert "evidence_text" in pair.truncation["truncated_fields"]
    assert pair.truncation["field_lengths"]["requirement_text"]["submitted_length"] == 500
    assert pair.truncation["field_lengths"]["evidence_text"]["submitted_length"] == 2500
    assert len(pair.pair_id) == 32


def test_build_serialized_pairs_emits_zero_evidence_verdicts():
    preliminary = _make_preliminary(
        missing_requirements=[
            _make_requirement_match(
                req_id="req-missing",
                evidence_text=None,
                similarity=0.0,
                is_covered=False,
            )
        ]
    )

    pairs, zero_evidence_verdicts, aggregate = _build_serialized_pairs(
        preliminary,
        config=ScorerConfig(),
    )

    assert pairs == []
    assert len(zero_evidence_verdicts) == 1
    assert zero_evidence_verdicts[0]["verdict"] == "missing"
    assert aggregate["pair_count"] == 0


def test_pair_assessment_from_heuristic_marks_overlap_as_covered():
    preliminary = _make_preliminary(
        requirement_matches=[_make_requirement_match()]
    )
    requirement_match = preliminary.requirement_matches[0]
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    assessment = _pair_assessment_from_heuristic(pair)

    assert assessment.coverage_level == "covered"
    assert assessment.semantic_score >= 0.75


def test_pair_assessment_from_heuristic_marks_related_similarity_as_partial():
    requirement_match = _make_requirement_match(
        text="Kubernetes operations",
        evidence_text="Container orchestration and production deployment work",
        similarity=0.58,
        is_covered=False,
    )
    preliminary = _make_preliminary(missing_requirements=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    assessment = _pair_assessment_from_heuristic(pair)

    assert assessment.coverage_level == "partial"
    assert 0.55 <= assessment.semantic_score <= 0.79


def test_pair_assessment_from_heuristic_marks_tech_mismatch_as_missing():
    requirement_match = _make_requirement_match(
        text="Strong Java programming experience",
        evidence_text="Built Python backend APIs for internal services",
        similarity=0.82,
        is_covered=True,
    )
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    assessment = _pair_assessment_from_heuristic(pair)

    assert assessment.coverage_level == "missing"
    assert assessment.reason == "Evidence references different technologies than the requirement."


def test_select_best_assessment_prefers_score_then_confidence_then_similarity():
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    first_pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())
    second_candidate = RequirementEvidenceCandidate(
        evidence=_make_evidence("Built Python APIs", "projects"),
        similarity=0.81,
        rank=2,
    )
    second_pair = _serialize_pair(preliminary, requirement_match, second_candidate, config=ScorerConfig())
    assessments = {
        first_pair.pair_id: PairAssessment(
            pair_id=first_pair.pair_id,
            requirement_id=first_pair.requirement_id,
            coverage_level="partial",
            semantic_score=0.7,
            confidence=0.7,
            reason="partial",
        ),
        second_pair.pair_id: PairAssessment(
            pair_id=second_pair.pair_id,
            requirement_id=second_pair.requirement_id,
            coverage_level="covered",
            semantic_score=0.8,
            confidence=0.5,
            reason="covered",
        ),
    }

    best_pair, best_assessment = _select_best_assessment(
        [first_pair, second_pair],
        assessments,
    )

    assert best_pair == second_pair
    assert best_assessment == assessments[second_pair.pair_id]


def test_remote_cross_encoder_provider_maps_missing_pairs_to_heuristic(monkeypatch):
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"scores": []}

    captured = {}

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured["json"] = kwargs["json"]
        return FakeResponse()

    monkeypatch.setattr("core.scorer.semantic_fit.requests.post", fake_post)

    provider = RemoteCrossEncoderProvider(
        base_url="https://fit.example.com",
        api_key="secret",
        model="reranker-v1",
        timeout_ms=1500,
    )

    assessments, diagnostics = provider.score_pairs([pair])

    assert captured["url"].endswith("/v1/fit/score")
    assert captured["json"]["model"] == "reranker-v1"
    assert assessments[0].coverage_level in {"covered", "partial", "missing"}
    assert diagnostics["provider_route"] == "remote"

def test_remote_cross_encoder_provider_maps_scores_to_partial(monkeypatch):
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"scores": [{"pair_id": pair.pair_id, "raw_logit": 0.6}]}

    monkeypatch.setattr("core.scorer.semantic_fit.requests.post", lambda *args, **kwargs: FakeResponse())

    provider = RemoteCrossEncoderProvider(
        base_url="https://fit.example.com",
        api_key=None,
        model="reranker-v1",
        timeout_ms=1500,
    )

    assessments, _ = provider.score_pairs([pair])

    assert assessments[0].coverage_level == "partial"


def test_resolve_effective_fit_mode_falls_back_to_baseline_when_owner_missing():
    repo = MagicMock()
    config = ScorerConfig()
    config.semantic_fit.deploy_allowed_modes = ["cross_encoder", "llm"]
    config.semantic_fit.baseline_allowed_modes = ["cross_encoder"]

    resolved_mode, allowed = resolve_effective_fit_mode(repo, config, owner_id=None)

    assert resolved_mode == "cross_encoder"
    assert allowed == ["cross_encoder"]
    repo.get_capability.assert_not_called()


def test_resolve_effective_fit_mode_ignores_disabled_capability_row():
    repo = MagicMock()
    repo.get_capability.side_effect = [
        MagicMock(enabled=False, value_json={"modes": ["llm"]}),
        MagicMock(enabled=False, value_json={"mode": "llm"}),
    ]
    config = ScorerConfig()
    config.semantic_fit.deploy_allowed_modes = ["cross_encoder", "llm"]

    resolved_mode, allowed = resolve_effective_fit_mode(repo, config, owner_id="user-1")

    assert resolved_mode == "cross_encoder"
    assert allowed == ["cross_encoder"]

def test_cross_encoder_route_policy_remote_without_remote_provider_uses_threshold_fallback():
    job = MagicMock()
    job.id = "job-remote-fallback"
    job.title = "Python Engineer"
    job.company = "Acme"
    job.description = "Backend Python role"

    requirement = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-remote-fallback",
            req_type="required",
            text="Python backend API development",
        ),
        evidence=_make_evidence("Built Python backend APIs for internal services", "experience"),
        similarity=0.72,
        is_covered=True,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.7,
        requirement_matches=[requirement],
        missing_requirements=[],
        resume_fingerprint="fp-remote-fallback",
    )
    config = ScorerConfig()
    config.semantic_fit.cross_encoder.route_policy = "remote"

    result = CrossEncoderSemanticFitScorer(
        local_provider=LocalCrossEncoderProvider(
            model_name="BAAI/bge-reranker-v2-m3",
            runtime="heuristic",
            allow_heuristic=True,
        ),
        remote_provider=None,
        fallback_scorer=ThresholdSemanticFitScorer(),
    ).score(
        preliminary,
        fit_penalties=0.0,
        config=config,
    )

    assert result.fit_components["provider_route"] == "threshold"
    assert result.fit_components["effective_fit_mode"] == "threshold"
    assert "remote provider" in result.fit_components["semantic_fit_fallback_reason"]

def test_cross_encoder_without_available_local_provider_uses_threshold_fallback():
    job = MagicMock()
    job.id = "job-no-local"
    requirement = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-no-local",
            req_type="required",
            text="Python backend API development",
        ),
        evidence=_make_evidence("Built Python backend APIs for internal services", "experience"),
        similarity=0.72,
        is_covered=True,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.7,
        requirement_matches=[requirement],
        missing_requirements=[],
        resume_fingerprint="fp-no-local",
    )
    config = ScorerConfig()
    config.semantic_fit.cross_encoder.route_policy = "local"

    result = CrossEncoderSemanticFitScorer(
        local_provider=None,
        remote_provider=None,
        fallback_scorer=ThresholdSemanticFitScorer(),
    ).score(
        preliminary,
        fit_penalties=0.0,
        config=config,
    )

    assert result.fit_components["effective_fit_mode"] == "threshold"
    assert result.fit_components["provider_route"] == "threshold"
    assert "disabled" in result.fit_components["semantic_fit_fallback_reason"]

def test_cross_encoder_local_policy_does_not_fall_through_to_remote_provider():
    class FakeRemoteProvider:
        "remote"

        @property
        def provider_id(self):
            return "remote:test-model"

        def score_pairs(self, pairs):
            return (
                [
                    MagicMock(
                        pair_id=pairs[0].pair_id,
                        requirement_id=pairs[0].requirement_id,
                        coverage_level="covered",
                        semantic_score=0.91,
                        confidence=0.88,
                        reason="Evidence strongly matches the requirement.",
                    )
                ],
                {
                    "provider_id": self.provider_id,
                    "provider_route": self.route_name,
                    "latency_ms": 12.3,
                },
            )

    job = MagicMock()
    job.id = "job-local-remote"
    requirement = RequirementMatchResult(
        requirement=_make_requirement(
            req_id="req-local-remote",
            req_type="required",
            text="Python backend API development",
        ),
        evidence=_make_evidence("Built Python backend APIs for internal services", "experience"),
        similarity=0.72,
        is_covered=True,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.7,
        requirement_matches=[requirement],
        missing_requirements=[],
        resume_fingerprint="fp-local-remote",
    )
    config = ScorerConfig()
    config.semantic_fit.cross_encoder.route_policy = "local"

    result = CrossEncoderSemanticFitScorer(
        local_provider=None,
        remote_provider=FakeRemoteProvider(),
        fallback_scorer=ThresholdSemanticFitScorer(),
    ).score(
        preliminary,
        fit_penalties=0.0,
        config=config,
    )

    assert result.fit_components["effective_fit_mode"] == "threshold"
    assert result.fit_components["provider_route"] == "threshold"
    assert "local provider is disabled" in result.fit_components["semantic_fit_fallback_reason"]

def test_cross_encoder_auto_prefers_remote_in_production(monkeypatch):
    provider = CrossEncoderSemanticFitScorer(
        local_provider=MagicMock(route_name="local"),
        remote_provider=MagicMock(route_name="remote"),
        fallback_scorer=ThresholdSemanticFitScorer(),
    )
    config = ScorerConfig()
    config.semantic_fit.cross_encoder.route_policy = "auto"
    config.semantic_fit.cross_encoder.remote.enabled = True
    config.semantic_fit.cross_encoder.remote_promote_pair_count = 1
    monkeypatch.setenv("JOBSCOUT_ENV", "production")

    providers, error = provider._providers_for_route(
        route_policy="auto",
        pair_count=2,
        config=config,
    )

    assert providers[0].route_name == "remote"
    assert error is None

def test_build_retrieval_diagnostics_dense_mode():
    preliminary = _make_preliminary()
    preliminary.lexical_score = None
    preliminary.retrieval_score = None

    diagnostics = _build_retrieval_diagnostics(preliminary)

    assert diagnostics == {
        "mode": "dense",
        "sources": ["dense"],
        "retrieval_score": 0.0,
        "job_similarity": 0.75,
    }

def test_coverage_level_and_reason_thresholds():
    assert _coverage_level_and_reason(0.85)[0] == "covered"
    assert _coverage_level_and_reason(0.60)[0] == "partial"
    assert _coverage_level_and_reason(0.10)[0] == "missing"

def test_candidate_evidence_candidates_creates_fallback_candidate_from_primary_evidence():
    requirement_match = _make_requirement_match()
    requirement_match.evidence_candidates = []

    candidates = _candidate_evidence_candidates(requirement_match)

    assert len(candidates) == 1
    assert candidates[0].rank == 1
    assert candidates[0].similarity == requirement_match.similarity

def test_zero_evidence_verdict_preserves_partial_match_state():
    requirement_match = _make_requirement_match(
        evidence_text="Somewhat related platform work",
        similarity=0.41,
        is_covered=False,
    )

    verdict = _zero_evidence_verdict(requirement_match, threshold=0.5)

    assert verdict["verdict"] == "partial"
    assert verdict["reason"] != "No supporting resume evidence was recalled for this requirement."

def test_truncation_aggregate_counts_truncated_pairs():
    preliminary = _make_preliminary(
        requirement_matches=[
            _make_requirement_match(
                text="R" * 700,
                evidence_text="E" * 3000,
            )
        ]
    )
    requirement_match = preliminary.requirement_matches[0]
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    aggregate = _truncation_aggregate([pair])

    assert aggregate["any_truncated"] is True
    assert aggregate["pair_count"] == 1
    assert aggregate["truncated_pair_count"] == 1
    assert aggregate["total_truncated_chars"] > 0

def test_local_cross_encoder_provider_runtime_helpers():
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="sentence_transformers",
    )

    assert provider._candidate_runtimes() == ["sentence_transformers"]
    assert provider._normalize_runtime_scores({"scores": [{"score": 0.3}, [0.5], 0.9]}) == [0.3, 0.5, 0.9]

    def factory(model_name, cache_dir=None):
        return model_name, cache_dir

    filtered = provider._filter_supported_kwargs(factory, {"cache_dir": "/tmp", "ignored": True})
    assert filtered == {"cache_dir": "/tmp"}

def test_local_cross_encoder_provider_normalize_runtime_errors():
    with pytest.raises(TypeError):
        LocalCrossEncoderProvider._normalize_runtime_item({"unexpected": 1})

    with pytest.raises(TypeError):
        LocalCrossEncoderProvider._normalize_runtime_scores({"unexpected": 1})

def test_local_cross_encoder_provider_loads_flag_embedding_runtime_via_factory(monkeypatch):
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="flag_embedding",
        cache_path="/models",
        trust_remote_code=True,
    )

    class FakeReranker:
        @staticmethod
        def from_finetuned(model_name, cache_dir=None, trust_remote_code=None):
            return {
                "model_name": model_name,
                "cache_dir": cache_dir,
                "trust_remote_code": trust_remote_code,
            }

    fake_module = types.SimpleNamespace(FlagAutoReranker=FakeReranker)
    monkeypatch.setattr("core.scorer.semantic_fit.importlib.import_module", lambda name: fake_module)

    model = provider._load_flag_embedding_runtime()

    assert model["model_name"] == "BAAI/bge-reranker-v2-m3"
    assert model["cache_dir"] == "/models"
    assert provider.provider_id == "flag_embedding:BAAI/bge-reranker-v2-m3"

def test_local_cross_encoder_provider_loads_sentence_transformers_runtime(monkeypatch):
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="sentence_transformers",
        cache_path="/cache",
    )

    class FakeCrossEncoder:
        def __init__(self, model_name, **kwargs):
            self.model_name = model_name
            self.kwargs = kwargs

    monkeypatch.setitem(sys.modules, "sentence_transformers", types.SimpleNamespace(CrossEncoder=FakeCrossEncoder))

    model = provider._load_sentence_transformers_runtime()

    assert model.model_name == "BAAI/bge-reranker-v2-m3"
    assert model.kwargs["cache_folder"] == "/cache"
    assert provider.provider_id == "sentence_transformers:BAAI/bge-reranker-v2-m3"

def test_local_cross_encoder_provider_unknown_runtime_falls_back_to_heuristic():
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="custom_runtime",
        allow_heuristic=True,
    )

    result = provider._load_model()

    assert result is False
    assert provider.effective_route_name == "local_heuristic"

def test_local_cross_encoder_provider_heuristic_scores_pairs():
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="heuristic",
        allow_heuristic=True,
    )

    assessments, diagnostics = provider.score_pairs([pair])

    assert len(assessments) == 1
    assert diagnostics["provider_route"] == "local_heuristic"
    assert diagnostics["provider_id"] == "heuristic-local"

def test_local_cross_encoder_provider_scores_pairs_with_compute_score_runtime():
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="heuristic",
        allow_heuristic=True,
    )
    provider._model = types.SimpleNamespace(compute_score=lambda pairs, batch_size=32: [3.0])
    provider._provider_id = "flag_embedding:test"
    provider._effective_route_name = "local"

    assessments, diagnostics = provider.score_pairs([pair])

    assert assessments[0].coverage_level == "covered"
    assert diagnostics["provider_route"] == "local"

def test_local_cross_encoder_provider_scores_pairs_with_predict_runtime():
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    class PredictModel:
        def predict(self, pairs, batch_size=32, show_progress_bar=False):
            return {"scores": [{"score": 0.6}]}

    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="heuristic",
        allow_heuristic=True,
    )
    provider._model = PredictModel()
    provider._provider_id = "sentence_transformers:test"
    provider._effective_route_name = "local"

    assessments, _ = provider.score_pairs([pair])

    assert assessments[0].coverage_level == "partial"

def test_local_cross_encoder_provider_raises_for_unsupported_runtime_object():
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())
    provider = LocalCrossEncoderProvider(
        model_name="BAAI/bge-reranker-v2-m3",
        runtime="heuristic",
        allow_heuristic=True,
    )
    provider._model = object()
    provider._provider_id = "local:test"
    provider._effective_route_name = "local"

    with pytest.raises(TypeError, match="Unsupported local cross-encoder runtime"):
        provider.score_pairs([pair])

def test_select_best_assessment_skips_missing_pairs():
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    best_pair, best_assessment = _select_best_assessment([pair], {})

    assert best_pair is None
    assert best_assessment is None

def test_score_requirement_match_without_candidate_pairs_uses_fallback_adjustment():
    requirement_match = _make_requirement_match(
        evidence_text="Built Python APIs",
        similarity=0.2,
        is_covered=False,
    )

    adjusted, verdict = _score_requirement_match(
        requirement_match=requirement_match,
        candidate_pairs=[],
        assessments_by_pair={},
        threshold=0.5,
        provider_route="local",
    )

    assert adjusted.is_covered is False
    assert verdict is None

def test_score_requirement_match_without_assessment_returns_unjudged_verdict():
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())

    adjusted, verdict = _score_requirement_match(
        requirement_match=requirement_match,
        candidate_pairs=[pair],
        assessments_by_pair={},
        threshold=0.5,
        provider_route="remote",
    )

    assert adjusted.is_covered is False
    assert verdict["verdict"] == "missing"
    assert verdict["provider_route"] == "remote"

def test_scored_requirement_verdict_includes_truncation_and_provider_route():
    requirement_match = _make_requirement_match()
    preliminary = _make_preliminary(requirement_matches=[requirement_match])
    pair = _serialize_pair(preliminary, requirement_match, requirement_match.evidence_candidates[0], config=ScorerConfig())
    assessment = PairAssessment(
        pair_id=pair.pair_id,
        requirement_id=pair.requirement_id,
        coverage_level="covered",
        semantic_score=0.91,
        confidence=0.88,
        reason="Evidence strongly matches the requirement.",
    )

    verdict = _scored_requirement_verdict(requirement_match, pair, assessment, "remote")

    assert verdict["provider_route"] == "remote"
    assert "truncation" in verdict

def test_mode_normalization_and_capability_defaults():
    config = ScorerConfig().semantic_fit
    config.deploy_allowed_modes = ["cross_encoder", "cross_encoder", "llm", "ignored"]
    config.baseline_allowed_modes = ["cross_encoder"]

    assert _normalize_modes(config.deploy_allowed_modes) == ["cross_encoder", "llm"]
    assert _default_effective_allowed(config) == ["cross_encoder"]

def test_preferred_capability_mode_ignores_invalid_payload():
    repo = MagicMock()
    repo.get_capability.return_value = MagicMock(enabled=True, value_json="bad")

    preferred = _preferred_capability_mode(repo, "user-1", ["cross_encoder", "llm"])

    assert preferred is None

def test_effective_fit_mode_maps_routes():
    assert _effective_fit_mode("local_heuristic", "heuristic-local") == "threshold"
    assert _effective_fit_mode("threshold", "threshold") == "threshold"
    assert _effective_fit_mode("llm", "llm") == "llm"
    assert _effective_fit_mode("remote", "flag_embedding:model") == "cross_encoder"

def test_fallback_adjusted_match_preserves_threshold_logic():
    requirement_match = _make_requirement_match(
        similarity=0.72,
        is_covered=True,
    )

    adjusted = _fallback_adjusted_match(requirement_match, 0.5)

    assert adjusted.is_covered is True
    assert adjusted.similarity >= 0.5

def test_missing_assessment_verdict_marks_unjudged_provider_route():
    requirement_match = _make_requirement_match()

    verdict = _missing_assessment_verdict(requirement_match, "remote")

    assert verdict["reason"] == "Requirement was not judged by the semantic scorer; preserved fallback classification."
    assert verdict["provider_route"] == "remote"

def test_llm_semantic_fit_with_no_serialized_pairs_uses_empty_summary():
    job = MagicMock()
    job.id = "job-empty"
    job.title = "Python Engineer"
    job.company = "Acme"
    job.description = "Backend role"
    requirement = _make_requirement_match(
        req_id="req-empty",
        evidence_text=None,
        similarity=0.0,
        is_covered=False,
    )
    preliminary = JobMatchPreliminary(
        job=job,
        job_similarity=0.4,
        requirement_matches=[],
        missing_requirements=[requirement],
        resume_fingerprint="fp-empty",
    )

    result = LLMSemanticFitScorer(FakeLLMService()).score(
        preliminary,
        fit_penalties=0.0,
        config=ScorerConfig(),
    )

    assert result.fit_components["semantic_fit_summary"] == "No recalled resume evidence supported the evaluated requirements."
    assert result.fit_components["semantic_fit_diagnostics"]["provider_route"] == "llm"


def test_score_text_pairs_heuristic_returns_nonzero_for_overlap():
    provider = LocalCrossEncoderProvider("heuristic-model", runtime="heuristic", allow_heuristic=True)
    scores = provider.score_text_pairs([("python backend", "Python FastAPI backend service")])
    assert scores[0] > 0.0


def test_score_text_pairs_heuristic_returns_zero_for_no_overlap():
    provider = LocalCrossEncoderProvider("heuristic-model", runtime="heuristic", allow_heuristic=True)
    scores = provider.score_text_pairs([("python", "Java enterprise application")])
    assert scores[0] == 0.0
