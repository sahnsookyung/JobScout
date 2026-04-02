"""Unit tests for semantic fit scoring contracts."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from core.config_loader import ScorerConfig
from core.llm.fake_service import FakeLLMService
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
    _build_serialized_pairs,
    _pair_assessment_from_heuristic,
    _select_best_assessment,
    _serialize_pair,
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
    assert result.fit_confidence == 0.86
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
    )

    provider._load_flag_embedding_runtime = MagicMock(side_effect=ImportError("no flag embedding"))
    provider._load_sentence_transformers_runtime = MagicMock(side_effect=ImportError("no sentence transformers"))

    result = provider._load_model()

    assert result is False
    assert provider.provider_id == "heuristic-local"
    assert provider.effective_route_name == "local_heuristic"


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

def test_cross_encoder_local_policy_can_fall_through_to_remote_provider():
    class FakeRemoteProvider:
        route_name = "remote"

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

    assert result.fit_components["effective_fit_mode"] == "cross_encoder"
    assert result.fit_components["provider_route"] == "remote"
