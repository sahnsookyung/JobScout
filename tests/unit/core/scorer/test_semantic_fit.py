"""Unit tests for semantic fit scoring contracts."""

import pytest
from unittest.mock import MagicMock

from core.config_loader import ScorerConfig
from core.llm.fake_service import FakeLLMService
from core.matcher import JobMatchPreliminary, RequirementMatchResult
from core.scorer.semantic_fit import (
    CrossEncoderSemanticFitScorer,
    FEATURE_ALLOWED_MODES,
    FEATURE_PREFERRED_MODE,
    LLMSemanticFitScorer,
    LocalCrossEncoderProvider,
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


def test_resolve_effective_fit_mode_uses_entitlement_preference_when_allowed():
    repo = MagicMock()
    repo.get_entitlement.side_effect = [
        MagicMock(enabled=True, value_json={"modes": ["cross_encoder", "llm"]}),
        MagicMock(enabled=True, value_json={"mode": "llm"}),
    ]
    config = ScorerConfig()
    config.semantic_fit.deploy_allowed_modes = ["cross_encoder", "llm"]

    resolved_mode, allowed = resolve_effective_fit_mode(repo, config, owner_id="user-1")

    assert resolved_mode == "llm"
    assert allowed == ["cross_encoder", "llm"]
    repo.get_entitlement.assert_any_call("user-1", FEATURE_ALLOWED_MODES)
    repo.get_entitlement.assert_any_call("user-1", FEATURE_PREFERRED_MODE)


def test_resolve_effective_fit_mode_ignores_invalid_entitlement_payloads():
    repo = MagicMock()
    repo.get_entitlement.side_effect = [
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
