"""Contracts for compact fit requests and untrusted model responses."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from core.config_loader import SemanticFitLlmConfig, load_config
from core.llm.provider_factory import runtime_llm_config_from_fit
from core.scorer.compact_fit import assess_compact_requirements


def pair(requirement: str, evidence: str, index: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        requirement_id=requirement, pair_id=f"{requirement}:{index}",
        fields={"requirement_text": "Python production development", "evidence_text": evidence},
    )


def judge(**kwargs):
    payload = json.loads(kwargs["text"])
    return {"judgments": [{
        "requirement_id": r["id"], "evidence_id": r["candidates"][-1]["id"],
        "coverage_level": "covered", "semantic_score": 0.95,
        "confidence": 0.9, "reason": "Explicit supporting evidence.",
    } for r in reversed(payload["requirements"])]}


def test_deduplicates_evidence_and_maps_reordered_judgments_to_original_ids():
    provider = Mock()
    provider.extract_structured_data.side_effect = judge
    pairs = [pair("r1", "Same evidence"), pair("r2", "Same evidence"), pair("r2", "Best evidence", 1)]
    result = assess_compact_requirements(provider, pairs, SemanticFitLlmConfig())
    payload = json.loads(provider.extract_structured_data.call_args.kwargs["text"])
    assert [[c["id"] for c in r["candidates"]] for r in payload["requirements"]] == [[0], [0, 1]]
    assert {j["pair_id"] for j in result["pair_judgments"]} == {"r1:0", "r2:1"}
    assert "r1" not in provider.extract_structured_data.call_args.kwargs["text"]


def test_splits_by_requirement_limit_and_preserves_every_requirement():
    provider = Mock()
    provider.extract_structured_data.side_effect = judge
    result = assess_compact_requirements(provider, [pair(str(i), "Python") for i in range(19)],
                                         SemanticFitLlmConfig(compact_max_requirements=8))
    assert provider.extract_structured_data.call_count == 3
    assert {j["requirement_id"] for j in result["pair_judgments"]} == {str(i) for i in range(19)}


def test_splits_by_utf8_byte_limit_without_truncating_evidence():
    provider = Mock()
    provider.extract_structured_data.side_effect = judge
    pairs = [pair(str(i), "日本語" * 300 + str(i)) for i in range(3)]
    result = assess_compact_requirements(provider, pairs,
                                         SemanticFitLlmConfig(compact_max_request_bytes=6000))
    assert len(result["pair_judgments"]) == 3
    assert provider.extract_structured_data.call_count == 3
    assert json.loads(provider.extract_structured_data.call_args.kwargs["text"])["requirements"][0]["candidates"][0]["text"] == pairs[-1].fields["evidence_text"]


def test_oversize_requirement_fails_before_any_provider_call():
    provider = Mock()
    with pytest.raises(ValueError, match="byte limit"):
        assess_compact_requirements(provider, [pair("first", "Short"), pair("second", "長" * 10000)],
                                    SemanticFitLlmConfig())
    provider.extract_structured_data.assert_not_called()


@pytest.mark.parametrize("fault", ["missing", "duplicate", "unknown", "foreign_evidence", "nonfinite", "extra", "bool_id"])
def test_rejects_incomplete_or_invalid_judgments(fault):
    def invalid(**kwargs):
        result = judge(**kwargs)
        rows = result["judgments"]
        if fault == "missing":
            rows.pop()
        elif fault == "duplicate":
            rows[1] = dict(rows[0])
        elif fault == "unknown":
            rows[0]["requirement_id"] = 99
        elif fault == "foreign_evidence":
            rows[0]["evidence_id"] = 99
        elif fault == "nonfinite":
            rows[0]["semantic_score"] = float("nan")
        elif fault == "extra":
            rows[0]["untrusted"] = "unexpected"
        else:
            rows[0]["requirement_id"] = True
        return result
    provider = Mock()
    provider.extract_structured_data.side_effect = invalid
    with pytest.raises(ValueError):
        assess_compact_requirements(provider, [pair("r1", "Python"), pair("r2", "SQL")], SemanticFitLlmConfig())


def test_empty_input_never_calls_provider():
    provider = Mock()
    assert assess_compact_requirements(provider, [], SemanticFitLlmConfig())["pair_judgments"] == []
    provider.extract_structured_data.assert_not_called()


def test_fit_runtime_and_environment_forward_explicit_options(monkeypatch):
    for key, value in {
        "FIT_LLM_ENABLED": "true", "FIT_LLM_COMPACT_REQUIREMENTS": "true",
        "FIT_LLM_ENABLE_THINKING": "false", "FIT_LLM_TIMEOUT_SECONDS": "60",
        "FIT_LLM_MAX_OUTPUT_TOKENS": "2048", "FIT_LLM_STRUCTURED_OUTPUT_MODE": "json_object",
        "FIT_LLM_BASE_URL": "https://example.test/v1", "FIT_LLM_API_KEY": "fixture",
        "FIT_SEMANTIC_DEPLOY_ALLOWED_MODES": '["llm"]',
        "FIT_SEMANTIC_BASELINE_ALLOWED_MODES": '["llm"]',
        "FIT_SEMANTIC_DEFAULT_MODE": "llm", "FIT_SEMANTIC_THRESHOLD_FALLBACK_ENABLED": "false",
        "MATCHER_BATCH_SIZE": "20",
    }.items():
        monkeypatch.setenv(key, value)
    config = load_config()
    fit = config.matching.scorer.semantic_fit.llm
    assert fit.enabled and fit.compact_requirements
    assert config.matching.scorer.semantic_fit.deploy_allowed_modes == ["llm"]
    assert not config.matching.scorer.semantic_fit.threshold_fallback_enabled
    assert config.matching.matcher.batch_size == 20
    runtime = runtime_llm_config_from_fit(fit)
    assert runtime.enable_thinking is False
    assert runtime.structured_output_mode == "json_object"
    assert runtime.timeout_seconds == 60
    assert runtime.max_output_tokens == 2048


def test_compact_judgment_selects_recalled_evidence_and_updates_final_score():
    from core.config_loader import ScorerConfig
    from core.matcher.models import JobMatchPreliminary, RequirementEvidenceCandidate, RequirementMatchResult
    from core.scorer.semantic_fit import LLMSemanticFitScorer
    from core.scorer.service import ScoringService

    config = ScorerConfig(penalty_missing_required=100)
    config.semantic_fit.llm.compact_requirements = True
    config.semantic_fit.llm.model = "fixture-model"
    config.semantic_fit.threshold_fallback_enabled = False
    provider = Mock()
    provider.extract_structured_data.side_effect = judge
    wrong = SimpleNamespace(text="Built Java systems", source_section="experience")
    right = SimpleNamespace(text="Built Python APIs in production", source_section="experience")
    requirement = SimpleNamespace(id="python", text="Production Python APIs", req_type="required", weight=1.0)
    match = RequirementMatchResult(requirement, wrong, 0.4, False, evidence_candidates=[
        RequirementEvidenceCandidate(wrong, 0.4, 1), RequirementEvidenceCandidate(right, 0.35, 2),
    ])
    item = JobMatchPreliminary(SimpleNamespace(id="job", title="Engineer", company="Fixture",
                                              canonical_job_summary="Untrusted job context", description=""),
                               0.8, [], [match], "fixture")
    scorer = ScoringService(None, config, semantic_fit_scorer=LLMSemanticFitScorer(provider))
    result = scorer.score_preliminary_match(item)
    assert result.matched_requirements[0].evidence is right
    assert result.penalties == 0
    assert result.fit_score > 80
    assert result.fit_scorer["version"].endswith("-compact-1")
    assert result.fit_components["semantic_fit_diagnostics"]["provider_id"] == "fixture-model"
    assert "Untrusted job context" not in provider.extract_structured_data.call_args.kwargs["text"]


@pytest.mark.parametrize("value", ['"llm"', '["invalid"]', 'not-json', '["llm", 1]'])
def test_invalid_allowed_modes_are_rejected(value, monkeypatch):
    monkeypatch.setenv("FIT_SEMANTIC_DEPLOY_ALLOWED_MODES", value)
    with pytest.raises(ValueError):
        load_config()
