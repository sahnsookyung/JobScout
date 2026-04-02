from unittest.mock import patch

import pytest

from core.llm.fake_service import (
    FAIL_EMBEDDING_MARKER,
    FAIL_EXTRACTION_MARKER,
    FakeLLMService,
    _fake_semantic_fit_response,
    _meaningful_overlap,
    _tokenize,
    _unit_normalize,
)


def test_tokenize_and_overlap_ignore_generic_tokens():
    assert "python" in _tokenize("Python backend APIs")
    overlap = _meaningful_overlap(
        "5 years experience with Python backend development",
        "Built Python services with hands on API ownership",
    )
    assert overlap == {"python"}


def test_unit_normalize_handles_zero_vector():
    vector = [0.0, 0.0, 0.0]

    normalized = _unit_normalize(vector)

    assert normalized[-1] == 1.0


def test_fake_semantic_fit_response_handles_keyword_mismatch_and_empty_evidence():
    payload = {
        "pairs": [
            {
                "pair_id": "p1",
                "requirement_id": "r1",
                "requirement_text": "Strong Java programming experience",
                "evidence_text": "Built Python backend APIs",
                "original_similarity": 0.8,
                "req_type": "required",
            },
            {
                "pair_id": "p2",
                "requirement_id": "r2",
                "requirement_text": "AWS production experience",
                "evidence_text": "",
                "original_similarity": 0.0,
                "req_type": "required",
            },
        ]
    }

    response = _fake_semantic_fit_response(payload)

    assert response["summary"] == "Covered 0 of 2 required requirements."
    assert response["pair_judgments"][0]["coverage_level"] == "missing"
    assert response["pair_judgments"][1]["reason"] == "No matching resume evidence was available for this requirement."


def test_fake_service_extract_structured_data_supports_semantic_fit_schema():
    service = FakeLLMService()
    payload = {
        "pairs": [
            {
                "pair_id": "p1",
                "requirement_id": "r1",
                "requirement_text": "Python APIs",
                "evidence_text": "Built Python APIs",
                "original_similarity": 0.6,
                "req_type": "required",
            }
        ]
    }

    result = service.extract_structured_data(
        text='{"pairs":[{"pair_id":"p1","requirement_id":"r1","requirement_text":"Python APIs","evidence_text":"Built Python APIs","original_similarity":0.6,"req_type":"required"}]}',
        schema_spec={"name": "semantic_fit_pairs_v1"},
    )

    assert result["summary"] == "Covered 1 of 1 required requirements."


def test_fake_service_extract_requirements_data_sets_role_defaults():
    service = FakeLLMService()

    result = service.extract_requirements_data("Senior remote Python FastAPI AWS backend role")

    assert result["seniority_level"] == "Senior"
    assert result["remote_policy"] == "Remote (Global)"
    assert "python" in result["tech_stack"]
    assert result["requirements"]


def test_fake_service_extract_resume_data_requires_fixture_shape():
    service = FakeLLMService()

    with pytest.raises(ValueError, match="structured resume fixture"):
        service.extract_resume_data('{"profile":{}}')


def test_fake_service_failure_modes_raise_for_extraction_and_embedding():
    service = FakeLLMService()

    with pytest.raises(ValueError, match="Fake extraction failure"):
        service.extract_structured_data(FAIL_EXTRACTION_MARKER, schema_spec={})

    with pytest.raises(ValueError, match="Fake embedding failure"):
        service.generate_embedding(FAIL_EMBEDDING_MARKER)

    with patch.dict("os.environ", {"JOBSCOUT_FAKE_AI_FAILURE_MODE": "embedding"}):
        with pytest.raises(ValueError, match="Fake embedding failure"):
            service.generate_embedding("safe text")


def test_fake_service_generates_batch_embeddings():
    service = FakeLLMService(embedding_dimensions=32)

    embeddings = service.generate_embeddings_batch(["python", "java"])

    assert len(embeddings) == 2
    assert len(embeddings[0]) == 32
