from unittest.mock import patch

import pytest

from core.llm.fake_service import (
    FAIL_EMBEDDING_MARKER,
    FAIL_EXTRACTION_MARKER,
    FakeLLMService,
    _fake_preference_profile_response,
    _fake_preference_rerank_response,
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

def test_fake_preference_profile_response_extracts_categories():
    result = _fake_preference_profile_response("Mentorship and modern backend teams")

    assert result["raw_text"] == "Mentorship and modern backend teams"
    assert result["team_culture"]
    assert result["tech_stack"]

def test_fake_preference_rerank_response_scores_matching_jobs():
    result = _fake_preference_rerank_response(
        {
            "profile": _fake_preference_profile_response("Python mentorship"),
            "jobs": [
                {
                    "job_id": "job-1",
                    "title": "Python Engineer",
                    "summary": "Python backend platform with mentorship",
                    "skills": ["python", "backend"],
                },
                {
                    "job_id": "job-2",
                    "title": "Java Engineer",
                    "summary": "Java services role",
                    "skills": ["java"],
                },
            ],
        },
        judge_mode=False,
    )

    assert result["results"][0]["preference_score"] > result["results"][1]["preference_score"]


def test_fake_preference_rerank_response_uses_requirements_and_benefits_in_haystack():
    result = _fake_preference_rerank_response(
        {
            "profile": _fake_preference_profile_response("mentorship growth"),
            "jobs": [
                {
                    "job_id": "job-1",
                    "title": "Backend Engineer",
                    "summary": "",
                    "company_description": "",
                    "skills": [],
                    "requirements": ["Mentorship culture and backend ownership"],
                    "benefits": ["Growth budget for learning"],
                }
            ],
        },
        judge_mode=False,
    )

    assert result["results"][0]["preference_score"] > 0.5
    assert "team_culture_match" in result["results"][0]["preference_reason_codes"]

def test_fake_service_extract_structured_data_supports_preference_schemas():
    service = FakeLLMService()

    profile = service.extract_structured_data(
        text="Python mentorship",
        schema_spec={"name": "preference_profile_schema"},
    )
    rerank = service.extract_structured_data(
        text='{"profile":{"raw_text":"Python mentorship","parse_version":"2026-04-01.v1","parser_confidence":0.8,"work_style":[],"team_culture":[{"label":"Mentorship","weight":0.9,"confidence":0.9}],"tech_stack":[{"label":"Python","weight":0.9,"confidence":0.9}],"mission_domain":[],"growth_preferences":[],"negative_preferences":[]},"jobs":[{"job_id":"job-1","title":"Python Engineer","company":"Acme","location_text":"Remote","work_mode":"remote","employment_type":"Full-time","summary":"Python backend platform with mentorship","company_description":"","skills":["python","backend"]}],"mode":"semantic_rerank"}',
        schema_spec={"name": "preference_semantic_rerank_v1"},
    )

    assert profile["raw_text"] == "Python mentorship"
    assert rerank["results"][0]["job_id"] == "job-1"


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
