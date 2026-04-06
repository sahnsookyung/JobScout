from unittest.mock import patch

import pytest

from tests.mocks.fake_service import (
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
    {
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


def test_fake_semantic_fit_partial_coverage():
    """Similarity >= 0.45, no keyword dimension tokens, no text overlap → partial."""
    payload = {
        "pairs": [
            {
                "pair_id": "p1",
                "requirement_id": "r1",
                "requirement_text": "Agile sprint coordination",
                "evidence_text": "Stakeholder presentations quarterly",
                "original_similarity": 0.5,
                "req_type": "required",
            },
        ]
    }
    result = _fake_semantic_fit_response(payload)
    assert result["pair_judgments"][0]["coverage_level"] == "partial"


def test_fake_semantic_fit_missing_coverage_low_similarity():
    """Evidence present but low similarity and no overlap → missing."""
    payload = {
        "pairs": [
            {
                "pair_id": "p1",
                "requirement_id": "r1",
                "requirement_text": "Kubernetes cluster management",
                "evidence_text": "Built frontend UI components",
                "original_similarity": 0.2,
                "req_type": "required",
            },
        ]
    }
    result = _fake_semantic_fit_response(payload)
    assert result["pair_judgments"][0]["coverage_level"] == "missing"
    assert result["pair_judgments"][0]["semantic_score"] == 0.0


def test_fake_preference_profile_deduplicates_signals():
    """Repeated token for the same signal is added only once."""
    result = _fake_preference_profile_response("python python python")
    assert len(result["tech_stack"]) == 1


def test_fake_preference_profile_negative_phrases():
    """Negative phrases are captured in negative_preferences."""
    result = _fake_preference_profile_response(
        "I want to avoid consulting and no startup chaos"
    )
    labels = [item["label"] for item in result["negative_preferences"]]
    assert "avoid consulting" in labels
    assert "no startup chaos" in labels


def test_fake_preference_rerank_negative_preference_conflicts():
    """Job matching a negative preference gets penalized and 'Conflicts' explanation."""
    profile = {
        "work_style": [],
        "team_culture": [],
        "tech_stack": [],
        "mission_domain": [],
        "growth_preferences": [],
        "negative_preferences": [
            {"label": "avoid salesforce", "weight": 0.9, "confidence": 0.9}
        ],
    }
    result = _fake_preference_rerank_response(
        {
            "profile": profile,
            "jobs": [
                {
                    "job_id": "job-1",
                    "title": "Salesforce Developer",
                    "summary": "Salesforce CRM development role",
                }
            ],
        },
        judge_mode=False,
    )
    entry = result["results"][0]
    assert entry["preference_score"] == 0.0
    assert "Conflicts" in entry["preference_explanation"]


def test_fake_preference_rerank_judge_mode_boosts_score():
    """judge_mode=True adds a small score and confidence bonus when score > 0."""
    profile = _fake_preference_profile_response("python backend")
    result_normal = _fake_preference_rerank_response(
        {
            "profile": profile,
            "jobs": [{"job_id": "j1", "title": "Python Backend", "summary": "Python services"}],
        },
        judge_mode=False,
    )
    result_judge = _fake_preference_rerank_response(
        {
            "profile": profile,
            "jobs": [{"job_id": "j1", "title": "Python Backend", "summary": "Python services"}],
        },
        judge_mode=True,
    )
    score_normal = result_normal["results"][0]["preference_score"]
    score_judge = result_judge["results"][0]["preference_score"]
    if score_normal > 0:
        assert score_judge >= score_normal


def test_fake_service_extract_structured_data_preference_llm_judge():
    """preference_llm_judge_v1 schema routes to rerank with judge_mode=True."""
    service = FakeLLMService()
    profile = _fake_preference_profile_response("python mentorship")
    payload_json = (
        '{"profile":%s,"jobs":[{"job_id":"j1","title":"Python Engineer",'
        '"summary":"Python mentorship platform"}],"mode":"llm_judge"}'
        % __import__("json").dumps(profile)
    )
    result = service.extract_structured_data(payload_json, {"name": "preference_llm_judge_v1"})
    assert "results" in result
    assert result["results"][0]["job_id"] == "j1"


def test_fake_service_extract_structured_data_non_dict_raises_for_fit():
    """Non-dict JSON raises ValueError for semantic_fit_pairs_v1."""
    service = FakeLLMService()
    with pytest.raises(ValueError, match="JSON payload object"):
        service.extract_structured_data("[1, 2, 3]", {"name": "semantic_fit_pairs_v1"})


def test_fake_service_extract_structured_data_non_dict_raises_for_rerank():
    """Non-dict JSON raises ValueError for preference_semantic_rerank_v1."""
    service = FakeLLMService()
    with pytest.raises(ValueError, match="JSON payload object"):
        service.extract_structured_data("[1, 2, 3]", {"name": "preference_semantic_rerank_v1"})


def test_fake_service_extract_structured_data_non_dict_raises_for_judge():
    """Non-dict JSON raises ValueError for preference_llm_judge_v1."""
    service = FakeLLMService()
    with pytest.raises(ValueError, match="JSON payload object"):
        service.extract_structured_data("[1, 2, 3]", {"name": "preference_llm_judge_v1"})


def test_fake_service_extract_structured_data_fallback_returns_dict():
    """Unknown schema with valid dict JSON returns it directly."""
    service = FakeLLMService()
    result = service.extract_structured_data(
        '{"foo": "bar"}', {"name": "unknown_schema"}
    )
    assert result == {"foo": "bar"}


def test_fake_service_extract_structured_data_fallback_returns_empty_for_non_dict_json():
    """Unknown schema with non-dict JSON (array) returns empty dict."""
    service = FakeLLMService()
    result = service.extract_structured_data("[1, 2, 3]", {"name": "unknown_schema"})
    assert result == {}


def test_fake_service_extract_resume_data_raises_on_non_json():
    """Non-JSON text raises ValueError with 'JSON fixture input' message."""
    service = FakeLLMService()
    with pytest.raises(ValueError, match="JSON fixture input"):
        service.extract_resume_data("this is not json")


def test_fake_service_extract_resume_data_returns_valid_fixture():
    """Valid fixture with profile and extraction keys is returned as-is."""
    service = FakeLLMService()
    fixture = {"profile": {"experience": []}, "extraction": {"confidence": 0.9, "warnings": []}}
    result = service.extract_resume_data(__import__("json").dumps(fixture))
    assert result == fixture


def test_fake_service_generate_embedding_small_dimensions_breaks_hash_loop():
    """When embedding_dimensions <= hash_offset, the hash loop exits immediately."""
    # _KEYWORD_DIMENSIONS has 15 entries (indices 0-14), so hash_offset = 15
    service = FakeLLMService(embedding_dimensions=15)
    vector = service.generate_embedding("some unique token text here")
    assert len(vector) == 15


def test_fake_service_unload_model_is_noop():
    """unload_model on FakeLLMService logs and does not raise."""
    service = FakeLLMService()
    service.unload_model("some-model")  # must not raise
