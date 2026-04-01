from types import SimpleNamespace
from unittest.mock import Mock, patch

from web.backend.services.candidate_preferences_service import CandidatePreferencesService


def _config(allowed_modes=None, default_mode="semantic_rerank"):
    return SimpleNamespace(
        preferences=SimpleNamespace(
            default_mode=default_mode,
            allowed_modes=allowed_modes or ["semantic_rerank"],
            parser=SimpleNamespace(
                enabled=False,
                model=None,
                base_url=None,
                api_key=None,
                api_secret=None,
                headers=None,
                temperature=0.0,
                embedding_model="text-embedding-3-small",
                embedding_dimensions=1024,
                embedding_base_url=None,
                embedding_api_key=None,
                embedding_api_secret=None,
                embedding_headers=None,
            ),
        )
    )


@patch("web.backend.services.candidate_preferences_service.get_config")
def test_update_preferences_falls_back_to_default_mode_when_disallowed(mock_get_config):
    mock_get_config.return_value = _config(allowed_modes=["semantic_rerank"])
    db = Mock()
    service = CandidatePreferencesService(db)

    preferences = SimpleNamespace(
        owner_id="user-1",
        remote_mode="any",
        target_locations=[],
        visa_sponsorship_required=False,
        salary_min=None,
        employment_types=[],
        soft_preferences="",
        soft_preference_summary=None,
        preference_mode="semantic_rerank",
        preference_profile=None,
        revision=0,
    )
    service.repo.candidate_preferences.get_or_create_preferences = Mock(return_value=preferences)

    response = service.update_preferences(
        SimpleNamespace(id="user-1"),
        {
            "remote_mode": "remote",
            "target_locations": ["Berlin"],
            "visa_sponsorship_required": False,
            "salary_min": 100000,
            "employment_types": ["Full-time"],
            "soft_preferences": "Mentorship",
            "preference_mode": "llm_judge",
        },
    )

    assert preferences.preference_mode == "semantic_rerank"
    assert response["effective_preference_mode"] == "semantic_rerank"
    assert response["allowed_preference_modes"] == ["semantic_rerank"]


@patch("web.backend.services.candidate_preferences_service.get_config")
@patch("web.backend.services.candidate_preferences_service.LLMPreferenceParser")
@patch("web.backend.services.candidate_preferences_service.build_preference_llm")
def test_update_preferences_stores_parsed_profile_and_summary(
    mock_build_preference_llm,
    mock_parser_class,
    mock_get_config,
):
    mock_get_config.return_value = _config(allowed_modes=["semantic_rerank", "llm_judge"])
    mock_build_preference_llm.return_value = object()
    parsed_profile = SimpleNamespace(
        model_dump=lambda: {
            "raw_text": "Mentorship and modern backend teams",
            "parse_version": "2026-04-01.v1",
            "parser_confidence": 0.8,
            "work_style": [{"label": "mentorship", "weight": 0.9, "confidence": 0.9}],
            "team_culture": [],
            "tech_stack": [{"label": "backend", "weight": 0.7, "confidence": 0.8}],
            "mission_domain": [],
            "growth_preferences": [],
            "negative_preferences": [],
        },
        work_style=[SimpleNamespace(label="mentorship")],
        team_culture=[],
        tech_stack=[SimpleNamespace(label="backend")],
        mission_domain=[],
        growth_preferences=[],
        negative_preferences=[],
    )
    mock_parser_class.return_value.parse.return_value = parsed_profile

    db = Mock()
    service = CandidatePreferencesService(db)
    preferences = SimpleNamespace(
        owner_id="user-1",
        remote_mode="any",
        target_locations=[],
        visa_sponsorship_required=False,
        salary_min=None,
        employment_types=[],
        soft_preferences="",
        soft_preference_summary=None,
        preference_mode="semantic_rerank",
        preference_profile=None,
        revision=0,
    )
    service.repo.candidate_preferences.get_or_create_preferences = Mock(return_value=preferences)

    response = service.update_preferences(
        SimpleNamespace(id="user-1"),
        {
            "remote_mode": "any",
            "target_locations": [],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [],
            "soft_preferences": "Mentorship and modern backend teams",
            "preference_mode": "semantic_rerank",
        },
    )

    assert preferences.preference_profile is not None
    assert preferences.soft_preference_summary == "mentorship, backend"
    assert response["soft_preference_summary"] == "mentorship, backend"
