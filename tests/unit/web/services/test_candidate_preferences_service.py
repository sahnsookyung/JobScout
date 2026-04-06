from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.scorer_matcher.preference_semantics import PreferenceProfile
from web.backend.services.candidate_preferences_service import CandidatePreferencesService


def _config(allowed_modes=None, default_mode="semantic_rerank"):
    _valid = {"semantic_rerank", "llm_judge"}
    _modes = allowed_modes or ["semantic_rerank"]
    prefs = SimpleNamespace(
        default_mode=default_mode,
        allowed_modes=_modes,
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
    prefs.allowed_modes_normalized = lambda: (
        [m for m in dict.fromkeys(str(x).strip().lower() for x in prefs.allowed_modes) if m in _valid]
        or [prefs.default_mode]
    )
    return SimpleNamespace(preferences=prefs)


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
def test_update_preferences_stores_summary_without_blocking_on_parser(
    mock_get_config,
):
    mock_get_config.return_value = _config(allowed_modes=["semantic_rerank", "llm_judge"])
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

    with patch.object(service, "_parse_preference_profile", return_value=None):
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

    assert preferences.preference_profile is None
    assert preferences.soft_preference_summary == "Mentorship and modern backend teams"
    assert response["soft_preference_summary"] == "Mentorship and modern backend teams"

@patch("web.backend.services.candidate_preferences_service.get_config")
def test_update_preferences_persists_preference_profile_when_available(mock_get_config):
    mock_get_config.return_value = _config(allowed_modes=["semantic_rerank", "llm_judge"])
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
    profile = PreferenceProfile(
        raw_text="Mentorship and backend teams",
        parser_confidence=0.81,
        team_culture=[{"label": "Mentorship", "weight": 0.9, "confidence": 0.9}],
    )

    with patch.object(service, "_parse_preference_profile", return_value=profile):
        response = service.update_preferences(
            SimpleNamespace(id="user-1"),
            {
                "remote_mode": "any",
                "target_locations": [],
                "visa_sponsorship_required": False,
                "salary_min": None,
                "employment_types": [],
                "soft_preferences": "Mentorship and backend teams",
                "preference_mode": "semantic_rerank",
            },
        )

    assert preferences.preference_profile == profile.model_dump(mode="json")
    assert response["soft_preference_summary"] == "Mentorship"


@patch("web.backend.services.candidate_preferences_service.get_config")
def test_update_preferences_normalizes_lists_and_invalid_remote_mode(mock_get_config):
    mock_get_config.return_value = _config(allowed_modes=["semantic_rerank"])
    db = Mock()
    service = CandidatePreferencesService(db)
    preferences = SimpleNamespace(
        owner_id="user-1",
        remote_mode="remote",
        target_locations=["Existing"],
        visa_sponsorship_required=True,
        salary_min=120000,
        employment_types=["Contract"],
        soft_preferences="Existing",
        soft_preference_summary="Existing",
        preference_mode="semantic_rerank",
        preference_profile={"stale": True},
        revision=3,
    )
    service.repo.candidate_preferences.get_or_create_preferences = Mock(return_value=preferences)

    response = service.update_preferences(
        SimpleNamespace(id="user-1"),
        {
            "remote_mode": "unknown",
            "target_locations": [" Berlin ", "", "berlin", "Remote"],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [" Full-time ", "full-time", "Contract"],
            "soft_preferences": "   ",
            "preference_mode": "semantic_rerank",
        },
    )

    assert preferences.remote_mode == "any"
    assert preferences.target_locations == ["Berlin", "Remote"]
    assert preferences.employment_types == ["Full-time", "Contract"]
    assert preferences.soft_preferences == ""
    assert preferences.soft_preference_summary is None
    assert preferences.preference_profile is None
    assert preferences.revision == 4
    assert response["soft_preferences"] == ""
    assert response["soft_preference_summary"] is None


@patch("web.backend.services.candidate_preferences_service.get_config")
def test_get_preferences_falls_back_to_default_allowed_and_effective_modes(mock_get_config):
    mock_get_config.return_value = _config(allowed_modes=["invalid-mode"])
    db = Mock()
    service = CandidatePreferencesService(db)
    preferences = SimpleNamespace(
        owner_id="user-1",
        remote_mode="hybrid",
        target_locations=None,
        visa_sponsorship_required=False,
        salary_min=None,
        employment_types=None,
        soft_preferences=None,
        soft_preference_summary=None,
        preference_mode=None,
        preference_profile=None,
        revision=None,
    )
    service.repo.candidate_preferences.get_or_create_preferences = Mock(return_value=preferences)

    response = service.get_preferences(SimpleNamespace(id="user-1"))

    assert response["preference_mode"] == "semantic_rerank"
    assert response["allowed_preference_modes"] == ["semantic_rerank"]
    assert response["effective_preference_mode"] == "semantic_rerank"
    assert response["target_locations"] == []
    assert response["employment_types"] == []
    assert response["soft_preferences"] == ""
    assert response["revision"] == 0


@patch("web.backend.services.candidate_preferences_service.get_config")
def test_update_preferences_truncates_long_summary_and_normalizes_mode(mock_get_config):
    mock_get_config.return_value = _config(allowed_modes=["semantic_rerank", "llm_judge"])
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
    long_preferences = "Platform engineering and distributed systems " * 6

    response = service.update_preferences(
        SimpleNamespace(id="user-1"),
        {
            "remote_mode": "hybrid",
            "target_locations": [],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [],
            "soft_preferences": long_preferences,
            "preference_mode": "  LLM_JUDGE  ",
        },
    )

    assert preferences.preference_mode == "llm_judge"
    assert response["effective_preference_mode"] == "llm_judge"
    assert len(response["soft_preference_summary"]) == 160
    assert response["soft_preference_summary"].endswith("…")


@patch("web.backend.services.candidate_preferences_service.get_config")
def test_get_preferences_uses_allowed_mode_when_default_is_disallowed(mock_get_config):
    mock_get_config.return_value = _config(
        allowed_modes=["llm_judge"],
        default_mode="semantic_rerank",
    )
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

    response = service.get_preferences(SimpleNamespace(id="user-1"))

    assert response["allowed_preference_modes"] == ["llm_judge"]
    assert response["effective_preference_mode"] == "llm_judge"
