from unittest.mock import Mock, patch

from core.config_loader import PreferenceModelConfig
from core.llm.fake_service import FakeLLMService
from services.scorer_matcher.preference_semantics import (
    LLMPreferenceParser,
    PREFERENCE_PROFILE_VERSION,
    PreferenceProfile,
    build_preference_llm,
    summarize_preference_profile,
)


def _config(**overrides) -> PreferenceModelConfig:
    base = {
        "enabled": True,
        "base_url": "https://preferences.example/v1",
        "api_key": "key",
        "api_secret": "secret",
        "headers": {"X-Test": "1"},
        "model": "gpt-preferences",
        "temperature": 0.2,
        "embedding_model": "text-embedding-3-small",
        "embedding_dimensions": 1024,
        "embedding_base_url": "https://embeddings.example/v1",
        "embedding_api_key": "embedding-key",
        "embedding_api_secret": "embedding-secret",
        "embedding_headers": {"X-Embedding": "1"},
    }
    base.update(overrides)
    return PreferenceModelConfig(**base)


def test_preference_parser_skips_blank_input():
    llm = Mock()
    parser = LLMPreferenceParser(llm)

    assert parser.parse("   ") is None
    llm.extract_structured_data.assert_not_called()


def test_preference_parser_returns_none_for_non_dict_payload():
    llm = Mock()
    llm.extract_structured_data.return_value = ["not", "a", "dict"]
    parser = LLMPreferenceParser(llm)

    assert parser.parse("Mentorship and strong backend teams") is None


def test_preference_parser_backfills_missing_metadata():
    llm = Mock()
    llm.extract_structured_data.return_value = {
        "raw_text": "",
        "parse_version": "",
        "parser_confidence": 0.72,
        "work_style": [
            {
                "label": "Mentorship",
                "weight": 0.8,
                "confidence": 0.9,
            }
        ],
    }
    parser = LLMPreferenceParser(llm)

    profile = parser.parse("  Mentorship and room to grow  ")

    assert profile == PreferenceProfile(
        raw_text="Mentorship and room to grow",
        parse_version=PREFERENCE_PROFILE_VERSION,
        parser_confidence=0.72,
        work_style=[
            {
                "label": "Mentorship",
                "weight": 0.8,
                "confidence": 0.9,
            }
        ],
    )


def test_build_preference_llm_returns_none_when_disabled():
    assert build_preference_llm(_config(enabled=False)) is None


@patch("services.scorer_matcher.preference_semantics.build_llm_provider")
def test_build_preference_llm_uses_shared_provider_factory(mock_build):
    mock_build.return_value = FakeLLMService(embedding_dimensions=1024)

    llm = build_preference_llm(_config())

    mock_build.assert_called_once()
    assert isinstance(llm, FakeLLMService)


@patch("services.scorer_matcher.preference_semantics.logger")
def test_build_preference_llm_logs_and_returns_none_without_model(mock_logger):
    llm = build_preference_llm(_config(model=None))

    assert llm is None
    mock_logger.info.assert_called_once()


@patch("services.scorer_matcher.preference_semantics.build_llm_provider")
def test_build_preference_llm_constructs_runtime_provider(mock_build):
    sentinel = object()
    mock_build.return_value = sentinel
    config = _config()

    llm = build_preference_llm(config)

    assert llm is sentinel
    mock_build.assert_called_once()


def test_summarize_preference_profile_prefers_unique_labels():
    profile = PreferenceProfile(
        raw_text="Mentorship, backend growth, mission-driven products",
        parser_confidence=0.81,
        work_style=[
            {"label": "Mentorship", "weight": 0.9, "confidence": 0.9},
            {"label": "Remote-first", "weight": 0.7, "confidence": 0.8},
        ],
        team_culture=[
            {"label": "Mentorship", "weight": 0.5, "confidence": 0.6},
            {"label": "High trust", "weight": 0.8, "confidence": 0.7},
        ],
        tech_stack=[
            {"label": "Python", "weight": 0.9, "confidence": 0.9},
        ],
        mission_domain=[
            {"label": "Climate", "weight": 0.7, "confidence": 0.7},
        ],
    )

    assert summarize_preference_profile(profile, profile.raw_text) == (
        "Mentorship, Remote-first, High trust, Python"
    )


def test_summarize_preference_profile_truncates_raw_text_when_needed():
    summary = summarize_preference_profile(
        None,
        "platform engineering and distributed systems " * 6,
        max_length=40,
    )

    assert len(summary) == 40
    assert summary.endswith("…")
