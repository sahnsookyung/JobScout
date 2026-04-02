from unittest.mock import Mock, patch

from core.config_loader import PreferenceModelConfig
from core.llm.fake_service import FakeLLMService
from services.scorer_matcher.preference_semantics import (
    LLMPreferenceJudge,
    LLMPreferenceParser,
    LLMPreferenceSemanticReranker,
    PREFERENCE_PROFILE_VERSION,
    PreferenceAssessment,
    PreferenceJobPayload,
    PreferenceProfile,
    build_preference_judge,
    build_preference_llm,
    build_preference_parser,
    build_preference_semantic_reranker,
    serialize_job_for_preference,
    summarize_preference_profile,
    _chunk_jobs_for_budget,
    _payload_char_budget,
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

def test_preference_reranker_returns_structured_assessments():
    llm = Mock()
    llm.extract_structured_data.return_value = {
        "results": [
            {
                "job_id": "job-1",
                "preference_score": 0.81,
                "preference_confidence": 0.76,
                "preference_reason_codes": ["team_culture_match"],
                "preference_explanation": "Matches mentorship preferences.",
            }
        ]
    }
    reranker = LLMPreferenceSemanticReranker(llm, max_input_tokens=2048)
    profile = PreferenceProfile(raw_text="Mentorship", parser_confidence=0.8)

    results = reranker.rerank(
        profile,
        [PreferenceJobPayload(job_id="job-1", title="Backend Engineer")],
    )

    assert results == [
        PreferenceAssessment(
            job_id="job-1",
            preference_score=0.81,
            preference_confidence=0.76,
            preference_reason_codes=["team_culture_match"],
            preference_explanation="Matches mentorship preferences.",
        )
    ]

def test_preference_judge_returns_structured_assessments():
    llm = Mock()
    llm.extract_structured_data.return_value = {
        "results": [
            {
                "job_id": "job-1",
                "preference_score": 0.91,
                "preference_confidence": 0.82,
                "preference_reason_codes": ["tech_stack_match"],
                "preference_explanation": "Strong Python preference match.",
            }
        ]
    }
    judge = LLMPreferenceJudge(llm, max_input_tokens=2048)
    profile = PreferenceProfile(raw_text="Python", parser_confidence=0.8)

    results = judge.judge(
        profile,
        [PreferenceJobPayload(job_id="job-1", title="Python Engineer")],
    )

    assert results[0].preference_score == 0.91
    assert results[0].preference_reason_codes == ["tech_stack_match"]


def test_build_preference_llm_returns_none_when_disabled():
    assert build_preference_llm(_config(enabled=False)) is None


@patch("services.scorer_matcher.preference_semantics.build_preference_llm")
def test_build_preference_parser_wraps_llm(mock_build_llm):
    llm = Mock()
    mock_build_llm.return_value = llm

    parser = build_preference_parser(_config())

    assert isinstance(parser, LLMPreferenceParser)
    assert parser.llm is llm


@patch("services.scorer_matcher.preference_semantics.build_preference_llm")
def test_build_preference_semantic_reranker_wraps_llm(mock_build_llm):
    llm = Mock()
    mock_build_llm.return_value = llm

    reranker = build_preference_semantic_reranker(_config())

    assert isinstance(reranker, LLMPreferenceSemanticReranker)
    assert reranker.llm is llm
    assert reranker.max_input_tokens == 2048


@patch("services.scorer_matcher.preference_semantics.build_preference_llm")
def test_build_preference_judge_wraps_llm(mock_build_llm):
    llm = Mock()
    mock_build_llm.return_value = llm

    judge = build_preference_judge(_config())

    assert isinstance(judge, LLMPreferenceJudge)
    assert judge.llm is llm
    assert judge.max_input_tokens == 2048


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

def test_serialize_job_for_preference_prefers_canonical_summary():
    job = Mock(
        id="job-1",
        title="Backend Engineer",
        company="Acme",
        location_text="Remote",
        is_remote=True,
        work_from_home_type="remote",
        job_type="Full-time",
        canonical_job_summary="Build backend platforms",
        description="Long description",
        company_description="Mentorship-focused team",
        skills_raw="python, fastapi",
        requirements=[Mock(text="Build Python APIs"), Mock(text="Mentor junior engineers")],
        benefits=[Mock(text="Learning budget"), Mock(text="Flexible schedule")],
        raw_payload={"ai_job_summary": "ignored"},
    )

    payload = serialize_job_for_preference(job)

    assert payload.job_id == "job-1"
    assert payload.work_mode == "remote"
    assert payload.summary == "Build backend platforms"
    assert payload.skills == ["python", "fastapi"]
    assert payload.requirements == ["Build Python APIs", "Mentor junior engineers"]
    assert payload.benefits == ["Learning budget", "Flexible schedule"]


def test_chunk_jobs_for_budget_splits_large_shortlist():
    profile = PreferenceProfile(raw_text="Mentorship", parser_confidence=0.8)
    jobs = [
        PreferenceJobPayload(
            job_id=f"job-{index}",
            title=f"Backend Engineer {index}",
            summary="x" * 1800,
            requirements=["y" * 280 for _ in range(8)],
            benefits=["z" * 280 for _ in range(8)],
        )
        for index in range(3)
    ]

    chunks = _chunk_jobs_for_budget(
        profile,
        jobs,
        scorer_name="semantic_rerank",
        max_input_tokens=600,
    )

    assert len(chunks) >= 2
    assert sum(len(chunk) for chunk in chunks) == 3


def test_preference_reranker_truncates_oversized_profile_before_scoring():
    llm = Mock()
    llm.extract_structured_data.return_value = {"results": []}
    reranker = LLMPreferenceSemanticReranker(llm, max_input_tokens=220)
    profile = PreferenceProfile(
        raw_text="platform engineering " * 300,
        parser_confidence=0.8,
        work_style=[{"label": "remote-first collaboration " * 20, "weight": 0.8, "confidence": 0.8}],
        team_culture=[{"label": "mentorship " * 20, "weight": 0.9, "confidence": 0.9}],
    )

    reranker.rerank(
        profile,
        [PreferenceJobPayload(job_id="job-1", title="Platform Engineer", summary="Mentorship and growth")],
    )

    sent_payload = llm.extract_structured_data.call_args.args[0]
    assert len(sent_payload) <= _payload_char_budget(220)


def test_preference_reranker_truncates_single_oversized_job_to_budget():
    llm = Mock()
    llm.extract_structured_data.return_value = {"results": []}
    reranker = LLMPreferenceSemanticReranker(llm, max_input_tokens=220)
    profile = PreferenceProfile(raw_text="mentorship", parser_confidence=0.8)
    oversized_job = PreferenceJobPayload(
        job_id="job-1",
        title="Platform Engineer " * 20,
        company="Acme " * 20,
        location_text="Remote " * 20,
        summary="backend growth mentorship " * 300,
        company_description="team culture learning " * 200,
        skills=["python " * 20 for _ in range(20)],
        requirements=["mentorship growth ownership " * 40 for _ in range(20)],
        benefits=["learning budget flexibility " * 40 for _ in range(20)],
    )

    reranker.rerank(profile, [oversized_job])

    sent_payload = llm.extract_structured_data.call_args.args[0]
    assert len(sent_payload) <= _payload_char_budget(220)
