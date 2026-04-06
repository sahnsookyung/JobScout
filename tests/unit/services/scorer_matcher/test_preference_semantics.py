import pytest
from unittest.mock import Mock, patch

from core.config_loader import PreferenceCrossEncoderConfig, PreferenceModelConfig, PreferencesConfig
from tests.mocks.fake_service import FakeLLMService
from services.scorer_matcher.preference_semantics import (
    CrossEncoderPreferenceReranker,
    LLMPreferenceJudge,
    LLMPreferenceParser,
    LLMPreferenceSemanticReranker,
    PREFERENCE_PROFILE_VERSION,
    PreferenceAssessment,
    PreferenceJobPayload,
    PreferenceProfile,
    WeightedPreference,
    build_preference_judge,
    build_preference_llm,
    build_preference_parser,
    build_preference_semantic_reranker,
    serialize_job_for_preference,
    summarize_preference_profile,
    _chunk_jobs_for_budget,
    _fit_single_job_payload_to_budget,
    job_work_mode,
    _normalize_job_text_list,
    _normalize_skills,
    _payload_char_budget,
    _truncate_preference_profile,
    _truncate_text,
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

    config = PreferencesConfig(reranker="llm", semantic_reranker=_config())
    reranker = build_preference_semantic_reranker(config)

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


# ---------------------------------------------------------------------------
# _normalize_skills
# ---------------------------------------------------------------------------

def test_normalize_skills_with_list_input():
    assert _normalize_skills(["Python", "FastAPI", ""]) == ["Python", "FastAPI"]


def test_normalize_skills_with_falsy_input():
    assert _normalize_skills(None) == []
    assert _normalize_skills("") == []


# ---------------------------------------------------------------------------
# _truncate_text edge cases
# ---------------------------------------------------------------------------

def test_truncate_text_max_chars_zero_or_one():
    assert _truncate_text("hello", 0) == ""
    assert _truncate_text("hello", 1) == "h"


# ---------------------------------------------------------------------------
# _normalize_job_text_list
# ---------------------------------------------------------------------------

def test_normalize_job_text_list_with_object_having_text_attr():
    class FakeReq:
        def __init__(self, text):
            self.text = text

    items = [FakeReq("Python experience"), FakeReq("FastAPI knowledge")]
    result = _normalize_job_text_list(items)
    assert "Python experience" in result


def test_normalize_job_text_list_with_dict_items():
    items = [{"text": "Remote work"}, {"label": "Flexible hours"}, {"name": "Equity"}]
    result = _normalize_job_text_list(items)
    assert "Remote work" in result
    assert "Flexible hours" in result


def test_normalize_job_text_list_respects_max_items():
    items = [{"text": f"item {i}"} for i in range(20)]
    result = _normalize_job_text_list(items, max_items=3)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# job_work_mode
# ---------------------------------------------------------------------------

def testjob_work_mode_returns_hybrid():
    class FakeJob:
        is_remote = False
        work_from_home_type = "hybrid"
        location_text = ""

    assert job_work_mode(FakeJob()) == "hybrid"


def testjob_work_mode_returns_hybrid_from_location():
    class FakeJob:
        is_remote = False
        work_from_home_type = ""
        location_text = "New York / Hybrid"

    assert job_work_mode(FakeJob()) == "hybrid"


# ---------------------------------------------------------------------------
# _truncate_preference_profile raises when too large after all truncation
# ---------------------------------------------------------------------------

def test_truncate_preference_profile_raises_when_still_too_large():
    """Profile that still exceeds budget after all truncation raises ValueError."""
    profile = PreferenceProfile(raw_text="mentorship", parser_confidence=0.9)
    # Patch the budget function to return a tiny value so even a minimal profile fails
    with patch(
        "services.scorer_matcher.preference_semantics._payload_char_budget",
        return_value=1,
    ):
        with pytest.raises(ValueError, match="max_input_tokens"):
            _truncate_preference_profile(profile, max_input_tokens=10)


# ---------------------------------------------------------------------------
# _fit_single_job_payload_to_budget
# ---------------------------------------------------------------------------

def test_fit_single_job_early_return_when_already_fits():
    """Job that already fits budget is returned without shrinking."""
    profile = PreferenceProfile(raw_text="mentorship", parser_confidence=0.8)
    job = PreferenceJobPayload(job_id="j1", title="SWE", summary="short summary")
    result = _fit_single_job_payload_to_budget(
        profile, job, scorer_name="test", budget_chars=100_000
    )
    assert result.job_id == "j1"
    assert result.summary == "short summary"


def test_fit_single_job_raises_when_cannot_fit():
    """Job that cannot be shrunk to budget raises ValueError."""
    profile = PreferenceProfile(raw_text="x" * 5_000, parser_confidence=0.9)
    job = PreferenceJobPayload(job_id="j1", title="SWE", summary="short")
    with pytest.raises(ValueError, match="max_input_tokens"):
        _fit_single_job_payload_to_budget(
            profile, job, scorer_name="test", budget_chars=10
        )


# ---------------------------------------------------------------------------
# _chunk_jobs_for_budget — empty input
# ---------------------------------------------------------------------------

def test_chunk_jobs_for_budget_empty_returns_empty():
    profile = PreferenceProfile(raw_text="mentorship", parser_confidence=0.8)
    assert _chunk_jobs_for_budget(profile, [], scorer_name="test", max_input_tokens=4096) == []


# ---------------------------------------------------------------------------
# _BaseLLMPreferenceScorer._score edge cases
# ---------------------------------------------------------------------------

def test_preference_reranker_score_empty_jobs_returns_empty():
    llm = Mock()
    reranker = LLMPreferenceSemanticReranker(llm, max_input_tokens=4096)
    profile = PreferenceProfile(raw_text="mentorship", parser_confidence=0.8)
    result = reranker.rerank(profile, [])
    assert result == []
    llm.extract_structured_data.assert_not_called()


def test_preference_reranker_skips_non_dict_llm_response():
    """When LLM returns a non-dict, the chunk result is silently skipped."""
    llm = Mock()
    llm.extract_structured_data.return_value = "not a dict"
    reranker = LLMPreferenceSemanticReranker(llm, max_input_tokens=4096)
    profile = PreferenceProfile(raw_text="mentorship", parser_confidence=0.8)
    result = reranker.rerank(
        profile,
        [PreferenceJobPayload(job_id="j1", title="SWE", summary="mentorship role")],
    )
    assert result == []


# ---------------------------------------------------------------------------
# build_preference_semantic_reranker / build_preference_judge disabled path
# ---------------------------------------------------------------------------

def test_build_preference_semantic_reranker_returns_none_when_disabled():
    config = PreferencesConfig(reranker="llm", semantic_reranker=_config(enabled=False))
    assert build_preference_semantic_reranker(config) is None


def test_build_preference_judge_returns_none_when_disabled():
    config = _config(enabled=False)
    assert build_preference_judge(config) is None


# ---------------------------------------------------------------------------
# CrossEncoderPreferenceReranker
# ---------------------------------------------------------------------------

def test_cross_encoder_reranker_returns_empty_for_no_jobs():
    ce = Mock()
    reranker = CrossEncoderPreferenceReranker(ce)
    profile = PreferenceProfile(raw_text="python", parser_confidence=0.8)
    assert reranker.rerank(profile, []) == []
    ce.score_text_pairs.assert_not_called()


def test_cross_encoder_reranker_zero_score_when_no_preference_labels():
    ce = Mock()
    reranker = CrossEncoderPreferenceReranker(ce)
    # Profile has no category labels → no pairs to score
    profile = PreferenceProfile(raw_text="no preferences", parser_confidence=0.5)
    result = reranker.rerank(
        profile,
        [PreferenceJobPayload(job_id="j1", title="Python Engineer", summary="python role")],
    )
    assert result[0].preference_score == 0.0
    ce.score_text_pairs.assert_not_called()


def test_cross_encoder_reranker_scores_job_from_cross_encoder_output():
    ce = Mock()
    # title segment only; score 0.8 × weight 1.0 → weighted = 0.8
    ce.score_text_pairs.return_value = [0.8]
    reranker = CrossEncoderPreferenceReranker(ce)
    profile = PreferenceProfile(
        raw_text="python",
        parser_confidence=0.8,
        tech_stack=[WeightedPreference(label="python", weight=1.0, confidence=0.9)],
    )
    job = PreferenceJobPayload(job_id="j1", title="Python Backend")
    result = reranker.rerank(profile, [job])
    assert result[0].job_id == "j1"
    assert result[0].preference_score > 0.0


def test_cross_encoder_reranker_negative_conflict_zeroes_score():
    ce = Mock()
    reranker = CrossEncoderPreferenceReranker(ce)
    profile = PreferenceProfile(
        raw_text="avoid salesforce",
        parser_confidence=0.8,
        negative_preferences=[WeightedPreference(label="salesforce", weight=0.9, confidence=0.9)],
    )
    job = PreferenceJobPayload(job_id="j1", title="Salesforce Developer", summary="Salesforce CRM")
    result = reranker.rerank(profile, [job])
    assert result[0].preference_score == 0.0
    assert "negative_preference_conflict" in result[0].preference_reason_codes
    ce.score_text_pairs.assert_not_called()


def test_cross_encoder_reranker_emits_category_match_codes():
    ce = Mock()
    # Two segments for one label: title (0.9) + summary (0.1)
    ce.score_text_pairs.return_value = [0.9, 0.1]
    reranker = CrossEncoderPreferenceReranker(ce)
    profile = PreferenceProfile(
        raw_text="python",
        parser_confidence=0.8,
        tech_stack=[WeightedPreference(label="python", weight=1.0, confidence=0.9)],
    )
    job = PreferenceJobPayload(job_id="j1", title="Python Backend", summary="java role")
    result = reranker.rerank(profile, [job])
    assert "tech_stack_match" in result[0].preference_reason_codes


def test_cross_encoder_reranker_all_zero_scores_gives_empty_reason_codes():
    ce = Mock()
    ce.score_text_pairs.return_value = [0.0, 0.0]
    reranker = CrossEncoderPreferenceReranker(ce)
    profile = PreferenceProfile(
        raw_text="python",
        parser_confidence=0.8,
        tech_stack=[WeightedPreference(label="python", weight=1.0, confidence=0.9)],
    )
    job = PreferenceJobPayload(job_id="j1", title="Python Backend", summary="role")
    result = reranker.rerank(profile, [job])
    assert result[0].preference_score == 0.0
    assert result[0].preference_reason_codes == []


def test_cross_encoder_reranker_any_positive_score_emits_reason_code():
    ce = Mock()
    ce.score_text_pairs.return_value = [0.01]  # tiny but positive
    reranker = CrossEncoderPreferenceReranker(ce)
    profile = PreferenceProfile(
        raw_text="python",
        parser_confidence=0.8,
        tech_stack=[WeightedPreference(label="python", weight=1.0, confidence=0.9)],
    )
    result = reranker.rerank(
        profile, [PreferenceJobPayload(job_id="j1", title="Python Backend")]
    )
    assert "tech_stack_match" in result[0].preference_reason_codes


def test_build_preference_semantic_reranker_routes_to_cross_encoder():
    config = PreferencesConfig(
        reranker="cross_encoder",
        cross_encoder=PreferenceCrossEncoderConfig(enabled=True),
    )
    reranker = build_preference_semantic_reranker(config)
    assert isinstance(reranker, CrossEncoderPreferenceReranker)


def test_build_preference_semantic_reranker_returns_none_for_disabled_cross_encoder():
    config = PreferencesConfig(
        reranker="cross_encoder",
        cross_encoder=PreferenceCrossEncoderConfig(enabled=False),
    )
    assert build_preference_semantic_reranker(config) is None
