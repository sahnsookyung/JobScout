"""Unit tests for candidate preference helper logic."""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from core.config_loader import PreferencesConfig
from services.scorer_matcher.candidate_preferences import (
    _job_matches_employment_types,
    _job_matches_locations,
    _job_matches_remote_mode,
    _job_meets_salary_floor,
    _allowed_preference_modes,
    _job_supports_visa,
    job_work_mode,
    _matches_candidate_preferences,
    _resolve_requested_mode,
    _resolve_preference_profile,
    _safe_mode,
    _stored_preference_profile,
    _apply_assessments,
    apply_candidate_preference_filters,
    apply_preference_semantic_reranking,
    load_candidate_preferences,
)
from services.scorer_matcher.preference_semantics import PreferenceProfile
from services.scorer_matcher.preference_semantics import PreferenceAssessment


def _job(**overrides):
    defaults = {
        "id": "job-1",
        "title": "Backend Engineer",
        "company": "Acme",
        "is_remote": None,
        "work_from_home_type": None,
        "location_text": "Berlin, Germany",
        "salary_min": None,
        "salary_max": None,
        "job_type": None,
        "description": "",
        "company_description": "",
        "skills_raw": "python, backend",
        "canonical_job_summary": "",
        "raw_payload": {},
    }
    defaults.update(overrides)
    defaults.setdefault("description_hash", f"hash-{defaults['id']}")
    defaults.setdefault("content_hash", f"hash-{defaults['id']}")
    return SimpleNamespace(**defaults)


def _preliminary(job=None):
    return SimpleNamespace(job=job or _job())


def _scored_match(
    job,
    *,
    fit_score=80.0,
    job_similarity=0.8,
    fit_components=None,
    preference_components=None,
):
    return SimpleNamespace(
        job=job,
        fit_score=fit_score,
        job_similarity=job_similarity,
        preference_score=None,
        fit_components=fit_components or {},
        preference_components=preference_components or {},
    )


class _OfferingsRepo:
    def __init__(self, profiles):
        self.profiles = profiles
        self.loaded_job_ids = []

    def get_job_offerings_profiles_by_job_ids(self, job_ids):
        self.loaded_job_ids = [str(job_id) for job_id in job_ids]
        return self.profiles


class _FailingOfferingsRepo(_OfferingsRepo):
    def __init__(self):
        super().__init__({})

    def get_job_offerings_profiles_by_job_ids(self, job_ids):
        self.loaded_job_ids = [str(job_id) for job_id in job_ids]
        raise RuntimeError("db unavailable")


def _offerings_row(job_id, confidence=0.8):
    return SimpleNamespace(
        job_post_id=job_id,
        profile_json={
            "schema_version": 1,
            "work_arrangement": "remote",
            "benefits_perks": [
                {
                    "label": "Learning budget",
                    "evidence": "Annual learning budget",
                    "confidence": confidence,
                }
            ],
            "confidence": confidence,
        },
        profile_schema_version=1,
        source_description_hash=f"hash-{job_id}",
    )


def _preferences_config() -> PreferencesConfig:
    return PreferencesConfig.model_validate(
        {
            "default_mode": "semantic_rerank",
            "allowed_modes": ["semantic_rerank", "llm_judge"],
            "parser": {"enabled": False, "model": None},
            "semantic_reranker": {"enabled": False, "model": None},
            "llm_judge": {"enabled": False, "model": None},
        }
    )


class TestLoadCandidatePreferences:
    def test_returns_none_without_owner_id(self):
        repo = MagicMock()

        assert load_candidate_preferences(repo, None) is None
        repo.candidate_preferences.get_preferences.assert_not_called()

    def test_returns_none_when_no_saved_preferences_exist(self):
        repo = MagicMock()
        repo.candidate_preferences.get_preferences.return_value = None

        assert load_candidate_preferences(repo, "user-1") is None

    def test_normalizes_preferences_snapshot(self):
        repo = MagicMock()
        repo.candidate_preferences.get_preferences.return_value = SimpleNamespace(
            remote_mode="  ",
            target_locations=["Berlin"],
            visa_sponsorship_required=True,
            salary_min=120000,
            employment_types=["Full-time"],
            soft_preferences="Mentorship",
            soft_preference_summary="Mentorship",
            preference_mode="semantic_rerank",
            preference_rerank_top_n=7,
            preference_profile={"raw_text": "Mentorship", "parser_confidence": 0.7},
            revision=7,
        )

        snapshot = load_candidate_preferences(repo, "user-1")

        assert snapshot == {
            "remote_mode": "any",
            "target_locations": ["Berlin"],
            "visa_sponsorship_required": True,
            "salary_min": 120000,
            "employment_types": ["Full-time"],
            "soft_preferences": "Mentorship",
            "soft_preference_summary": "Mentorship",
            "preference_mode": "semantic_rerank",
            "preference_rerank_top_n": 7,
            "preference_profile": {"raw_text": "Mentorship", "parser_confidence": 0.7},
            "revision": 7,
        }


class TestHardFilterHelpers:
    def testjob_work_mode_detects_remote_hybrid_and_onsite(self):
        assert job_work_mode(_job(is_remote=True)) == "remote"
        assert job_work_mode(_job(work_from_home_type="Hybrid")) == "hybrid"
        assert job_work_mode(_job(location_text="Hybrid - London")) == "hybrid"
        assert job_work_mode(_job(location_text="Austin")) == "onsite"

    def test_job_matches_remote_mode_covers_supported_modes(self):
        hybrid_job = _job(work_from_home_type="Hybrid")

        assert _job_matches_remote_mode(hybrid_job, "any") is True
        assert _job_matches_remote_mode(_job(is_remote=True), "remote") is True
        assert _job_matches_remote_mode(_job(location_text="Austin"), "remote") is False
        assert _job_matches_remote_mode(hybrid_job, "hybrid") is True
        assert _job_matches_remote_mode(hybrid_job, "onsite") is True
        assert _job_matches_remote_mode(hybrid_job, "unexpected") is True

    def test_job_matches_locations_handles_empty_missing_and_substring_matches(self):
        assert _job_matches_locations(_job(), []) is True
        assert _job_matches_locations(_job(location_text="", is_remote=True), ["Remote only"]) is True
        assert _job_matches_locations(_job(location_text="Berlin, Germany"), ["Berlin"]) is True
        assert _job_matches_locations(_job(location_text="Berlin, Germany"), ["Tokyo"]) is False

    def test_job_meets_salary_floor_handles_unknown_invalid_and_mismatch_cases(self):
        assert _job_meets_salary_floor(_job(), None) is True
        assert _job_meets_salary_floor(_job(), 120000) is True
        assert _job_meets_salary_floor(_job(salary_max=150000), "bad") is True
        assert _job_meets_salary_floor(_job(salary_max=90000), 120000) is False

    def test_job_matches_employment_types_handles_missing_and_mismatch_cases(self):
        assert _job_matches_employment_types(_job(), []) is True
        assert _job_matches_employment_types(_job(job_type=None), ["Full-time"]) is True
        assert _job_matches_employment_types(_job(job_type="Full-time"), ["Full-time"]) is True
        assert _job_matches_employment_types(_job(job_type="Contract"), ["Full-time"]) is False

    def test_job_supports_visa_uses_flags_and_text_hints(self):
        assert _job_supports_visa(_job(raw_payload={"visa_sponsorship_available": True})) is True
        assert _job_supports_visa(_job(raw_payload={"visa_sponsorship_available": False})) is False
        assert _job_supports_visa(_job(description="We offer visa sponsorship.")) is True
        assert _job_supports_visa(_job(description="No visa sponsorship available.")) is False
        assert _job_supports_visa(_job(raw_payload="not-a-dict")) is False

    def test_matches_candidate_preferences_rejects_each_failed_branch(self):
        preferences = {
            "remote_mode": "remote",
            "target_locations": ["Berlin"],
            "visa_sponsorship_required": True,
            "salary_min": 120000,
            "employment_types": ["Full-time"],
            "soft_preferences": "",
            "revision": 1,
        }

        assert _matches_candidate_preferences(_preliminary(_job(location_text="Austin")), preferences) is False
        assert _matches_candidate_preferences(
            _preliminary(_job(is_remote=True, location_text="Berlin")),
            preferences,
        ) is False
        assert _matches_candidate_preferences(
            _preliminary(
                _job(
                    is_remote=True,
                    location_text="Berlin",
                    description="Visa sponsorship available.",
                    salary_max=90000,
                )
            ),
            preferences,
        ) is False
        assert _matches_candidate_preferences(
            _preliminary(
                _job(
                    is_remote=True,
                    location_text="Berlin",
                    description="Visa sponsorship available.",
                    salary_max=130000,
                    job_type="Contract",
                )
            ),
            preferences,
        ) is False

    def test_apply_candidate_preference_filters_returns_original_matches_without_preferences(self):
        matches = [_preliminary(_job()), _preliminary(_job(location_text="Tokyo"))]

        assert apply_candidate_preference_filters(matches, None) == matches


class TestStoredPreferenceProfile:
    def test_returns_none_when_no_profile_key(self):
        assert _stored_preference_profile({}) is None

    def test_returns_none_on_validation_error(self):
        # raw_profile present but invalid — model_validate raises
        assert _stored_preference_profile({"preference_profile": {"bad": "data", "parser_confidence": "not-a-float"}}) is None

    def test_returns_profile_on_valid_data(self):
        profile = _stored_preference_profile(
            {"preference_profile": {"raw_text": "Python", "parser_confidence": 0.9}}
        )
        assert profile is not None
        assert profile.raw_text == "Python"


class TestResolvePreferenceProfile:
    def test_returns_none_when_soft_preferences_empty(self):
        config = _preferences_config()
        result = _resolve_preference_profile({"soft_preferences": ""}, config)
        assert result is None

    def test_returns_none_when_parser_is_none(self):
        config = _preferences_config()
        with patch(
            "services.scorer_matcher.candidate_preferences.build_preference_parser",
            return_value=None,
        ):
            result = _resolve_preference_profile({"soft_preferences": "Python"}, config)
        assert result is None

    def test_returns_none_when_parser_raises(self):
        config = _preferences_config()
        mock_parser = Mock()
        mock_parser.parse.side_effect = RuntimeError("parse error")
        with patch(
            "services.scorer_matcher.candidate_preferences.build_preference_parser",
            return_value=mock_parser,
        ):
            result = _resolve_preference_profile({"soft_preferences": "Python"}, config)
        assert result is None


class TestApplyAssessments:
    def test_missing_assessment_leaves_match_unscored(self):
        match = _scored_match(_job(id="job-99"), fit_components={})
        result = _apply_assessments(
            [match],
            [],
            requested_mode="semantic_rerank",
            effective_mode="semantic_rerank",
        )
        assert result[0].preference_components["preference_reason_codes"] == ["invalid_llm_output"]
        assert result[0].preference_score is None


class TestPreferenceSemanticReranking:
    @patch("services.scorer_matcher.candidate_preferences.build_preference_parser")
    def test_falls_back_to_fit_only_when_profile_unavailable(self, mock_build_parser):
        mock_build_parser.return_value = None
        matches = [_scored_match(_job(id="job-1"))]

        reranked = apply_preference_semantic_reranking(
            matches,
            {
                "soft_preferences": "Mentorship and backend teams",
                "preference_mode": "semantic_rerank",
                "preference_profile": None,
            },
            config=_preferences_config(),
        )

        assert reranked[0].preference_components["preference_mode_used"] == "fit_only_fallback"
        assert reranked[0].preference_components["preference_mode_effective"] == "semantic_rerank"
        assert reranked[0].preference_components["preference_fallback_reason"] == "preference_scorer_unavailable"

    @patch("services.scorer_matcher.candidate_preferences.build_preference_semantic_reranker")
    def test_semantic_reranker_stores_scores_preserves_input_order(
        self,
        mock_build_reranker,
    ):
        """_apply_assessments stores preference_score on each match; does NOT sort."""
        mock_build_reranker.return_value = Mock(
            rerank=Mock(
                return_value=[
                    PreferenceAssessment(
                        job_id="job-2",
                        preference_score=95,
                        preference_confidence=0.9,
                        preference_reason_codes=["team_culture_match"],
                        preference_explanation="Matches mentorship preferences.",
                    ),
                    PreferenceAssessment(
                        job_id="job-1",
                        preference_score=15,
                        preference_confidence=0.5,
                        preference_reason_codes=["no_preference_signal"],
                        preference_explanation="Weak preference signal.",
                    ),
                ]
            )
        )
        config = _preferences_config().model_copy(
            update={
                "semantic_reranker": _preferences_config().parser.model_copy(
                    update={"enabled": True, "model": "fake"}
                )
            }
        )
        profile = PreferenceProfile(
            raw_text="Mentorship and backend teams",
            parser_confidence=0.8,
            team_culture=[{"label": "Mentorship", "weight": 0.9, "confidence": 0.9}],
        )
        # Input order: job-2 first, job-1 second
        low_fit_high_pref = _scored_match(_job(id="job-2", title="Mentorship Team"), fit_score=84.0)
        high_fit_low_pref = _scored_match(_job(id="job-1", title="Backend Engineer"), fit_score=86.0)

        reranked = apply_preference_semantic_reranking(
            [low_fit_high_pref, high_fit_low_pref],
            {
                "soft_preferences": "Mentorship and backend teams",
                "preference_mode": "semantic_rerank",
                "preference_profile": profile.model_dump(mode="json"),
            },
            config=config,
        )

        # Input order preserved — no sort performed
        assert reranked[0].job.id == "job-2"
        assert reranked[1].job.id == "job-1"
        # Scores written to both matches
        assert reranked[0].preference_score == pytest.approx(95)
        assert reranked[1].preference_score == pytest.approx(15)
        assert reranked[0].preference_components["preference_mode_used"] == "semantic_rerank"

    @patch("services.scorer_matcher.candidate_preferences.build_preference_semantic_reranker")
    def test_top_n_window_uses_cached_offerings_and_marks_outside_window(
        self,
        mock_build_reranker,
    ):
        mock_build_reranker.return_value = Mock(
            rerank=Mock(
                return_value=[
                    PreferenceAssessment(
                        job_id="job-2",
                        preference_score=90,
                        preference_confidence=0.8,
                        preference_reason_codes=["benefits_perks_match"],
                        preference_explanation="Learning budget matches.",
                    )
                ]
            )
        )
        config = _preferences_config().model_copy(
            update={
                "semantic_reranker": _preferences_config().parser.model_copy(
                    update={
                        "enabled": True,
                        "model": "fake",
                        "top_n_default": 1,
                        "top_n_min": 1,
                        "top_n_max": 5,
                    }
                )
            }
        )
        profile = PreferenceProfile(raw_text="Learning budget", parser_confidence=0.8)
        outside = _scored_match(_job(id="job-1"), fit_score=80.0, job_similarity=0.7)
        high_fit = _scored_match(_job(id="job-2"), fit_score=90.0, job_similarity=0.6)
        repo = _OfferingsRepo({"job-2": _offerings_row("job-2")})

        result = apply_preference_semantic_reranking(
            [outside, high_fit],
            {
                "soft_preferences": "Learning budget",
                "preference_mode": "semantic_rerank",
                "preference_profile": profile.model_dump(mode="json"),
                "preference_rerank_top_n": 1,
                "revision": 3,
            },
            config=config,
            repo=repo,
        )

        assert repo.loaded_job_ids == ["job-2"]
        assert result[0].preference_score is None
        assert result[0].preference_components["preference_status"] == "outside_preference_window"
        assert result[1].preference_score == pytest.approx(90)
        assert result[1].preference_components["preference_rerank_top_n"] == 1
        assert result[1].preference_components["offerings_source_description_hash"] == "hash-job-2"

    def test_missing_offerings_marks_window_match_waiting(self):
        config = _preferences_config()
        profile = PreferenceProfile(raw_text="Learning budget", parser_confidence=0.8)
        match = _scored_match(_job(id="job-1"), fit_score=90.0)

        result = apply_preference_semantic_reranking(
            [match],
            {
                "soft_preferences": "Learning budget",
                "preference_mode": "semantic_rerank",
                "preference_profile": profile.model_dump(mode="json"),
                "preference_rerank_top_n": 1,
            },
            config=config,
            repo=_OfferingsRepo({}),
        )

        assert result.status.reason == "job_offerings_unavailable"
        assert result[0].preference_score is None
        assert result[0].preference_components["preference_status"] == "missing_job_offerings"

    def test_stale_offerings_marks_window_match_waiting(self):
        config = _preferences_config()
        profile = PreferenceProfile(raw_text="Learning budget", parser_confidence=0.8)
        match = _scored_match(
            _job(id="job-1", description_hash="new-description-hash"),
            fit_score=90.0,
        )

        result = apply_preference_semantic_reranking(
            [match],
            {
                "soft_preferences": "Learning budget",
                "preference_mode": "semantic_rerank",
                "preference_profile": profile.model_dump(mode="json"),
                "preference_rerank_top_n": 1,
            },
            config=config,
            repo=_OfferingsRepo({"job-1": _offerings_row("job-1")}),
        )

        assert result.status.reason == "job_offerings_unavailable"
        assert result[0].preference_score is None
        assert result[0].preference_components["preference_status"] == "missing_job_offerings"

    def test_offerings_load_failure_is_scorer_failed_not_waiting(self):
        config = _preferences_config()
        profile = PreferenceProfile(raw_text="Learning budget", parser_confidence=0.8)
        match = _scored_match(_job(id="job-1"), fit_score=90.0)

        result = apply_preference_semantic_reranking(
            [match],
            {
                "soft_preferences": "Learning budget",
                "preference_mode": "semantic_rerank",
                "preference_profile": profile.model_dump(mode="json"),
                "preference_rerank_top_n": 1,
            },
            config=config,
            repo=_FailingOfferingsRepo(),
        )

        assert result.status.reason == "preference_scorer_failed"
        assert result[0].preference_score is None
        assert result[0].preference_components["preference_status"] == "preference_scorer_failed"
        assert (
            result[0].preference_components["preference_fallback_reason"]
            == "preference_scorer_failed"
        )

    @patch("services.scorer_matcher.candidate_preferences.build_preference_semantic_reranker")
    def test_empty_llm_output_marks_invalid_instead_of_zero_scores(self, mock_build_reranker):
        mock_build_reranker.return_value = Mock(rerank=Mock(return_value=[]))
        config = _preferences_config().model_copy(
            update={
                "semantic_reranker": _preferences_config().parser.model_copy(
                    update={"enabled": True, "model": "fake"}
                )
            }
        )
        profile = PreferenceProfile(raw_text="Learning budget", parser_confidence=0.8)
        match = _scored_match(_job(id="job-1"), fit_score=90.0)

        result = apply_preference_semantic_reranking(
            [match],
            {
                "soft_preferences": "Learning budget",
                "preference_mode": "semantic_rerank",
                "preference_profile": profile.model_dump(mode="json"),
            },
            config=config,
            repo=_OfferingsRepo({"job-1": _offerings_row("job-1")}),
        )

        assert result.status.reason == "invalid_llm_output"
        assert result[0].preference_score is None
        assert result[0].preference_components["preference_status"] == "invalid_llm_output"

    @patch("services.scorer_matcher.candidate_preferences.build_preference_judge")
    def test_llm_judge_mode_uses_judge_builder(self, mock_build_judge):
        mock_build_judge.return_value = Mock(
            judge=Mock(
                return_value=[
                    PreferenceAssessment(
                        job_id="job-1",
                        preference_score=80,
                        preference_confidence=0.88,
                        preference_reason_codes=["tech_stack_match"],
                        preference_explanation="Strong Python preference match.",
                    )
                ]
            )
        )
        config = _preferences_config().model_copy(
            update={
                "llm_judge": _preferences_config().parser.model_copy(
                    update={"enabled": True, "model": "fake"}
                )
            }
        )
        profile = PreferenceProfile(
            raw_text="Python backend mentorship",
            parser_confidence=0.8,
            tech_stack=[{"label": "Python", "weight": 0.9, "confidence": 0.9}],
        )

        reranked = apply_preference_semantic_reranking(
            [_scored_match(_job(id="job-1", title="Python Engineer"))],
            {
                "soft_preferences": "Python backend mentorship",
                "preference_mode": "llm_judge",
                "preference_profile": profile.model_dump(mode="json"),
            },
            config=config,
        )

        assert reranked[0].preference_components["preference_mode_used"] == "llm_judge"
        assert reranked[0].preference_score == pytest.approx(80)

    def test_disallowed_requested_mode_resolves_to_allowed_mode_before_matching(self):
        config = _preferences_config().model_copy(update={"allowed_modes": ["semantic_rerank"]})
        requested_mode, effective_mode = _resolve_requested_mode("llm_judge", config)

        assert requested_mode == "llm_judge"
        assert effective_mode == "semantic_rerank"
        assert _allowed_preference_modes(config) == ["semantic_rerank"]

    @patch("services.scorer_matcher.candidate_preferences.build_preference_semantic_reranker")
    def test_disallowed_requested_mode_uses_allowed_reranker_instead_of_falling_back(
        self,
        mock_build_reranker,
    ):
        mock_build_reranker.return_value = Mock(
            rerank=Mock(
                return_value=[
                    PreferenceAssessment(
                        job_id="job-1",
                        preference_score=70,
                        preference_confidence=0.8,
                        preference_reason_codes=["work_style_match"],
                        preference_explanation="Good mentorship signal.",
                    )
                ]
            )
        )
        config = _preferences_config().model_copy(
            update={
                "allowed_modes": ["semantic_rerank"],
                "semantic_reranker": _preferences_config().parser.model_copy(
                    update={"enabled": True, "model": "fake"}
                ),
            }
        )
        profile = PreferenceProfile(raw_text="Mentorship", parser_confidence=0.8)

        reranked = apply_preference_semantic_reranking(
            [_scored_match(_job(id="job-1"))],
            {
                "soft_preferences": "Mentorship",
                "preference_mode": "llm_judge",
                "preference_profile": profile.model_dump(mode="json"),
            },
            config=config,
        )

        assert reranked[0].preference_components["preference_mode_requested"] == "llm_judge"
        assert reranked[0].preference_components["preference_mode_used"] == "semantic_rerank"
        assert reranked[0].preference_components["preference_mode_effective"] == "semantic_rerank"
        assert "preference_fallback_reason" not in reranked[0].preference_components

    def test_reranker_none_falls_back_to_fit_only(self):
        config = _preferences_config()
        match = _scored_match(_job(), fit_components={})
        profile = PreferenceProfile(raw_text="Python", parser_confidence=0.9)
        with patch(
            "services.scorer_matcher.candidate_preferences._resolve_preference_profile",
            return_value=profile,
        ), patch(
            "services.scorer_matcher.candidate_preferences.build_preference_semantic_reranker",
            return_value=None,
        ):
            result = apply_preference_semantic_reranking(
                [match],
                {"soft_preferences": "Python", "preference_mode": "semantic_rerank"},
                config=config,
            )
        assert result[0].preference_components["preference_mode_used"] == "fit_only_fallback"
        assert result[0].preference_components["preference_fallback_reason"] == "preference_scorer_unavailable"

    def test_reranking_exception_falls_back_to_fit_only(self):
        config = _preferences_config()
        match = _scored_match(_job(), fit_components={})
        profile = PreferenceProfile(raw_text="Python", parser_confidence=0.9)
        with patch(
            "services.scorer_matcher.candidate_preferences._resolve_preference_profile",
            return_value=profile,
        ), patch(
            "services.scorer_matcher.candidate_preferences.build_preference_semantic_reranker",
        ) as mock_build:
            mock_build.return_value.rerank.side_effect = RuntimeError("boom")
            result = apply_preference_semantic_reranking(
                [match],
                {"soft_preferences": "Python", "preference_mode": "semantic_rerank"},
                config=config,
            )
        assert result[0].preference_components["preference_mode_used"] == "fit_only_fallback"
        assert result[0].preference_components["preference_fallback_reason"] == "preference_scorer_failed"
        assert result.status.reason == "preference_scorer_failed"

    def test_empty_soft_preferences_returns_matches_unchanged(self):
        config = _preferences_config()
        match = _scored_match(_job(), fit_components={})
        result = apply_preference_semantic_reranking(
            [match],
            {"soft_preferences": "   ", "preference_mode": "semantic_rerank"},
            config=config,
        )
        assert result[0].fit_components == {}
        assert result[0].preference_components == {}

    def test_apply_assessments_stores_scores_on_all_matches_with_equal_scores(self):
        """_apply_assessments writes preference_score to every match; no sorting."""
        assessments = [
            PreferenceAssessment(
                job_id="job-1", preference_score=70, preference_confidence=0.8,
                preference_reason_codes=["tech_match"], preference_explanation="ok",
            ),
            PreferenceAssessment(
                job_id="job-2", preference_score=70, preference_confidence=0.8,
                preference_reason_codes=["tech_match"], preference_explanation="ok",
            ),
        ]
        # Input order: job-2, job-1
        m1 = _scored_match(_job(id="job-2"), fit_score=82.0, job_similarity=0.6)
        m2 = _scored_match(_job(id="job-1"), fit_score=82.0, job_similarity=0.6)

        result = _apply_assessments(
            [m1, m2], assessments,
            requested_mode="semantic_rerank", effective_mode="semantic_rerank",
        )

        # Order unchanged
        assert [m.job.id for m in result] == ["job-2", "job-1"]
        # Both scores written
        assert result[0].preference_score == pytest.approx(70)
        assert result[1].preference_score == pytest.approx(70)

    def test_apply_assessments_exposes_full_preference_signal_independently_of_fit(self):
        match = _scored_match(_job(id="job-1"), fit_components={"core": 0.8})
        assessment = PreferenceAssessment(
            job_id="job-1",
            preference_score=83,
            preference_confidence=0.74,
            preference_reason_codes=["tech_stack_match", "team_culture_match"],
            preference_explanation="Strong alignment with Python and mentorship preferences.",
        )

        result = _apply_assessments(
            [match],
            [assessment],
            requested_mode="semantic_rerank",
            effective_mode="semantic_rerank",
        )

        assert result[0].fit_components == {"core": 0.8}
        assert result[0].preference_score == pytest.approx(83)
        assert result[0].preference_components == {
            "preference_confidence": 0.74,
            "preference_reason_codes": ["tech_stack_match", "team_culture_match"],
            "preference_explanation": "Strong alignment with Python and mentorship preferences.",
            "preference_mode_requested": "semantic_rerank",
            "preference_mode_effective": "semantic_rerank",
            "preference_mode_used": "semantic_rerank",
            "preference_status": "applied",
        }


class TestPreferenceStatusDataclass:
    """The PreferenceStatus.to_dict path is the public contract used by the
    pipeline summary log + the API badge — its shape needs explicit pinning."""

    def test_minimal_payload_only_includes_applied_flag(self):
        from services.scorer_matcher.candidate_preferences import PreferenceStatus
        assert PreferenceStatus(applied=True).to_dict() == {"applied": True}

    def test_full_payload_includes_all_present_fields(self):
        from services.scorer_matcher.candidate_preferences import PreferenceStatus
        status = PreferenceStatus(
            applied=False,
            reason="runtime_error:ValueError",
            requested_mode="llm_judge",
            effective_mode="semantic_rerank",
        )
        assert status.to_dict() == {
            "applied": False,
            "reason": "runtime_error:ValueError",
            "requested_mode": "llm_judge",
            "effective_mode": "semantic_rerank",
        }

    def test_empty_string_reason_is_omitted(self):
        from services.scorer_matcher.candidate_preferences import PreferenceStatus
        status = PreferenceStatus(applied=False, reason="")
        assert status.to_dict() == {"applied": False}


class TestPreferenceRerankResultProtocol:
    """Other call-sites still iterate/index/len the result. Lock that behavior."""

    def test_iterates_over_matches(self):
        from services.scorer_matcher.candidate_preferences import (
            PreferenceRerankResult, PreferenceStatus,
        )
        m1, m2 = object(), object()
        result = PreferenceRerankResult(
            matches=[m1, m2], status=PreferenceStatus(applied=True),
        )
        assert list(result) == [m1, m2]

    def test_supports_index_and_len(self):
        from services.scorer_matcher.candidate_preferences import (
            PreferenceRerankResult, PreferenceStatus,
        )
        result = PreferenceRerankResult(
            matches=["a", "b", "c"], status=PreferenceStatus(applied=False),
        )
        assert len(result) == 3
        assert result[1] == "b"


class TestResolveRequestedMode:
    def test_unknown_mode_falls_back_to_default(self):
        cfg = PreferencesConfig(default_mode="semantic_rerank")
        requested, effective = _resolve_requested_mode("garbage", cfg)
        assert requested == "semantic_rerank"
        assert effective in cfg.allowed_modes_normalized()

    def test_default_used_when_requested_is_none(self):
        cfg = PreferencesConfig(default_mode="llm_judge")
        requested, effective = _resolve_requested_mode(None, cfg)
        assert requested == "llm_judge"
        assert effective in cfg.allowed_modes_normalized()


class TestSafeMode:
    """Ensures tainted preference mode values never flow unsanitized into logs."""

    @pytest.mark.parametrize("value", ["semantic_rerank", "llm_judge", "fit_only", "default"])
    def test_known_modes_pass_through(self, value):
        assert _safe_mode(value) == value

    @pytest.mark.parametrize("value", ["", "unknown", "rm -rf /", "<script>"])
    def test_unknown_values_collapse_to_other(self, value):
        assert _safe_mode(value) == "other"
