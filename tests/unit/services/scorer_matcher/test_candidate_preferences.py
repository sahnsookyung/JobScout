"""Unit tests for candidate preference helper logic."""

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
    _job_work_mode,
    _matches_candidate_preferences,
    _preference_sort_key,
    _resolve_requested_mode,
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
    return SimpleNamespace(**defaults)


def _preliminary(job=None):
    return SimpleNamespace(job=job or _job())


def _scored_match(job, *, overall_score=80.0, fit_score=80.0, job_similarity=0.8, fit_components=None):
    return SimpleNamespace(
        job=job,
        overall_score=overall_score,
        fit_score=fit_score,
        job_similarity=job_similarity,
        fit_components=fit_components or {},
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
            "preference_profile": {"raw_text": "Mentorship", "parser_confidence": 0.7},
            "revision": 7,
        }


class TestHardFilterHelpers:
    def test_job_work_mode_detects_remote_hybrid_and_onsite(self):
        assert _job_work_mode(_job(is_remote=True)) == "remote"
        assert _job_work_mode(_job(work_from_home_type="Hybrid")) == "hybrid"
        assert _job_work_mode(_job(location_text="Hybrid - London")) == "hybrid"
        assert _job_work_mode(_job(location_text="Austin")) == "onsite"

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

        assert reranked[0].fit_components["preference_mode_used"] == "semantic_rerank"
        assert reranked[0].fit_components["preference_fallback_reason"] == "preference_profile_unavailable"

    @patch("services.scorer_matcher.candidate_preferences.build_preference_semantic_reranker")
    def test_semantic_reranker_updates_scores_and_order_with_fit_band_guardrail(
        self,
        mock_build_reranker,
    ):
        mock_build_reranker.return_value = Mock(
            rerank=Mock(
                return_value=[
                    PreferenceAssessment(
                        job_id="job-2",
                        preference_score=0.95,
                        preference_confidence=0.9,
                        preference_reason_codes=["team_culture_match"],
                        preference_explanation="Matches mentorship preferences.",
                    ),
                    PreferenceAssessment(
                        job_id="job-1",
                        preference_score=0.15,
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

        assert reranked[0].job.id == "job-1"
        assert reranked[1].job.id == "job-2"
        assert reranked[1].fit_components["preference_score"] == 0.95
        assert reranked[1].fit_components["preference_mode_used"] == "semantic_rerank"

    @patch("services.scorer_matcher.candidate_preferences.build_preference_judge")
    def test_llm_judge_mode_uses_judge_builder(self, mock_build_judge):
        mock_build_judge.return_value = Mock(
            judge=Mock(
                return_value=[
                    PreferenceAssessment(
                        job_id="job-1",
                        preference_score=0.8,
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

        assert reranked[0].fit_components["preference_mode_used"] == "llm_judge"
        assert reranked[0].fit_components["preference_score"] == 0.8

    def test_preference_sort_key_prefers_band_then_preference(self):
        high_pref = _scored_match(
            _job(id="job-1"),
            fit_score=82.0,
            fit_components={"preference_score": 0.9},
        )
        low_pref = _scored_match(
            _job(id="job-2"),
            fit_score=82.0,
            fit_components={"preference_score": 0.2},
        )

        assert _preference_sort_key(high_pref) < _preference_sort_key(low_pref)

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
                        preference_score=0.7,
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

        assert reranked[0].fit_components["preference_mode_requested"] == "llm_judge"
        assert reranked[0].fit_components["preference_mode_used"] == "semantic_rerank"
        assert "preference_fallback_reason" not in reranked[0].fit_components

    def test_preference_sort_keeps_job_id_ascending_for_exact_ties(self):
        first = _scored_match(
            _job(id="job-1"),
            fit_score=82.0,
            job_similarity=0.6,
            fit_components={"preference_score": 0.7},
        )
        second = _scored_match(
            _job(id="job-2"),
            fit_score=82.0,
            job_similarity=0.6,
            fit_components={"preference_score": 0.7},
        )

        ordered = sorted([second, first], key=_preference_sort_key)

        assert [match.job.id for match in ordered] == ["job-1", "job-2"]
