"""Unit tests for candidate preference helper logic."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from services.scorer_matcher.candidate_preferences import (
    _job_matches_employment_types,
    _job_matches_locations,
    _job_matches_remote_mode,
    _job_meets_salary_floor,
    _job_supports_visa,
    _job_work_mode,
    _matches_candidate_preferences,
    _tokenize_soft_preferences,
    apply_candidate_preference_filters,
    apply_soft_preference_reranking,
    load_candidate_preferences,
)


def _job(**overrides):
    defaults = {
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
            preference_mode="semantic_rerank",
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
            "preference_mode": "semantic_rerank",
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


class TestSoftPreferenceHelpers:
    def test_tokenize_soft_preferences_drops_short_words_and_stopwords(self):
        assert _tokenize_soft_preferences("the next backend product role with growth") == {
            "backend",
            "product",
            "growth",
        }

    def test_apply_soft_preference_reranking_handles_empty_and_no_overlap_cases(self):
        matches = [
            SimpleNamespace(
                job=_job(description="Legacy COBOL systems", skills_raw="cobol, mainframe"),
                overall_score=80.0,
                fit_components={},
            )
        ]

        assert apply_soft_preference_reranking(matches, None) == matches
        assert apply_soft_preference_reranking(matches, {"soft_preferences": "", "revision": 1}) == matches
        assert apply_soft_preference_reranking(
            matches,
            {"soft_preferences": "python mentorship", "revision": 1},
        )[0].overall_score == 80.0

    def test_apply_soft_preference_reranking_adds_bonus_and_overlap_metadata(self):
        low_overlap = SimpleNamespace(
            job=_job(description="Python backend role"),
            overall_score=80.0,
            fit_components={},
        )
        high_overlap = SimpleNamespace(
            job=_job(
                description="Python backend role with mentorship and product growth",
                raw_payload={"ai_job_summary": "Growth and mentorship focused team"},
            ),
            overall_score=79.5,
            fit_components={},
        )

        reranked = apply_soft_preference_reranking(
            [low_overlap, high_overlap],
            {"soft_preferences": "python mentorship product growth", "revision": 2},
        )

        assert reranked[0] is high_overlap
        assert reranked[0].overall_score > reranked[1].overall_score
        assert reranked[0].fit_components["soft_preference_bonus"] > 0
        assert "mentorship" in reranked[0].fit_components["soft_preference_overlap"]
