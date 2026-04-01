"""Unit tests for scorer_matcher pipeline helpers."""

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from core.llm.fake_service import FakeLLMService
from services.scorer_matcher.candidate_preferences import (
    apply_candidate_preference_filters,
    apply_soft_preference_reranking,
)
from services.scorer_matcher.pipeline import (
    _convert_matches_to_dtos,
    _finish_pipeline_result,
    _load_resume_from_db,
    _result_after_matching,
    _result_after_saving,
    _run_matching_and_scoring,
    _run_scorer_service,
    SaveMatchesBatchResult,
    run_matching_pipeline,
)


def _uow(repo):
    manager = MagicMock()
    manager.__enter__.return_value = repo
    manager.__exit__.return_value = False
    return manager


def _dto(job_id: str = "job-1") -> SimpleNamespace:
    job = SimpleNamespace(id=job_id, title="Engineer", company="Acme", content_hash="hash-1")
    return SimpleNamespace(job=job, overall_score=85.0, fit_score=80.0)


def _preliminary(
    *,
    job_id: str,
    is_remote: bool | None = None,
    location_text: str = "Remote",
    salary_min: int | None = None,
    salary_max: int | None = None,
    job_type: str | None = None,
    description: str = "",
    raw_payload: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        job=SimpleNamespace(
            id=job_id,
            title="Engineer",
            company="Acme",
            is_remote=is_remote,
            work_from_home_type=None,
            location_text=location_text,
            salary_min=salary_min,
            salary_max=salary_max,
            job_type=job_type,
            description=description,
            company_description="",
            skills_raw="python, mentorship, backend",
            raw_payload=raw_payload or {},
        ),
        job_similarity=0.8,
        requirement_matches=[],
        missing_requirements=[],
        resume_fingerprint="fp-123",
    )


class TestLoadResumeFromDb:
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_missing_resume_returns_none(self, mock_uow):
        repo = MagicMock()
        repo.resume.get_structured_resume_by_fingerprint.return_value = None
        mock_uow.return_value = _uow(repo)

        assert _load_resume_from_db("fp-123") is None

    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_returns_extracted_data(self, mock_uow):
        repo = MagicMock()
        repo.resume.get_structured_resume_by_fingerprint.return_value = SimpleNamespace(
            extracted_data={"profile": {"summary": "Engineer"}},
        )
        mock_uow.return_value = _uow(repo)

        assert _load_resume_from_db("fp-123") == {"profile": {"summary": "Engineer"}}


class TestPipelineResults:
    def test_result_after_matching_cancelled_with_matches(self):
        stop = threading.Event()
        stop.set()

        result = _result_after_matching([_dto()], stop)

        assert result is not None
        assert result.cancelled is True
        assert result.matches_count == 1

    def test_result_after_saving_cancelled(self):
        stop = threading.Event()
        stop.set()

        result = _result_after_saving([_dto()], 1, stop, time.time())

        assert result is not None
        assert result.cancelled is True
        assert result.saved_count == 1

    def test_finish_pipeline_success(self):
        result = _finish_pipeline_result([_dto()], 1, 0, threading.Event(), time.time())

        assert result.success is True
        assert result.matches_count == 1
        assert result.saved_count == 1


class TestRunScorerService:
    def test_delegates_to_score_matches(self):
        scorer = MagicMock()
        scorer.score_matches.return_value = ["scored"]
        config = SimpleNamespace(result_policy=SimpleNamespace(min_fit=50.0))
        stop_event = threading.Event()

        with patch(
            "services.scorer_matcher.pipeline.get_result_policy_store",
            return_value=SimpleNamespace(get_current_policy=lambda: "policy"),
        ):
            result = _run_scorer_service(scorer, ["prelim"], config, stop_event)

        assert result == ["scored"]
        scorer.score_matches.assert_called_once_with(
            preliminary_matches=["prelim"],
            result_policy="policy",
            match_type="requirements_only",
            stop_event=stop_event,
        )


class TestCandidatePreferenceHelpers:
    def test_filters_preliminary_matches_with_hard_preferences(self):
        preferences = {
            "remote_mode": "remote",
            "target_locations": ["Berlin"],
            "visa_sponsorship_required": True,
            "salary_min": 120000,
            "employment_types": ["full-time"],
            "soft_preferences": "",
            "revision": 3,
        }
        preliminaries = [
            _preliminary(
                job_id="job-keep",
                is_remote=True,
                location_text="Berlin, Germany",
                salary_max=150000,
                job_type="Full-time",
                description="We offer visa sponsorship and relocation assistance.",
                raw_payload={"visa_sponsorship_available": True},
            ),
            _preliminary(
                job_id="job-drop",
                is_remote=False,
                location_text="New York, USA",
                salary_max=90000,
                job_type="Contract",
                description="Applicants must already be authorized to work.",
            ),
        ]

        filtered = apply_candidate_preference_filters(preliminaries, preferences)

        assert [match.job.id for match in filtered] == ["job-keep"]

    def test_soft_preferences_add_bounded_bonus_and_resort_matches(self):
        preferences = {
            "remote_mode": "any",
            "target_locations": [],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [],
            "soft_preferences": "mentorship product python backend growth",
            "revision": 5,
        }
        scored_matches = [
            SimpleNamespace(
                job=SimpleNamespace(
                    title="Platform Engineer",
                    company="Acme",
                    description="Stable backend role",
                    company_description="",
                    skills_raw="python, kubernetes",
                    job_type="Full-time",
                    location_text="Remote",
                    work_from_home_type="remote",
                    raw_payload={},
                ),
                overall_score=82.0,
                fit_score=82.0,
                fit_components={},
            ),
            SimpleNamespace(
                job=SimpleNamespace(
                    title="Product Backend Engineer",
                    company="Acme",
                    description="Python backend role with mentorship and growth",
                    company_description="",
                    skills_raw="python, backend, product, mentorship",
                    job_type="Full-time",
                    location_text="Remote",
                    work_from_home_type="remote",
                    raw_payload={},
                ),
                overall_score=81.0,
                fit_score=81.0,
                fit_components={},
            ),
        ]

        reranked = apply_soft_preference_reranking(scored_matches, preferences)

        assert reranked[0].job.title == "Product Backend Engineer"
        assert reranked[0].overall_score > reranked[1].overall_score
        assert 0 < reranked[0].fit_components["soft_preference_bonus"] <= 5.0


class TestConvertMatchesToDtos:
    def test_converts_scored_match_without_want_fields(self):
        scored_match = SimpleNamespace(
            job=SimpleNamespace(
                id="job-1",
                title="Engineer",
                company="Acme",
                location_text="Remote",
                is_remote=True,
                content_hash="hash-1",
            ),
            overall_score=82.0,
            fit_score=79.0,
            job_similarity=0.8,
            jd_required_coverage=0.75,
            jd_preferences_coverage=0.5,
            matched_requirements=[],
            missing_requirements=[],
            resume_fingerprint="fp-123",
            fit_components={"core": 0.79},
            base_score=82.0,
            penalties=0.0,
            penalty_details={},
            match_type="requirements_only",
        )

        dtos = _convert_matches_to_dtos([scored_match])

        assert len(dtos) == 1
        assert dtos[0].overall_score == 82.0
        assert dtos[0].fit_score == 79.0
        assert dtos[0].job.id == "job-1"


class TestRunMatchingAndScoring:
    @patch("services.scorer_matcher.pipeline._convert_matches_to_dtos", return_value=[_dto()])
    @patch("services.scorer_matcher.pipeline._run_scorer_service", return_value=["scored"])
    @patch("services.scorer_matcher.pipeline.ScoringService")
    @patch("services.scorer_matcher.pipeline._run_preliminary_matching", return_value=["prelim"])
    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_runs_fit_only_scoring_flow(
        self,
        mock_uow,
        mock_prepare,
        _mock_preliminary,
        mock_scorer_cls,
        mock_run_scorer,
        _mock_convert,
    ):
        repo = MagicMock()
        mock_uow.return_value = _uow(repo)
        scorer_config = MagicMock()
        mock_prepare.return_value = (
            SimpleNamespace(extracted_data={}, total_experience_years=3),
            MagicMock(),
        )

        result = _run_matching_and_scoring(
            ctx=MagicMock(),
            resume_data={"profile": {}},
            resume_fingerprint="fp-123",
            should_re_extract=False,
            matching_config=SimpleNamespace(scorer=scorer_config),
            stop_event=threading.Event(),
            status_callback=None,
        )

        assert result == [_dto()]
        mock_scorer_cls.assert_called_once_with(repo=repo, config=scorer_config, ai_service=None)
        mock_run_scorer.assert_called_once()

    @patch("services.scorer_matcher.pipeline._convert_matches_to_dtos", return_value=[_dto()])
    @patch("services.scorer_matcher.pipeline._run_scorer_service", return_value=["scored"])
    @patch("services.scorer_matcher.pipeline.ScoringService")
    @patch("services.scorer_matcher.pipeline._run_preliminary_matching", return_value=["prelim"])
    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_passes_ai_service_into_scoring_when_available(
        self,
        mock_uow,
        mock_prepare,
        _mock_preliminary,
        mock_scorer_cls,
        _mock_run_scorer,
        _mock_convert,
    ):
        repo = MagicMock()
        mock_uow.return_value = _uow(repo)
        scorer_config = MagicMock()
        fake_ai = FakeLLMService()
        mock_prepare.return_value = (
            SimpleNamespace(extracted_data={}, total_experience_years=3),
            MagicMock(),
        )

        _run_matching_and_scoring(
            ctx=SimpleNamespace(ai_service=fake_ai),
            resume_data={"profile": {}},
            resume_fingerprint="fp-123",
            should_re_extract=False,
            matching_config=SimpleNamespace(scorer=scorer_config),
            stop_event=threading.Event(),
            status_callback=None,
        )

        mock_scorer_cls.assert_called_once_with(
            repo=repo,
            config=scorer_config,
            ai_service=fake_ai,
        )

    @patch("services.scorer_matcher.pipeline._convert_matches_to_dtos", return_value=[_dto()])
    @patch("services.scorer_matcher.pipeline._run_scorer_service", return_value=["scored"])
    @patch("services.scorer_matcher.pipeline.ScoringService")
    @patch("services.scorer_matcher.pipeline._run_preliminary_matching")
    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_applies_candidate_preferences_before_scoring(
        self,
        mock_uow,
        mock_prepare,
        mock_run_preliminary,
        _mock_scorer_cls,
        mock_run_scorer,
        _mock_convert,
    ):
        repo = MagicMock()
        repo.candidate_preferences.get_preferences.return_value = SimpleNamespace(
            remote_mode="remote",
            target_locations=["Berlin"],
            visa_sponsorship_required=False,
            salary_min=None,
            employment_types=[],
            soft_preferences="",
            revision=4,
        )
        mock_uow.return_value = _uow(repo)
        mock_prepare.return_value = (
            SimpleNamespace(extracted_data={}, total_experience_years=3),
            MagicMock(),
        )
        mock_run_preliminary.return_value = [
            _preliminary(job_id="job-keep", is_remote=True, location_text="Berlin, Germany"),
            _preliminary(job_id="job-drop", is_remote=False, location_text="Austin, USA"),
        ]

        _run_matching_and_scoring(
            ctx=MagicMock(),
            resume_data={"profile": {}},
            resume_fingerprint="fp-123",
            should_re_extract=False,
            matching_config=SimpleNamespace(scorer=MagicMock()),
            stop_event=threading.Event(),
            status_callback=None,
            owner_id="user-1",
        )

        filtered_preliminaries = mock_run_scorer.call_args.args[1]
        assert len(filtered_preliminaries) == 1
        assert filtered_preliminaries[0].job.id == "job-keep"


class TestRunMatchingPipeline:
    def test_disabled_matching_returns_early(self):
        ctx = MagicMock()
        ctx.config.matching = None

        result = run_matching_pipeline(ctx)

        assert result.success is True
        assert result.matches_count == 0

    @patch("services.scorer_matcher.pipeline.send_notifications", return_value=1)
    @patch("services.scorer_matcher.pipeline._refresh_resume_match_set", return_value=0)
    @patch(
        "services.scorer_matcher.pipeline._save_matches_batch",
        return_value=SaveMatchesBatchResult(
            saved_count=1,
            failed_count=0,
            active_job_ids=frozenset({"job-1"}),
        ),
    )
    @patch("services.scorer_matcher.pipeline._run_matching_and_scoring", return_value=[_dto()])
    @patch(
        "services.scorer_matcher.pipeline._load_pipeline_resume",
        return_value=({"profile": {}}, "fp-123", False, None),
    )
    def test_notifies_after_scoring(
        self,
        _mock_resume,
        _mock_matching,
        _mock_save,
        mock_refresh,
        mock_notify,
    ):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True, recalculate_existing=True)
        ctx.notification_service = MagicMock()

        result = run_matching_pipeline(ctx)

        assert result.success is True
        assert result.notified_count == 1
        mock_refresh.assert_called_once_with("fp-123", active_job_ids=frozenset({"job-1"}))
        mock_notify.assert_called_once()

    @patch("services.scorer_matcher.pipeline._refresh_resume_match_set", return_value=2)
    @patch(
        "services.scorer_matcher.pipeline._save_matches_batch",
        return_value=SaveMatchesBatchResult(
            saved_count=0,
            failed_count=0,
            active_job_ids=frozenset(),
        ),
    )
    @patch("services.scorer_matcher.pipeline._run_matching_and_scoring", return_value=[])
    @patch(
        "services.scorer_matcher.pipeline._load_pipeline_resume",
        return_value=({"profile": {}}, "fp-123", False, None),
    )
    def test_refreshes_match_set_when_no_matches_remain(
        self,
        _mock_resume,
        _mock_matching,
        mock_save,
        mock_refresh,
    ):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True, recalculate_existing=True)
        ctx.notification_service = None

        result = run_matching_pipeline(ctx)

        assert result.success is True
        assert result.matches_count == 0
        assert result.saved_count == 0
        mock_refresh.assert_called_once_with("fp-123", active_job_ids=frozenset())
        mock_save.assert_called_once_with([], "fp-123", ctx.config.matching)

    @patch("services.scorer_matcher.pipeline._refresh_resume_match_set")
    @patch(
        "services.scorer_matcher.pipeline._save_matches_batch",
        return_value=SaveMatchesBatchResult(
            saved_count=1,
            failed_count=1,
            active_job_ids=frozenset({"job-1"}),
        ),
    )
    @patch("services.scorer_matcher.pipeline._run_matching_and_scoring", return_value=[_dto()])
    @patch(
        "services.scorer_matcher.pipeline._load_pipeline_resume",
        return_value=({"profile": {}}, "fp-123", False, None),
    )
    def test_skips_refresh_when_any_match_save_fails(
        self,
        _mock_resume,
        _mock_matching,
        mock_save,
        mock_refresh,
    ):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True, recalculate_existing=True)
        ctx.notification_service = None

        result = run_matching_pipeline(ctx)

        assert result.success is True
        assert result.saved_count == 1
        save_args = mock_save.call_args.args
        assert len(save_args[0]) == 1
        assert save_args[0][0].job.id == "job-1"
        assert save_args[1:] == ("fp-123", ctx.config.matching)
        mock_refresh.assert_not_called()
