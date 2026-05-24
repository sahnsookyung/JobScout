"""Unit tests for scorer_matcher pipeline helpers."""

import pytest
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from tests.mocks.fake_service import FakeLLMService
from services.scorer_matcher.candidate_preferences import (
    apply_candidate_preference_filters,
    apply_preference_semantic_reranking,
)
from core.config_loader import RankingConfig
from database.models import SYSTEM_OWNER_ID
from services.scorer_matcher.pipeline import (
    _prepare_selection_result,
    _convert_matches_to_dtos,
    _finish_pipeline_result,
    _load_resume_from_db,
    _ranking_snapshot_from_match,
    _result_after_matching,
    _result_after_saving,
    _resolve_ranking_context,
    PreparedSelectionResult,
    _save_matches_batch,
    _save_results_and_publish_selection,
    _run_matching_and_scoring,
    _run_scorer_service,
    _send_run_notifications,
    SaveMatchesBatchResult,
    _publish_match_selection_run,
    run_matching_pipeline,
)


def _uow(repo):
    manager = MagicMock()
    manager.__enter__.return_value = repo
    manager.__exit__.return_value = False
    return manager


def _dto(
    job_id: str = "job-1",
    *,
    match_id: str | None = None,
    fit_score: float = 80.0,
    preference_score: float | None = None,
    job_similarity: float = 0.8,
) -> SimpleNamespace:
    job = SimpleNamespace(id=job_id, title="Engineer", company="Acme", content_hash="hash-1")
    return SimpleNamespace(
        id=match_id or f"match-{job_id}",
        job=job,
        fit_score=fit_score,
        preference_score=preference_score,
        job_similarity=job_similarity,
        jd_required_coverage=0.75,
    )


def _persistable_dto(job_id: str = "job-1", *, content_hash: str = "hash-1"):
    dto = _dto(job_id=job_id)
    dto.job.content_hash = content_hash
    dto.fit_components = {"fit": "only"}
    dto.preference_components = {"preference": "only"}
    dto.ranking_snapshot = {"ranking_mode_used": "balanced"}
    dto.base_score = 90.0
    dto.penalties = 1.0
    dto.penalty_details = []
    dto.jd_required_coverage = 0.8
    dto.jd_preferred_requirement_coverage = 0.3
    dto.requirement_matches = [SimpleNamespace()]
    dto.missing_requirements = [SimpleNamespace()]
    dto.match_type = "requirements_only"
    return dto


def _prepared_selection_result(
    *dtos,
    owner_id: str | None = "user-1",
    persist_dtos: list | None = None,
    item_snapshots: list | None = None,
) -> PreparedSelectionResult:
    return PreparedSelectionResult(
        match_dtos=list(dtos),
        item_snapshots=list(item_snapshots or []),
        policy_snapshot=SimpleNamespace(),
        owner_id=owner_id,
        persist_match_dtos=list(persist_dtos or []),
    )


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
    @patch("services.scorer_matcher.pipeline.get_ranking_policy_store")
    def test_resolve_ranking_context_invalid_mode_falls_back_to_balanced(self, mock_store):
        mock_store.return_value.get_current_config.return_value = SimpleNamespace(
            active_default_mode="not-a-mode",
        )

        context = _resolve_ranking_context()

        assert context.mode.value == "balanced"

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
        config = SimpleNamespace(
            result_policy=SimpleNamespace(
                min_fit=50.0,
                min_jd_required_coverage=0.8,
                top_k=1,
            )
        )
        stop_event = threading.Event()
        policy = SimpleNamespace(
            min_fit=50.0,
            min_jd_required_coverage=0.8,
            top_k=1,
        )

        with patch(
            "services.scorer_matcher.pipeline.get_result_policy_store",
            return_value=SimpleNamespace(get_current_policy=lambda: policy),
        ):
            result = _run_scorer_service(scorer, ["prelim"], config, stop_event)

        assert result == ["scored"]
        scorer.score_matches.assert_called_once_with(
            preliminary_matches=["prelim"],
            result_policy=policy,
            match_type="requirements_only",
            stop_event=stop_event,
        )
        assert policy.min_fit == 0.0
        assert policy.min_jd_required_coverage is None
        assert policy.top_k == 1


class TestPrepareSelectionResult:
    @patch(
        "services.scorer_matcher.pipeline.resolve_notification_fit_floor",
        return_value=70.0,
    )
    @patch(
        "services.scorer_matcher.pipeline.get_result_policy_store",
        return_value=SimpleNamespace(
            get_current_policy=lambda: SimpleNamespace(
                min_fit=50.0,
                min_jd_required_coverage=None,
                top_k=1,
            )
        ),
    )
    def test_applies_ranking_before_top_k(self, _mock_policy_store, _mock_notification_floor):
        matches = [
            _dto("job-fit", fit_score=90.0, preference_score=0.1),
            _dto("job-pref", fit_score=80.0, preference_score=0.95),
        ]

        result = _prepare_selection_result(
            matches,
            ctx=SimpleNamespace(config=SimpleNamespace(notifications=SimpleNamespace())),
            owner_id="user-1",
            ranking_context=SimpleNamespace(
                mode=SimpleNamespace(value="balanced"),
                config=RankingConfig(
                    active_default_mode="balanced",
                    balanced_w_pref=0.7,
                    balanced_w_fit=0.3,
                ),
            ),
            matching_config=SimpleNamespace(),
            resume_resolution_reason="test",
            task_id="task-1",
        )

        assert [match.job.id for match in result.selected_matches] == ["job-pref"]

    @patch(
        "services.scorer_matcher.pipeline.resolve_notification_fit_floor",
        return_value=70.0,
    )
    @patch(
        "services.scorer_matcher.pipeline.get_result_policy_store",
        return_value=SimpleNamespace(
            get_current_policy=lambda: SimpleNamespace(
                min_fit=40.0,
                min_jd_required_coverage=None,
                top_k=5,
            )
        ),
    )
    def test_below_min_fit_items_are_tiered_as_excluded(
        self,
        _mock_policy_store,
        _mock_notification_floor,
    ):
        """Post two-tier contract: below-floor items are retained as excluded,
        not dropped and not auto-promoted. Preserves the user's configured floor
        while making below-floor runs visible in the UI."""
        matches = [
            SimpleNamespace(
                job=SimpleNamespace(id="job-1"),
                fit_score=32.0,
                preference_score=None,
                job_similarity=0.8,
                jd_required_coverage=0.2,
                fit_components={"effective_fit_mode": "threshold"},
            ),
            SimpleNamespace(
                job=SimpleNamespace(id="job-2"),
                fit_score=28.0,
                preference_score=None,
                job_similarity=0.7,
                jd_required_coverage=0.15,
                fit_components={"effective_fit_mode": "threshold"},
            ),
        ]

        result = _prepare_selection_result(
            matches,
            ctx=SimpleNamespace(config=SimpleNamespace(notifications=SimpleNamespace())),
            owner_id="user-1",
            ranking_context=SimpleNamespace(
                mode=SimpleNamespace(value="balanced"),
                config=RankingConfig(active_default_mode="balanced"),
            ),
            matching_config=SimpleNamespace(),
            resume_resolution_reason="test",
            task_id="task-1",
        )

        assert result.selected_matches == []
        assert result.policy_snapshot.fit_floor_used == 40.0
        tiers = [(item.selection_tier, item.excluded_reason) for item in result.item_snapshots]
        assert tiers == [("excluded", "below_min_fit"), ("excluded", "below_min_fit")]

    def test_run_scorer_service_widens_policy_to_score_every_preliminary(self):
        from services.scorer_matcher.pipeline import _run_scorer_service
        scorer = Mock()
        scorer.score_matches.return_value = []
        # ResultPolicy stub WITHOUT model_copy → exercises the else-branch (lines 634-637).
        policy = SimpleNamespace(
            min_fit=40.0, min_jd_required_coverage=0.5, top_k=10,
        )
        matching_config = SimpleNamespace(result_policy=policy)
        # 12 preliminary matches > policy.top_k=10 → widened_top_k must be 12.
        prelims = list(range(12))
        with patch(
            "services.scorer_matcher.pipeline._resolve_result_policy",
            return_value=policy,
        ):
            _run_scorer_service(scorer, prelims, matching_config, threading.Event())
        called_kwargs = scorer.score_matches.call_args.kwargs
        widened = called_kwargs["result_policy"]
        assert widened.min_fit == 0.0
        assert widened.min_jd_required_coverage is None
        assert widened.top_k == 12

    def test_run_scorer_service_no_op_when_no_preliminary_matches(self):
        from services.scorer_matcher.pipeline import _run_scorer_service
        scorer = Mock()
        scorer.score_matches.return_value = []
        with patch(
            "services.scorer_matcher.pipeline._resolve_result_policy",
            return_value=SimpleNamespace(
                min_fit=40.0, min_jd_required_coverage=0.5, top_k=5,
            ),
        ):
            _run_scorer_service(
                scorer, [], SimpleNamespace(result_policy=None), threading.Event()
            )
        # Policy untouched when no prelims (the if-block is skipped).
        called = scorer.score_matches.call_args.kwargs["result_policy"]
        assert called.min_fit == 40.0
        assert called.top_k == 5


class TestEvidenceRerankProvider:
    def test_returns_none_when_evidence_rerank_disabled(self):
        from services.scorer_matcher.pipeline import _resolve_evidence_rerank_provider
        cfg = SimpleNamespace(evidence_rerank_enabled=False, cross_encoder=None)
        assert _resolve_evidence_rerank_provider(cfg) is None

    def test_returns_none_when_local_provider_disabled(self):
        from services.scorer_matcher.pipeline import _resolve_evidence_rerank_provider
        cfg = SimpleNamespace(
            evidence_rerank_enabled=True,
            cross_encoder=SimpleNamespace(local=SimpleNamespace(enabled=False)),
        )
        assert _resolve_evidence_rerank_provider(cfg) is None

    def test_uses_shared_provider_when_enabled(self, monkeypatch):
        from services.scorer_matcher.pipeline import _resolve_evidence_rerank_provider

        seen_kwargs = {}

        def fake_get(**kwargs):
            seen_kwargs.update(kwargs)
            return Mock(name="shared-provider")

        monkeypatch.setattr(
            "services.scorer_matcher.pipeline.get_shared_local_cross_encoder_provider",
            fake_get,
        )
        cfg = SimpleNamespace(
            evidence_rerank_enabled=True,
            cross_encoder=SimpleNamespace(local=SimpleNamespace(
                enabled=True,
                model_name="bge",
                model_cache_path="/cache",
                runtime="auto",
                max_batch_size=16,
                trust_remote_code=False,
            )),
        )
        provider = _resolve_evidence_rerank_provider(cfg)
        assert provider is not None
        assert seen_kwargs["model_name"] == "bge"
        assert seen_kwargs["cache_path"] == "/cache"


class TestTwoTierFlagInPrepareSelection:
    @patch(
        "services.scorer_matcher.pipeline.resolve_notification_fit_floor",
        return_value=70.0,
    )
    @patch(
        "services.scorer_matcher.pipeline.get_result_policy_store",
        return_value=SimpleNamespace(
            get_current_policy=lambda: SimpleNamespace(
                min_fit=40.0,
                min_jd_required_coverage=None,
                top_k=5,
            )
        ),
    )
    def test_two_tier_selection_disabled_suppresses_excluded_snapshots(
        self,
        _mock_policy_store,
        _mock_notification_floor,
    ):
        """§J rollout gate: when TWO_TIER_SELECTION_ENABLED=false the engine must
        not persist excluded-tier items, so pre-§C single-tier behavior is
        preserved byte-for-byte."""
        matches = [
            SimpleNamespace(
                job=SimpleNamespace(id="job-1"),
                fit_score=32.0,
                preference_score=None,
                job_similarity=0.8,
                jd_required_coverage=0.2,
                fit_components={"effective_fit_mode": "threshold"},
            ),
        ]

        result = _prepare_selection_result(
            matches,
            ctx=SimpleNamespace(config=SimpleNamespace(notifications=SimpleNamespace())),
            owner_id="user-1",
            ranking_context=SimpleNamespace(
                mode=SimpleNamespace(value="balanced"),
                config=RankingConfig(active_default_mode="balanced"),
            ),
            matching_config=SimpleNamespace(two_tier_selection_enabled=False),
            resume_resolution_reason="test",
            task_id="task-1",
        )

        assert result.selected_matches == []
        assert result.item_snapshots == []


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

    @patch("services.scorer_matcher.candidate_preferences.build_preference_semantic_reranker")
    def test_soft_preferences_semantically_rerank_within_fit_band(self, mock_build_reranker):
        mock_build_reranker.return_value = MagicMock(
            rerank=MagicMock(
                return_value=[
                    SimpleNamespace(
                        job_id="job-high-preference",
                        preference_score=0.95,
                        preference_confidence=0.91,
                        preference_reason_codes=["team_culture_match"],
                        preference_explanation="Matches mentorship preferences.",
                    ),
                    SimpleNamespace(
                        job_id="job-low-preference",
                        preference_score=0.2,
                        preference_confidence=0.55,
                        preference_reason_codes=["no_preference_signal"],
                        preference_explanation="Weak preference signal.",
                    ),
                ]
            )
        )
        preferences = {
            "remote_mode": "any",
            "target_locations": [],
            "visa_sponsorship_required": False,
            "salary_min": None,
            "employment_types": [],
            "soft_preferences": "mentorship product python backend growth",
            "preference_mode": "semantic_rerank",
            "preference_profile": {
                "raw_text": "mentorship product python backend growth",
                "parse_version": "2026-04-01.v1",
                "parser_confidence": 0.8,
                "team_culture": [
                    {"label": "Mentorship", "weight": 0.9, "confidence": 0.9},
                ],
            },
            "revision": 5,
        }
        _valid_modes = {"semantic_rerank", "llm_judge"}
        preference_config = SimpleNamespace(
            default_mode="semantic_rerank",
            allowed_modes=["semantic_rerank"],
            parser=SimpleNamespace(enabled=False, model=None),
            semantic_reranker=SimpleNamespace(enabled=True, model="fake"),
            llm_judge=SimpleNamespace(enabled=False, model=None),
        )
        preference_config.allowed_modes_normalized = lambda: (
            [m for m in dict.fromkeys(preference_config.allowed_modes) if m in _valid_modes]
            or [preference_config.default_mode]
        )
        scored_matches = [
            SimpleNamespace(
                job=SimpleNamespace(
                    id="job-low-preference",
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
                fit_score=82.0,
                fit_components={},
            ),
            SimpleNamespace(
                job=SimpleNamespace(
                    id="job-high-preference",
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
                fit_score=81.0,
                fit_components={},
            ),
        ]

        reranked = apply_preference_semantic_reranking(
            scored_matches,
            preferences,
            config=preference_config,
        )

        # Input order is preserved — no sorting at pipeline stage
        assert reranked[0].job.title == "Platform Engineer"
        assert reranked[1].job.title == "Product Backend Engineer"
        # Preference scores written to each match
        assert reranked[0].preference_score == pytest.approx(0.2)
        assert reranked[1].preference_score == pytest.approx(0.95)


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
            fit_score=79.0,
            preference_score=None,
            job_similarity=0.8,
            jd_required_coverage=0.75,
            jd_preferred_requirement_coverage=0.5,
            matched_requirements=[],
            missing_requirements=[],
            resume_fingerprint="fp-123",
            fit_components={"core": 0.79},
            preference_components={"preference_mode_used": "semantic_rerank"},
            base_score=82.0,
            penalties=0.0,
            penalty_details={},
            match_type="requirements_only",
        )

        dtos = _convert_matches_to_dtos([scored_match])

        assert len(dtos) == 1
        assert dtos[0].fit_score == 79.0
        assert dtos[0].preference_score is None
        assert dtos[0].job.id == "job-1"


class TestRunMatchingAndScoring:
    @patch(
        "services.scorer_matcher.pipeline._resolve_ranking_context",
        return_value=SimpleNamespace(
            mode=SimpleNamespace(value="balanced"),
            config=RankingConfig(active_default_mode="balanced"),
        ),
    )
    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_cancelled_after_resume_preparation_returns_prepared_envelope(
        self,
        mock_uow,
        mock_prepare,
        _mock_ranking_context,
    ):
        repo = MagicMock()
        mock_uow.return_value = _uow(repo)
        mock_prepare.return_value = (
            SimpleNamespace(extracted_data={}, total_experience_years=3, owner_id="user-1"),
            MagicMock(),
        )
        stop_event = threading.Event()
        stop_event.set()

        result = _run_matching_and_scoring(
            ctx=SimpleNamespace(config=SimpleNamespace(preferences=SimpleNamespace()), ai_service=None),
            resume_data={"profile": {}},
            resume_fingerprint="fp-123",
            should_re_extract=False,
            matching_config=SimpleNamespace(scorer=MagicMock()),
            stop_event=stop_event,
            status_callback=None,
            task_id="task-1",
        )

        assert isinstance(result, PreparedSelectionResult)
        assert result.match_dtos == []
        assert result.owner_id == "user-1"

    @patch("services.scorer_matcher.pipeline._convert_matches_to_dtos", return_value=[_dto()])
    @patch(
        "services.scorer_matcher.pipeline._prepare_selection_result",
        return_value=SimpleNamespace(
            selected_matches=["reranked"],
            item_snapshots=[],
            policy_snapshot=SimpleNamespace(),
            owner_id="user-1",
        ),
    )
    @patch("services.scorer_matcher.pipeline.apply_preference_semantic_reranking", return_value=["reranked"])
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
        mock_apply_preferences,
        mock_prepare_selection,
        _mock_convert,
    ):
        repo = MagicMock()
        repo.candidate_preferences.get_preferences.return_value = None
        mock_uow.return_value = _uow(repo)
        scorer_config = MagicMock()
        mock_prepare.return_value = (
            SimpleNamespace(extracted_data={}, total_experience_years=3),
            MagicMock(),
        )

        result = _run_matching_and_scoring(
            ctx=SimpleNamespace(config=SimpleNamespace(preferences=SimpleNamespace()), ai_service=None),
            resume_data={"profile": {}},
            resume_fingerprint="fp-123",
            should_re_extract=False,
            matching_config=SimpleNamespace(scorer=scorer_config),
            stop_event=threading.Event(),
            status_callback=None,
        )

        assert result.match_dtos == [_dto()]
        mock_scorer_cls.assert_called_once_with(repo=repo, config=scorer_config, ai_service=None)
        mock_run_scorer.assert_called_once()
        mock_apply_preferences.assert_called_once_with(
            ["scored"],
            None,
            config=SimpleNamespace(),
        )
        prepare_args = mock_prepare_selection.call_args.args
        assert prepare_args[0] == ["reranked"]
        assert mock_prepare_selection.call_args.kwargs["matching_config"] == SimpleNamespace(scorer=scorer_config)

    @patch("services.scorer_matcher.pipeline._convert_matches_to_dtos", return_value=[_dto()])
    @patch(
        "services.scorer_matcher.pipeline._prepare_selection_result",
        return_value=SimpleNamespace(
            selected_matches=["scored"],
            item_snapshots=[],
            policy_snapshot=SimpleNamespace(),
            owner_id="user-1",
        ),
    )
    @patch("services.scorer_matcher.pipeline.apply_preference_semantic_reranking", return_value=["scored"])
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
        _mock_run_preference,
        _mock_prepare_selection,
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
            ctx=SimpleNamespace(
                ai_service=fake_ai,
                config=SimpleNamespace(preferences=SimpleNamespace()),
            ),
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
    @patch(
        "services.scorer_matcher.pipeline._prepare_selection_result",
        return_value=SimpleNamespace(
            selected_matches=["scored"],
            item_snapshots=[],
            policy_snapshot=SimpleNamespace(),
            owner_id="user-1",
        ),
    )
    @patch("services.scorer_matcher.pipeline.apply_preference_semantic_reranking", return_value=["scored"])
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
        _mock_run_preference,
        _mock_prepare_selection,
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
            ctx=SimpleNamespace(config=SimpleNamespace(preferences=SimpleNamespace()), ai_service=None),
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

    @patch(
        "services.scorer_matcher.pipeline._convert_matches_to_dtos",
        side_effect=[
            [_dto(job_id="job-primary")],
            [_dto(job_id="job-primary"), _dto(job_id="job-excluded")],
        ],
    )
    @patch(
        "services.scorer_matcher.pipeline._prepare_selection_result",
        return_value=SimpleNamespace(
            selected_matches=[
                SimpleNamespace(job=SimpleNamespace(id="job-primary")),
            ],
            item_snapshots=[
                SimpleNamespace(job_id="job-primary"),
                SimpleNamespace(job_id="job-excluded"),
            ],
            policy_snapshot=SimpleNamespace(),
            owner_id="user-1",
        ),
    )
    @patch(
        "services.scorer_matcher.pipeline.apply_preference_semantic_reranking",
        return_value=[
            SimpleNamespace(job=SimpleNamespace(id="job-primary")),
            SimpleNamespace(job=SimpleNamespace(id="job-excluded")),
        ],
    )
    @patch(
        "services.scorer_matcher.pipeline._run_scorer_service",
        return_value=[
            SimpleNamespace(job=SimpleNamespace(id="job-primary")),
            SimpleNamespace(job=SimpleNamespace(id="job-excluded")),
        ],
    )
    @patch("services.scorer_matcher.pipeline.ScoringService")
    @patch("services.scorer_matcher.pipeline._run_preliminary_matching", return_value=["prelim"])
    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_prepares_persistence_dtos_for_excluded_selection_snapshots(
        self,
        mock_uow,
        mock_prepare,
        _mock_preliminary,
        _mock_scorer_cls,
        _mock_run_scorer,
        _mock_apply_preferences,
        _mock_prepare_selection,
        mock_convert,
    ):
        repo = MagicMock()
        repo.candidate_preferences.get_preferences.return_value = None
        mock_uow.return_value = _uow(repo)
        mock_prepare.return_value = (
            SimpleNamespace(extracted_data={}, total_experience_years=3),
            MagicMock(),
        )

        result = _run_matching_and_scoring(
            ctx=SimpleNamespace(config=SimpleNamespace(preferences=SimpleNamespace()), ai_service=None),
            resume_data={"profile": {}},
            resume_fingerprint="fp-123",
            should_re_extract=False,
            matching_config=SimpleNamespace(scorer=MagicMock()),
            stop_event=threading.Event(),
            status_callback=None,
            owner_id="user-1",
        )

        assert [dto.job.id for dto in result.match_dtos] == ["job-primary"]
        assert [dto.job.id for dto in result.persist_match_dtos] == [
            "job-primary",
            "job-excluded",
        ]
        assert mock_convert.call_count == 2


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
            job_match_ids_by_job_id={"job-1": "match-job-1"},
        ),
    )
    @patch(
        "services.scorer_matcher.pipeline._publish_match_selection_run",
        return_value="selection-run-1",
    )
    @patch(
        "services.scorer_matcher.pipeline._run_matching_and_scoring",
        return_value=_prepared_selection_result(_dto()),
    )
    @patch(
        "services.scorer_matcher.pipeline._load_pipeline_resume",
        return_value=({"profile": {}}, "fp-123", False, None),
    )
    def test_notifies_after_scoring(
        self,
        _mock_resume,
        _mock_publish,
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
            job_match_ids_by_job_id={},
        ),
    )
    @patch(
        "services.scorer_matcher.pipeline._publish_match_selection_run",
        return_value="selection-run-1",
    )
    @patch(
        "services.scorer_matcher.pipeline._run_matching_and_scoring",
        return_value=_prepared_selection_result(),
    )
    @patch(
        "services.scorer_matcher.pipeline._load_pipeline_resume",
        return_value=({"profile": {}}, "fp-123", False, None),
    )
    def test_refreshes_match_set_when_no_matches_remain(
        self,
        _mock_resume,
        _mock_publish,
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
            job_match_ids_by_job_id={"job-1": "match-job-1"},
        ),
    )
    @patch(
        "services.scorer_matcher.pipeline._run_matching_and_scoring",
        return_value=_prepared_selection_result(_dto()),
    )
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

    @patch("services.scorer_matcher.pipeline.send_notifications", return_value=1)
    @patch("services.scorer_matcher.pipeline._refresh_resume_match_set", return_value=0)
    @patch(
        "services.scorer_matcher.pipeline._save_matches_batch",
        return_value=SaveMatchesBatchResult(
            saved_count=1,
            failed_count=0,
            active_job_ids=frozenset({"job-1"}),
            job_match_ids_by_job_id={"job-1": "match-job-1"},
        ),
    )
    @patch(
        "services.scorer_matcher.pipeline._publish_match_selection_run",
        return_value="selection-run-123",
    )
    @patch(
        "services.scorer_matcher.pipeline._run_matching_and_scoring",
        return_value=_prepared_selection_result(_dto()),
    )
    @patch(
        "services.scorer_matcher.pipeline._load_pipeline_resume",
        return_value=({"profile": {}}, "fp-123", False, None),
    )
    def test_passes_selection_run_id_to_notifications(
        self,
        _mock_resume,
        _mock_matching,
        mock_publish,
        _mock_save,
        mock_refresh,
        mock_notify,
    ):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True, recalculate_existing=True)
        ctx.config.notifications = SimpleNamespace(enabled=True)
        ctx.notification_service = MagicMock()

        result = run_matching_pipeline(ctx, owner_id="user-1", task_id="task-1")

        assert result.success is True
        mock_refresh.assert_called_once_with("fp-123", active_job_ids=frozenset({"job-1"}))
        mock_publish.assert_called_once()
        assert mock_notify.call_args.kwargs["selection_run_id"] == "selection-run-123"


class TestPipelineNotificationAndPublicationHelpers:
    def test_send_run_notifications_skips_when_no_service_or_enabled_config(self):
        ctx = SimpleNamespace(
            notification_service=None,
            config=SimpleNamespace(notifications=SimpleNamespace(enabled=False)),
        )

        count = _send_run_notifications(
            ctx,
            failed_count=0,
            resume_fingerprint="fp-123",
            stop_event=threading.Event(),
            status_callback=MagicMock(),
            selection_run_id="run-1",
            owner_id="user-1",
            task_id="task-1",
        )

        assert count == 0

    @patch("services.scorer_matcher.pipeline.send_notifications", return_value=2)
    def test_send_run_notifications_invokes_status_callback_and_delegates(self, mock_send):
        status_callback = MagicMock()
        ctx = SimpleNamespace(
            notification_service=MagicMock(),
            config=SimpleNamespace(notifications=SimpleNamespace(enabled=True)),
        )

        count = _send_run_notifications(
            ctx,
            failed_count=0,
            resume_fingerprint="fp-123",
            stop_event=threading.Event(),
            status_callback=status_callback,
            selection_run_id="run-1",
            owner_id="user-1",
            task_id="task-1",
        )

        assert count == 2
        status_callback.assert_called_once_with("notifying")
        assert mock_send.call_args.kwargs["selection_run_id"] == "run-1"

    @patch("services.scorer_matcher.pipeline._publish_match_selection_run", return_value="run-1")
    @patch("services.scorer_matcher.pipeline._refresh_resume_match_set")
    @patch("services.scorer_matcher.pipeline._save_matches_batch")
    def test_save_results_and_publish_selection_refreshes_then_publishes(
        self,
        mock_save,
        mock_refresh,
        mock_publish,
    ):
        mock_save.return_value = SaveMatchesBatchResult(
            saved_count=1,
            failed_count=0,
            active_job_ids=frozenset({"job-1"}),
            job_match_ids_by_job_id={"job-1": "match-1"},
        )

        save_result, selection_run_id = _save_results_and_publish_selection(
            match_dtos=[_dto()],
            resume_fingerprint="fp-123",
            matching_config=SimpleNamespace(),
            prepared_selection=_prepared_selection_result(_dto()),
            task_id="task-1",
        )

        assert save_result.saved_count == 1
        assert selection_run_id == "run-1"
        mock_refresh.assert_called_once_with("fp-123", active_job_ids=frozenset({"job-1"}))
        mock_publish.assert_called_once()
        assert mock_publish.call_args.kwargs["owner_id"] == "user-1"

    @patch("services.scorer_matcher.pipeline._publish_match_selection_run", return_value="run-1")
    @patch("services.scorer_matcher.pipeline._refresh_resume_match_set")
    @patch("services.scorer_matcher.pipeline._save_matches_batch")
    def test_save_results_and_publish_selection_uses_resolved_resume_owner(
        self,
        mock_save,
        _mock_refresh,
        mock_publish,
    ):
        mock_save.return_value = SaveMatchesBatchResult(
            saved_count=1,
            failed_count=0,
            active_job_ids=frozenset({"job-1"}),
            job_match_ids_by_job_id={"job-1": "match-1"},
        )
        prepared_selection = _prepared_selection_result(_dto(), owner_id="resume-owner-1")

        _save_results_and_publish_selection(
            match_dtos=[_dto()],
            resume_fingerprint="fp-123",
            matching_config=SimpleNamespace(),
            prepared_selection=prepared_selection,
            task_id="task-1",
        )

        assert mock_publish.call_args.kwargs["owner_id"] == "resume-owner-1"

    @patch("services.scorer_matcher.pipeline._publish_match_selection_run", return_value="run-1")
    @patch("services.scorer_matcher.pipeline._refresh_resume_match_set")
    @patch("services.scorer_matcher.pipeline._save_matches_batch")
    def test_save_results_and_publish_selection_saves_persistence_dtos(
        self,
        mock_save,
        mock_refresh,
        mock_publish,
    ):
        primary = _dto(job_id="job-primary")
        excluded = _dto(job_id="job-excluded")
        mock_save.return_value = SaveMatchesBatchResult(
            saved_count=2,
            failed_count=0,
            active_job_ids=frozenset({"job-primary", "job-excluded"}),
            job_match_ids_by_job_id={
                "job-primary": "match-primary",
                "job-excluded": "match-excluded",
            },
        )

        save_result, selection_run_id = _save_results_and_publish_selection(
            match_dtos=[primary],
            resume_fingerprint="fp-123",
            matching_config=SimpleNamespace(),
            prepared_selection=_prepared_selection_result(
                primary,
                persist_dtos=[primary, excluded],
                item_snapshots=[
                    SimpleNamespace(job_id="job-primary"),
                    SimpleNamespace(job_id="job-excluded"),
                ],
            ),
            task_id="task-1",
        )

        assert save_result.saved_count == 2
        assert selection_run_id == "run-1"
        saved_dtos = mock_save.call_args.args[0]
        assert [dto.job.id for dto in saved_dtos] == ["job-primary", "job-excluded"]
        mock_refresh.assert_called_once_with(
            "fp-123",
            active_job_ids=frozenset({"job-primary", "job-excluded"}),
        )
        mock_publish.assert_called_once()

    @patch("services.scorer_matcher.pipeline._publish_match_selection_run")
    @patch("services.scorer_matcher.pipeline._refresh_resume_match_set")
    @patch("services.scorer_matcher.pipeline._save_matches_batch")
    def test_save_results_and_publish_selection_skips_publication_on_save_failure(
        self,
        mock_save,
        mock_refresh,
        mock_publish,
    ):
        mock_save.return_value = SaveMatchesBatchResult(
            saved_count=0,
            failed_count=1,
            active_job_ids=frozenset(),
            job_match_ids_by_job_id={},
        )

        save_result, selection_run_id = _save_results_and_publish_selection(
            match_dtos=[_dto()],
            resume_fingerprint="fp-123",
            matching_config=SimpleNamespace(),
            prepared_selection=_prepared_selection_result(_dto()),
            task_id="task-1",
        )

        assert save_result.failed_count == 1
        assert selection_run_id is None
        mock_refresh.assert_not_called()
        mock_publish.assert_not_called()


class TestDtoRankingSnapshot:
    def test_ranking_snapshot_from_match_without_explanation_is_empty(self):
        assert _ranking_snapshot_from_match(SimpleNamespace(ranking_explanation=None)) == {}

    def test_ranking_snapshot_from_match_serializes_explanation_dataclass(self):
        from core.ranking.engine import RankingExplanation

        explanation = RankingExplanation(
            ranking_mode_used="balanced",
            config_version="cfg-1",
            preference_score=0.7,
            fit_score=0.8,
            similarity_score=0.6,
            explanation_label="Balanced fit and preference",
            dominant_reason_code="balanced_blend",
            missing_scores=[],
        )

        assert _ranking_snapshot_from_match(SimpleNamespace(ranking_explanation=explanation)) == {
            "ranking_mode_used": "balanced",
            "config_version": "cfg-1",
            "preference_score": 0.7,
            "fit_score": 0.8,
            "similarity_score": 0.6,
            "balanced_primary_score": None,
            "explanation_label": "Balanced fit and preference",
            "dominant_reason_code": "balanced_blend",
            "missing_scores": [],
        }


class TestSaveMatchesBatch:
    @patch("services.scorer_matcher.pipeline.save_match_to_db")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_save_matches_batch_refreshes_existing_active_snapshot(self, mock_uow, mock_save):
        existing = SimpleNamespace(
            id="existing-match-1",
            status="active",
            job_content_hash="hash-1",
        )
        repo = SimpleNamespace(
            db=MagicMock(),
            get_existing_match=MagicMock(return_value=existing),
        )
        mock_uow.return_value = _uow(repo)

        result = _save_matches_batch(
            [_persistable_dto()],
            "fp-123",
            SimpleNamespace(recalculate_existing=False),
        )

        assert result.saved_count == 1
        assert result.job_match_ids_by_job_id == {"job-1": "existing-match-1"}
        assert existing.preference_components == {"preference": "only"}
        assert existing.preferred_requirement_coverage == 0.3
        repo.db.flush.assert_called_once()
        mock_save.assert_not_called()

    @patch("services.scorer_matcher.pipeline.save_match_to_db")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_save_matches_batch_creates_stale_replacement_when_content_changed(
        self,
        mock_uow,
        mock_save,
    ):
        existing = SimpleNamespace(
            id="old-match-1",
            status="active",
            job_content_hash="old-hash",
            invalidated_reason=None,
        )
        repo = SimpleNamespace(
            get_existing_match=MagicMock(return_value=existing),
        )
        mock_uow.return_value = _uow(repo)
        mock_save.return_value = SimpleNamespace(id="new-match-1")

        result = _save_matches_batch(
            [_persistable_dto(content_hash="new-hash")],
            "fp-123",
            SimpleNamespace(recalculate_existing=False),
        )

        assert result.saved_count == 1
        assert existing.status == "stale"
        assert existing.invalidated_reason == "Job content updated"
        assert result.job_match_ids_by_job_id == {"job-1": "new-match-1"}
        mock_save.assert_called_once()

    @patch("services.scorer_matcher.pipeline.save_match_to_db")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_save_matches_batch_saves_new_match(self, mock_uow, mock_save):
        repo = SimpleNamespace(get_existing_match=MagicMock(return_value=None))
        mock_uow.return_value = _uow(repo)
        mock_save.return_value = SimpleNamespace(id="match-1")

        result = _save_matches_batch(
            [_persistable_dto()],
            "fp-123",
            SimpleNamespace(recalculate_existing=False),
        )

        assert result.saved_count == 1
        assert result.failed_count == 0
        assert result.active_job_ids == frozenset({"job-1"})
        assert result.job_match_ids_by_job_id == {"job-1": "match-1"}


class TestPublishMatchSelectionRun:
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_publishes_missing_owner_id_as_system_owner(self, mock_uow):
        repo = MagicMock()
        repo.match_selection.publish_selection_run.return_value = SimpleNamespace(id="run-1")
        mock_uow.return_value = _uow(repo)

        selection_run_id = _publish_match_selection_run(
            owner_id=None,
            resume_fingerprint="fp-123",
            task_id="task-1",
            prepared_selection=_prepared_selection_result(_dto()),
            save_batch_result=SaveMatchesBatchResult(
                saved_count=1,
                failed_count=0,
                active_job_ids=frozenset({"job-1"}),
                job_match_ids_by_job_id={"job-1": "match-job-1"},
            ),
        )

        assert selection_run_id == "run-1"
        assert (
            repo.match_selection.publish_selection_run.call_args.kwargs["owner_id"]
            == SYSTEM_OWNER_ID
        )
