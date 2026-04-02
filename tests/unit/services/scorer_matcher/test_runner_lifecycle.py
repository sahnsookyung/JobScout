#!/usr/bin/env python3
"""Unit tests for matching runner resume lifecycle behavior."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.scorer_matcher.pipeline import (
    SaveMatchesBatchResult,
    run_matching_pipeline,
)


def _uow(repo, on_exit=None):
    manager = MagicMock()
    manager.__enter__.return_value = repo
    if on_exit is None:
        manager.__exit__.return_value = False
    else:
        def _exit(_exc_type, _exc, _tb):
            on_exit()
            return False

        manager.__exit__.side_effect = _exit
    return manager


def _matching_config():
    return SimpleNamespace(
        enabled=True,
        matcher=SimpleNamespace(),
        scorer=SimpleNamespace(),
        result_policy=SimpleNamespace(),
        recalculate_existing=False,
    )


def _ctx():
    return SimpleNamespace(
        config=SimpleNamespace(matching=_matching_config(), etl=None),
        ai_service=MagicMock(),
        job_etl_service=MagicMock(),
        notification_service=None,
    )


def _structured_resume(extracted_data=None):
    if extracted_data is None:
        extracted_data = {"profile": {"summary": {"text": "Summary"}}}
    return SimpleNamespace(
        extracted_data=extracted_data,
        total_experience_years=6.0,
    )


class _ExpiringStructuredResume:
    def __init__(self, extracted_data):
        self._extracted_data = extracted_data
        self.total_experience_years = 6.0
        self.expired = False

    @property
    def extracted_data(self):
        if self.expired:
            raise RuntimeError("detached structured resume")
        return self._extracted_data


def test_run_matching_pipeline_blocks_when_latest_resume_is_still_processing():
    ctx = _ctx()
    repo = MagicMock()
    repo.get_latest_ready_resume_fingerprint.return_value = None
    repo.get_latest_resume_processing_state.return_value = SimpleNamespace(
        processing_status="embedding"
    )

    with patch("services.scorer_matcher.pipeline.job_uow", return_value=_uow(repo)):
        result = run_matching_pipeline(ctx)

    assert result.success is False
    assert "still processing (embedding)" in result.error


def test_run_matching_pipeline_requires_any_ready_resume_to_exist():
    ctx = _ctx()
    repo = MagicMock()
    repo.get_latest_ready_resume_fingerprint.return_value = None
    repo.get_latest_resume_processing_state.return_value = None

    with patch("services.scorer_matcher.pipeline.job_uow", return_value=_uow(repo)):
        result = run_matching_pipeline(ctx)

    assert result.success is False
    assert result.error == "No ready resume found. Upload and process a resume first."


def test_run_matching_pipeline_requires_structured_data_for_ready_resume():
    ctx = _ctx()
    repo = MagicMock()
    repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
    repo.get_latest_resume_processing_state.return_value = None
    repo.resume.get_structured_resume_by_fingerprint.return_value = None

    with patch("services.scorer_matcher.pipeline.job_uow", return_value=_uow(repo)):
        result = run_matching_pipeline(ctx)

    assert result.success is False
    assert "missing structured data" in result.error


def test_run_matching_pipeline_fails_if_ready_resume_disappears_before_matching():
    ctx = _ctx()
    first_repo = MagicMock()
    first_repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
    first_repo.get_latest_resume_processing_state.return_value = None
    first_repo.resume.get_structured_resume_by_fingerprint.return_value = _structured_resume()

    second_repo = MagicMock()
    second_repo.resume.get_structured_resume_by_fingerprint.return_value = None

    with patch(
        "services.scorer_matcher.pipeline.job_uow",
        side_effect=[_uow(first_repo), _uow(second_repo)],
    ):
        result = run_matching_pipeline(ctx)

    assert result.success is False
    assert "Resume not found in database" in result.error


def test_run_matching_pipeline_fails_if_stored_ready_resume_cannot_be_validated():
    ctx = _ctx()
    first_repo = MagicMock()
    first_repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
    first_repo.get_latest_resume_processing_state.return_value = None
    first_repo.resume.get_structured_resume_by_fingerprint.return_value = _structured_resume(
        {"invalid": "schema"}
    )

    second_repo = MagicMock()
    second_repo.resume.get_structured_resume_by_fingerprint.return_value = _structured_resume(
        {"invalid": "schema"}
    )

    with patch(
        "services.scorer_matcher.pipeline.job_uow",
        side_effect=[_uow(first_repo), _uow(second_repo)],
    ), patch(
        "services.scorer_matcher.pipeline.MatcherService",
        return_value=MagicMock(),
    ), patch(
        "services.scorer_matcher.pipeline.ResumeSchema.model_validate",
        side_effect=ValueError("bad schema"),
    ):
        result = run_matching_pipeline(ctx)

    assert result.success is False
    assert "Failed to parse stored ready resume" in result.error


def test_run_matching_pipeline_uses_latest_ready_resume_and_reaches_save_boundary():
    ctx = _ctx()
    structured = _structured_resume(
        {"profile": {"summary": {"text": "Ready summary"}, "experience": []}}
    )
    first_repo = MagicMock()
    first_repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
    first_repo.get_latest_resume_processing_state.return_value = None
    first_repo.resume.get_structured_resume_by_fingerprint.return_value = structured

    second_repo = MagicMock()
    second_repo.resume.get_structured_resume_by_fingerprint.return_value = structured

    steps = []
    matcher = MagicMock()
    matcher.match_resume_two_stage.return_value = []
    scorer = MagicMock()
    scorer.score_matches.return_value = []

    with patch(
        "services.scorer_matcher.pipeline.job_uow",
        side_effect=[_uow(first_repo), _uow(second_repo), _uow(second_repo)],
    ), patch("services.scorer_matcher.pipeline.MatcherService", return_value=matcher), patch(
        "services.scorer_matcher.pipeline.ScoringService", return_value=scorer
    ), patch(
        "services.scorer_matcher.pipeline.ResumeSchema.model_validate",
        return_value=SimpleNamespace(profile=SimpleNamespace()),
    ), patch(
        "services.scorer_matcher.pipeline._save_matches_batch",
        return_value=SaveMatchesBatchResult(
            saved_count=0,
            failed_count=0,
            active_job_ids=frozenset(),
        ),
    ):
        result = run_matching_pipeline(ctx, status_callback=steps.append)

    assert result.success is True
    assert steps == [
        "loading_resume",
        "vector_matching",
        "scoring",
        "saving_results",
    ]
    matcher.match_resume_two_stage.assert_called_once()


def test_run_matching_pipeline_materializes_ready_resume_before_uow_closes():
    ctx = _ctx()
    expiring_resume = _ExpiringStructuredResume(
        {"profile": {"summary": {"text": "Ready summary"}, "experience": []}}
    )
    first_repo = MagicMock()
    first_repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
    first_repo.get_latest_resume_processing_state.return_value = None
    first_repo.resume.get_structured_resume_by_fingerprint.return_value = expiring_resume

    second_repo = MagicMock()
    second_repo.resume.get_structured_resume_by_fingerprint.return_value = _structured_resume(
        {"profile": {"summary": {"text": "Ready summary"}, "experience": []}}
    )

    matcher = MagicMock()
    matcher.match_resume_two_stage.return_value = []
    scorer = MagicMock()
    scorer.score_matches.return_value = []

    with patch(
        "services.scorer_matcher.pipeline.job_uow",
        side_effect=[
            _uow(first_repo, on_exit=lambda: setattr(expiring_resume, "expired", True)),
            _uow(second_repo),
            _uow(second_repo),
        ],
    ), patch("services.scorer_matcher.pipeline.MatcherService", return_value=matcher), patch(
        "services.scorer_matcher.pipeline.ScoringService", return_value=scorer
    ), patch(
        "services.scorer_matcher.pipeline.ResumeSchema.model_validate",
        return_value=SimpleNamespace(profile=SimpleNamespace()),
    ), patch(
        "services.scorer_matcher.pipeline._save_matches_batch",
        return_value=SaveMatchesBatchResult(
            saved_count=0,
            failed_count=0,
            active_job_ids=frozenset(),
        ),
    ):
        result = run_matching_pipeline(ctx)

    assert result.success is True
    assert matcher.match_resume_two_stage.call_args.kwargs["resume_data"] == {
        "profile": {"summary": {"text": "Ready summary"}, "experience": []}
    }


def test_run_matching_pipeline_does_not_fall_back_to_configured_resume_file_when_no_ready_resume():
    ctx = _ctx()
    ctx.config.etl = SimpleNamespace(
        resume=SimpleNamespace(
            resume_file="/tmp/resume.json",
            force_re_extraction=False,
        ),
        resume_file=None,
    )
    ctx.job_etl_service.process_resume.return_value = (True, "fp-config", {"raw_text": "ignored"})

    select_repo = MagicMock()
    select_repo.get_latest_ready_resume_fingerprint.return_value = None
    select_repo.get_latest_resume_processing_state.return_value = None

    with patch(
        "services.scorer_matcher.pipeline.job_uow",
        side_effect=[_uow(select_repo)],
    ):
        result = run_matching_pipeline(ctx)

    assert result.success is False
    assert result.error == "No ready resume found. Upload and process a resume first."
    ctx.job_etl_service.process_resume.assert_not_called()


def test_run_matching_pipeline_reports_cancelled_before_save_when_stop_requested():
    ctx = _ctx()
    structured = _structured_resume()
    first_repo = MagicMock()
    first_repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
    first_repo.get_latest_resume_processing_state.return_value = None
    first_repo.resume.get_structured_resume_by_fingerprint.return_value = structured

    second_repo = MagicMock()
    second_repo.resume.get_structured_resume_by_fingerprint.return_value = structured

    matcher = MagicMock()
    matcher.match_resume_two_stage.return_value = []
    scorer = MagicMock()
    stop_event = threading.Event()

    def _score_matches(**_kwargs):
        stop_event.set()
        return []

    scorer.score_matches.side_effect = _score_matches

    with patch(
        "services.scorer_matcher.pipeline.job_uow",
        side_effect=[_uow(first_repo), _uow(second_repo)],
    ), patch("services.scorer_matcher.pipeline.MatcherService", return_value=matcher), patch(
        "services.scorer_matcher.pipeline.ScoringService", return_value=scorer
    ), patch(
        "services.scorer_matcher.pipeline.ResumeSchema.model_validate",
        return_value=SimpleNamespace(profile=SimpleNamespace()),
    ):
        result = run_matching_pipeline(ctx, stop_event=stop_event)

    assert result.success is False
    assert result.cancelled is True
    assert result.error == "Cancelled by user"


def test_run_matching_pipeline_uses_default_recall_top_k_when_semantic_fit_config_missing():
    ctx = _ctx()
    structured = _structured_resume(
        {"profile": {"summary": {"text": "Ready summary"}, "experience": []}}
    )
    first_repo = MagicMock()
    first_repo.get_latest_ready_resume_fingerprint.return_value = "fp-ready"
    first_repo.get_latest_resume_processing_state.return_value = None
    first_repo.resume.get_structured_resume_by_fingerprint.return_value = structured

    second_repo = MagicMock()
    second_repo.resume.get_structured_resume_by_fingerprint.return_value = structured

    matcher = MagicMock()
    matcher.match_resume_two_stage.return_value = []
    scorer = MagicMock()
    scorer.score_matches.return_value = []

    with patch(
        "services.scorer_matcher.pipeline.job_uow",
        side_effect=[_uow(first_repo), _uow(second_repo), _uow(second_repo)],
    ), patch("services.scorer_matcher.pipeline.MatcherService", return_value=matcher) as matcher_cls, patch(
        "services.scorer_matcher.pipeline.ScoringService", return_value=scorer
    ), patch(
        "services.scorer_matcher.pipeline.ResumeSchema.model_validate",
        return_value=SimpleNamespace(profile=SimpleNamespace()),
    ), patch(
        "services.scorer_matcher.pipeline._save_matches_batch",
        return_value=SaveMatchesBatchResult(
            saved_count=0,
            failed_count=0,
            active_job_ids=frozenset(),
        ),
    ):
        result = run_matching_pipeline(ctx)

    assert result.success is True
    assert matcher_cls.call_args.kwargs["requirement_recall_top_k"] == 5
