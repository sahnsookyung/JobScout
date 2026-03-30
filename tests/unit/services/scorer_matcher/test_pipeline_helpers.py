#!/usr/bin/env python3
"""Unit tests for scorer_matcher pipeline helper functions."""

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.scorer_matcher.pipeline import (
    MatchingPipelineResult,
    load_user_wants_data,
    _load_resume_from_db,
    _load_requested_resume,
    _load_pipeline_resume,
    _load_latest_ready_resume,
    _result_after_matching,
    _result_after_saving,
    _finish_pipeline_result,
    _load_user_wants_embeddings,
    _load_structured_resume,
    _get_pre_extracted_resume,
    _save_matches_batch,
    _log_match_results,
    _run_scorer_service,
    _resolve_result_policy,
    _build_job_facet_embeddings_map,
    _log_resume_preparation,
    _prepare_matching_run,
    _run_preliminary_matching,
    _run_matching_and_scoring,
    _build_evidence_dto,
    _matched_req_to_dto,
    _missing_req_to_dto,
    _convert_matches_to_dtos,
    run_matching_pipeline,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uow(repo):
    m = MagicMock()
    m.__enter__.return_value = repo
    m.__exit__.return_value = False
    return m


def _dto(job_id="j-1", content_hash="hash-1"):
    job = SimpleNamespace(id=job_id, title="Eng", company="Acme", content_hash=content_hash)
    return SimpleNamespace(job=job, overall_score=85.0, fit_score=80.0, want_score=75.0)


# ---------------------------------------------------------------------------
# _load_resume_from_db
# ---------------------------------------------------------------------------

class TestLoadResumeFromDb:
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_no_structured_resume_returns_none(self, mock_uow):
        repo = MagicMock()
        repo.resume.get_structured_resume_by_fingerprint.return_value = None
        mock_uow.return_value = _uow(repo)
        assert _load_resume_from_db("fp-abc123") is None

    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_resume_missing_extracted_data_returns_none(self, mock_uow):
        repo = MagicMock()
        sr = SimpleNamespace(extracted_data=None)
        repo.resume.get_structured_resume_by_fingerprint.return_value = sr
        mock_uow.return_value = _uow(repo)
        assert _load_resume_from_db("fp-abc123") is None

    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_returns_extracted_data(self, mock_uow):
        repo = MagicMock()
        data = {"profile": {"summary": {"text": "Engineer"}}}
        sr = SimpleNamespace(extracted_data=data)
        repo.resume.get_structured_resume_by_fingerprint.return_value = sr
        mock_uow.return_value = _uow(repo)
        assert _load_resume_from_db("fp-abc123") == data

    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_exception_returns_none(self, mock_uow):
        mock_uow.side_effect = RuntimeError("db error")
        assert _load_resume_from_db("fp-abc123") is None


# ---------------------------------------------------------------------------
# _load_requested_resume
# ---------------------------------------------------------------------------

class TestLoadRequestedResume:
    @patch("services.scorer_matcher.pipeline._load_resume_from_db")
    def test_no_data_returns_error_result(self, mock_load):
        mock_load.return_value = None
        data, re_extract, err = _load_requested_resume("fp-abc123456789ab")
        assert data is None
        assert err is not None
        assert err.success is False

    @patch("services.scorer_matcher.pipeline._load_resume_from_db")
    def test_success_returns_data(self, mock_load):
        mock_load.return_value = {"profile": {}}
        data, re_extract, err = _load_requested_resume("fp-abc123456789ab")
        assert data == {"profile": {}}
        assert re_extract is False
        assert err is None


# ---------------------------------------------------------------------------
# _result_after_matching
# ---------------------------------------------------------------------------

class TestResultAfterMatching:
    def test_not_stopped_returns_none(self):
        stop = threading.Event()
        assert _result_after_matching([], stop) is None

    def test_stopped_no_matches_returns_cancelled(self):
        stop = threading.Event()
        stop.set()
        result = _result_after_matching([], stop)
        assert result is not None
        assert result.cancelled is True
        assert result.matches_count == 0

    def test_stopped_with_matches_returns_cancelled_with_count(self):
        stop = threading.Event()
        stop.set()
        result = _result_after_matching([_dto(), _dto("j-2")], stop)
        assert result.cancelled is True
        assert result.matches_count == 2


# ---------------------------------------------------------------------------
# _result_after_saving
# ---------------------------------------------------------------------------

class TestResultAfterSaving:
    def test_not_stopped_returns_none(self):
        stop = threading.Event()
        assert _result_after_saving([_dto()], 1, stop, time.time()) is None

    def test_stopped_returns_cancelled(self):
        stop = threading.Event()
        stop.set()
        result = _result_after_saving([_dto(), _dto("j-2")], 1, stop, time.time())
        assert result.cancelled is True
        assert result.matches_count == 2
        assert result.saved_count == 1


# ---------------------------------------------------------------------------
# _finish_pipeline_result
# ---------------------------------------------------------------------------

class TestFinishPipelineResult:
    def test_not_stopped_returns_success(self):
        stop = threading.Event()
        result = _finish_pipeline_result([_dto()], 1, 0, stop, time.time())
        assert result.success is True
        assert result.matches_count == 1
        assert result.saved_count == 1
        assert result.cancelled is False

    def test_stopped_returns_cancelled(self):
        stop = threading.Event()
        stop.set()
        result = _finish_pipeline_result([_dto(), _dto("j-2")], 1, 0, stop, time.time())
        assert result.cancelled is True
        assert result.matches_count == 2


# ---------------------------------------------------------------------------
# _load_user_wants_embeddings
# ---------------------------------------------------------------------------

class TestLoadUserWantsEmbeddings:
    def test_no_file_configured_returns_empty(self):
        config = SimpleNamespace(user_wants_file=None)
        assert _load_user_wants_embeddings(config, MagicMock()) == []

    @patch("services.scorer_matcher.pipeline.os.path.exists", return_value=False)
    def test_file_not_on_disk_returns_empty(self, _):
        config = SimpleNamespace(user_wants_file="/abs/path/wants.txt")
        assert _load_user_wants_embeddings(config, MagicMock()) == []

    @patch("services.scorer_matcher.pipeline.load_user_wants_data", return_value=[])
    @patch("services.scorer_matcher.pipeline.os.path.exists", return_value=True)
    def test_empty_wants_returns_empty(self, _exists, _load):
        config = SimpleNamespace(user_wants_file="/abs/wants.txt")
        assert _load_user_wants_embeddings(config, MagicMock()) == []

    @patch("services.scorer_matcher.pipeline.load_user_wants_data", return_value=["want a"])
    @patch("services.scorer_matcher.pipeline.os.path.exists", return_value=True)
    def test_batch_embedding_path(self, _exists, _load):
        ai = MagicMock()
        ai.generate_embeddings.return_value = [[0.1, 0.2]]
        config = SimpleNamespace(user_wants_file="/abs/wants.txt")
        result = _load_user_wants_embeddings(config, ai)
        assert result == [[0.1, 0.2]]

    @patch("services.scorer_matcher.pipeline.load_user_wants_data", return_value=["want a", "want b"])
    @patch("services.scorer_matcher.pipeline.os.path.exists", return_value=True)
    def test_batch_fails_falls_back_to_per_item(self, _exists, _load):
        ai = MagicMock()
        ai.generate_embeddings.side_effect = RuntimeError("batch error")
        ai.generate_embedding.side_effect = [[0.1], [0.2]]
        config = SimpleNamespace(user_wants_file="/abs/wants.txt")
        _load_user_wants_embeddings(config, ai)
        assert ai.generate_embedding.call_count == 2

    @patch("services.scorer_matcher.pipeline.load_user_wants_data", return_value=["want a"])
    @patch("services.scorer_matcher.pipeline.os.path.exists", return_value=True)
    def test_per_item_error_skips_want(self, _exists, _load):
        ai = MagicMock(spec=[])  # no generate_embeddings
        ai.generate_embedding = MagicMock(side_effect=RuntimeError("fail"))
        config = SimpleNamespace(user_wants_file="/abs/wants.txt")
        result = _load_user_wants_embeddings(config, ai)
        assert result == []

    @patch("services.scorer_matcher.pipeline.load_user_wants_data", return_value=["want a"])
    @patch("services.scorer_matcher.pipeline.os.path.exists", return_value=False)
    @patch("services.scorer_matcher.pipeline.os.path.isabs", return_value=False)
    @patch("services.scorer_matcher.pipeline.os.path.join", return_value="/cwd/wants.txt")
    def test_relative_path_joined_with_cwd(self, mock_join, _isabs, _exists, _load):
        config = SimpleNamespace(user_wants_file="relative/wants.txt")
        _load_user_wants_embeddings(config, MagicMock())
        mock_join.assert_called_once()


# ---------------------------------------------------------------------------
# _load_structured_resume
# ---------------------------------------------------------------------------

class TestLoadStructuredResume:
    def test_should_re_extract_returns_none(self):
        repo = MagicMock()
        result = _load_structured_resume(repo, "fp", should_re_extract=True)
        assert result is None
        repo.resume.get_structured_resume_by_fingerprint.assert_not_called()

    def test_no_re_extract_queries_db(self):
        repo = MagicMock()
        sr = SimpleNamespace(extracted_data={"profile": {}})
        repo.resume.get_structured_resume_by_fingerprint.return_value = sr
        result = _load_structured_resume(repo, "fp", should_re_extract=False)
        assert result is sr


# ---------------------------------------------------------------------------
# _get_pre_extracted_resume
# ---------------------------------------------------------------------------

class TestGetPreExtractedResume:
    def test_should_re_extract_returns_none(self):
        assert _get_pre_extracted_resume(MagicMock(), should_re_extract=True) is None

    def test_no_structured_resume_returns_none(self):
        assert _get_pre_extracted_resume(None, should_re_extract=False) is None

    def test_no_extracted_data_returns_none(self):
        sr = SimpleNamespace(extracted_data=None)
        assert _get_pre_extracted_resume(sr, should_re_extract=False) is None

    @patch("services.scorer_matcher.pipeline.ResumeSchema")
    def test_valid_resume_returns_validated(self, mock_schema):
        sr = SimpleNamespace(extracted_data={"profile": {}}, resume_fingerprint="fp")
        validated = MagicMock()
        mock_schema.model_validate.return_value = validated
        result = _get_pre_extracted_resume(sr, should_re_extract=False)
        assert result is validated

    @patch("services.scorer_matcher.pipeline.ResumeSchema")
    def test_invalid_resume_raises_value_error(self, mock_schema):
        sr = SimpleNamespace(extracted_data={"bad": "data"}, resume_fingerprint="fp")
        mock_schema.model_validate.side_effect = ValueError("bad schema")
        try:
            _get_pre_extracted_resume(sr, should_re_extract=False)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "Failed to parse stored ready resume" in str(e)


# ---------------------------------------------------------------------------
# _save_matches_batch
# ---------------------------------------------------------------------------

class TestSaveMatchesBatch:
    def test_empty_list_returns_zero(self):
        config = SimpleNamespace(recalculate_existing=False)
        assert _save_matches_batch([], "fp", config) == 0

    @patch("services.scorer_matcher.pipeline.save_match_to_db")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_new_match_is_saved(self, mock_uow, mock_save):
        repo = MagicMock()
        repo.get_existing_match.return_value = None
        mock_uow.return_value = _uow(repo)
        config = SimpleNamespace(recalculate_existing=False)

        count = _save_matches_batch([_dto()], "fp", config)
        assert count == 1
        mock_save.assert_called_once()

    @patch("services.scorer_matcher.pipeline.save_match_to_db")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_existing_active_match_skipped_when_no_recalculate(self, mock_uow, mock_save):
        repo = MagicMock()
        existing = MagicMock()
        existing.status = "active"
        existing.job_content_hash = "hash-1"  # same as dto
        repo.get_existing_match.return_value = existing
        mock_uow.return_value = _uow(repo)
        config = SimpleNamespace(recalculate_existing=False)

        count = _save_matches_batch([_dto(content_hash="hash-1")], "fp", config)
        assert count == 0
        mock_save.assert_not_called()

    @patch("services.scorer_matcher.pipeline.save_match_to_db")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_stale_replacement_when_content_changed(self, mock_uow, mock_save):
        repo = MagicMock()
        existing = MagicMock()
        existing.status = "active"
        existing.job_content_hash = "old-hash"
        repo.get_existing_match.return_value = existing
        mock_uow.return_value = _uow(repo)
        config = SimpleNamespace(recalculate_existing=False)

        count = _save_matches_batch([_dto(content_hash="new-hash")], "fp", config)
        assert count == 1
        assert existing.status == "stale"
        mock_save.assert_called_once_with(scored_match=_dto(content_hash="new-hash"), repo=repo, is_stale_replacement=True)

    @patch("services.scorer_matcher.pipeline.save_match_to_db")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_exception_is_caught_returns_zero(self, mock_uow, mock_save):
        mock_uow.side_effect = RuntimeError("db error")
        config = SimpleNamespace(recalculate_existing=False)
        count = _save_matches_batch([_dto()], "fp", config)
        assert count == 0


# ---------------------------------------------------------------------------
# _log_match_results
# ---------------------------------------------------------------------------

class TestLogMatchResults:
    def test_empty_list_no_error(self):
        _log_match_results([])  # should not raise

    def test_logs_top_5(self):
        dtos = [
            SimpleNamespace(
                job=SimpleNamespace(title=f"Job {i}", company="Co"),
                overall_score=float(i * 10),
                fit_score=float(i * 10),
                want_score=0.0,
            )
            for i in range(10)
        ]
        _log_match_results(dtos)  # should not raise, logs top 5


# ---------------------------------------------------------------------------
# _load_pipeline_resume
# ---------------------------------------------------------------------------

class TestLoadPipelineResume:
    @patch("services.scorer_matcher.pipeline._load_requested_resume")
    def test_explicit_fingerprint_calls_load_requested(self, mock_load):
        mock_load.return_value = ({"profile": {}}, False, None)
        ctx = MagicMock()
        data, fp, re_extract, err = _load_pipeline_resume(ctx, "fp-explicit")
        mock_load.assert_called_once_with("fp-explicit")
        assert fp == "fp-explicit"
        assert data == {"profile": {}}
        assert err is None

    @patch("services.scorer_matcher.pipeline._load_latest_ready_resume")
    def test_no_fingerprint_calls_load_latest(self, mock_latest):
        mock_latest.return_value = ("fp-latest", {"profile": {}}, False, None)
        ctx = MagicMock()
        data, fp, re_extract, err = _load_pipeline_resume(ctx, None)
        mock_latest.assert_called_once_with(ctx)
        assert fp == "fp-latest"


# ---------------------------------------------------------------------------
# run_matching_pipeline — disabled config
# ---------------------------------------------------------------------------

class TestRunMatchingPipelineDisabled:
    def test_matching_disabled_returns_success_with_error_message(self):
        ctx = MagicMock()
        ctx.config.matching = None  # no config → treated as disabled
        result = run_matching_pipeline(ctx)
        assert result.success is True
        assert result.matches_count == 0
        assert "disabled" in (result.error or "").lower()

    def test_matching_enabled_false_returns_early(self):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=False)
        result = run_matching_pipeline(ctx)
        assert result.success is True
        assert result.matches_count == 0


# ---------------------------------------------------------------------------
# run_matching_pipeline — notification step
# ---------------------------------------------------------------------------

class TestRunMatchingPipelineNotification:
    @patch("services.scorer_matcher.pipeline.send_notifications")
    @patch("services.scorer_matcher.pipeline._save_matches_batch")
    @patch("services.scorer_matcher.pipeline._run_matching_and_scoring")
    @patch("services.scorer_matcher.pipeline._load_pipeline_resume")
    @patch("services.scorer_matcher.pipeline._load_user_wants_embeddings")
    def test_notification_sent_when_service_present(
        self, mock_wants, mock_load, mock_run, mock_save, mock_notify
    ):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True)
        ctx.notification_service = MagicMock()

        mock_load.return_value = ({"profile": {}}, "fp-abc", False, None)
        mock_wants.return_value = []
        mock_run.return_value = [_dto()]
        mock_save.return_value = 1
        mock_notify.return_value = 1

        result = run_matching_pipeline(ctx)
        mock_notify.assert_called_once()
        assert result.notified_count == 1

    @patch("services.scorer_matcher.pipeline.send_notifications")
    @patch("services.scorer_matcher.pipeline._save_matches_batch")
    @patch("services.scorer_matcher.pipeline._run_matching_and_scoring")
    @patch("services.scorer_matcher.pipeline._load_pipeline_resume")
    @patch("services.scorer_matcher.pipeline._load_user_wants_embeddings")
    def test_notification_skipped_when_stopped(
        self, mock_wants, mock_load, mock_run, mock_save, mock_notify
    ):
        stop = threading.Event()
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True)
        ctx.notification_service = MagicMock()

        mock_load.return_value = ({"profile": {}}, "fp-abc", False, None)
        mock_wants.return_value = []
        mock_run.return_value = [_dto()]
        mock_save.return_value = 1

        # After save, set stop so notification is skipped
        def set_stop(*args, **kwargs):
            stop.set()
            return 1
        mock_save.side_effect = set_stop

        result = run_matching_pipeline(ctx, stop_event=stop)
        mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# _run_scorer_service
# ---------------------------------------------------------------------------

class TestRunScorerService:
    @patch("services.scorer_matcher.pipeline._resolve_result_policy", return_value=None)
    def test_without_user_wants_calls_score_matches(self, _policy):
        scorer = MagicMock()
        scorer.score_matches.return_value = []
        result = _run_scorer_service(scorer, [], MagicMock(), [], {}, threading.Event())
        scorer.score_matches.assert_called_once()
        assert result == []

    @patch("services.scorer_matcher.pipeline._resolve_result_policy", return_value=None)
    def test_with_user_wants_passes_embeddings(self, _policy):
        scorer = MagicMock()
        scorer.score_matches.return_value = ["match1"]
        embeddings = [[0.1, 0.2]]
        facets = {"j-1": [0.3]}
        result = _run_scorer_service(scorer, [], MagicMock(), embeddings, facets, threading.Event())
        call_kwargs = scorer.score_matches.call_args.kwargs
        assert "user_want_embeddings" in call_kwargs
        assert call_kwargs["user_want_embeddings"] == embeddings
        assert result == ["match1"]


# ---------------------------------------------------------------------------
# _resolve_result_policy
# ---------------------------------------------------------------------------

class TestResolveResultPolicy:
    @patch("services.scorer_matcher.pipeline.get_result_policy_store")
    def test_uses_store_when_available(self, mock_store):
        policy = MagicMock()
        mock_store.return_value.get_current_policy.return_value = policy
        config = SimpleNamespace(result_policy="config_policy")
        result = _resolve_result_policy(config)
        assert result is policy

    @patch("services.scorer_matcher.pipeline.get_result_policy_store")
    def test_falls_back_to_config_on_exception(self, mock_store):
        mock_store.return_value.get_current_policy.side_effect = RuntimeError("store error")
        config = SimpleNamespace(result_policy="fallback_policy")
        result = _resolve_result_policy(config)
        assert result == "fallback_policy"


# ---------------------------------------------------------------------------
# _build_job_facet_embeddings_map
# ---------------------------------------------------------------------------

class TestBuildJobFacetEmbeddingsMap:
    def test_builds_map_per_unique_job(self):
        facets = {"j-1": [0.1, 0.2], "j-2": [0.3, 0.4]}
        repo = MagicMock()
        repo.get_job_facet_embeddings.side_effect = lambda job_id: facets.get(str(job_id), [])

        pm1 = SimpleNamespace(job=SimpleNamespace(id="j-1"))
        pm2 = SimpleNamespace(job=SimpleNamespace(id="j-2"))
        pm3 = SimpleNamespace(job=SimpleNamespace(id="j-1"))  # duplicate

        result = _build_job_facet_embeddings_map(repo, [pm1, pm2, pm3])
        assert set(result.keys()) == {"j-1", "j-2"}
        assert repo.get_job_facet_embeddings.call_count == 2  # deduped


# ---------------------------------------------------------------------------
# DTO conversion helpers
# ---------------------------------------------------------------------------

class TestBuildEvidenceDto:
    def test_none_returns_none(self):
        assert _build_evidence_dto(None) is None

    def test_non_none_builds_dto(self):
        evidence = SimpleNamespace(text="Good match", source_section="skills", tags=["python"])
        dto = _build_evidence_dto(evidence)
        assert dto.text == "Good match"
        assert dto.source_section == "skills"
        assert dto.tags == ["python"]


class TestMatchedReqToDto:
    def test_builds_dto(self):
        req = SimpleNamespace(
            requirement=SimpleNamespace(id="r-1", req_type="required"),
            evidence=SimpleNamespace(text="ev", source_section="exp", tags=[]),
            similarity=0.9,
            is_covered=True,
        )
        dto = _matched_req_to_dto(req)
        assert dto.requirement.id == "r-1"
        assert dto.requirement.req_type == "required"
        assert dto.similarity == 0.9
        assert dto.is_covered is True
        assert dto.evidence is not None


class TestMissingReqToDto:
    def test_builds_dto_with_is_covered_false(self):
        req = SimpleNamespace(
            requirement=SimpleNamespace(id="r-2", req_type="preferred"),
            similarity=0.3,
        )
        dto = _missing_req_to_dto(req)
        assert dto.requirement.id == "r-2"
        assert dto.is_covered is False
        assert dto.similarity == 0.3


class TestConvertMatchesToDtos:
    def test_empty_returns_empty(self):
        assert _convert_matches_to_dtos([]) == []

    def test_converts_match_to_dto(self):
        match = SimpleNamespace(
            job=SimpleNamespace(
                id="j-1",
                title="Eng",
                company="Acme",
                location_text="Remote",
                is_remote=True,
                content_hash="hash-1",
            ),
            overall_score=85.0,
            fit_score=80.0,
            want_score=75.0,
            job_similarity=0.9,
            jd_required_coverage=0.8,
            jd_preferences_coverage=0.7,
            matched_requirements=[],
            missing_requirements=[],
            resume_fingerprint="fp-abc",
            fit_components=None,
            want_components=None,
            base_score=70.0,
            penalties=0.0,
            penalty_details=None,
            fit_weight=0.6,
            want_weight=0.4,
            match_type="requirements_only",
        )
        dtos = _convert_matches_to_dtos([match])
        assert len(dtos) == 1
        dto = dtos[0]
        assert dto.job.id == "j-1"
        assert dto.overall_score == 85.0
        assert dto.fit_score == 80.0
        assert dto.resume_fingerprint == "fp-abc"


# ---------------------------------------------------------------------------
# load_user_wants_data (file-level)
# ---------------------------------------------------------------------------

class TestLoadUserWantsData:
    def test_file_not_found_returns_empty(self, tmp_path):
        result = load_user_wants_data(str(tmp_path / "nonexistent.txt"))
        assert result == []

    def test_exception_returns_empty(self):
        # Passing a directory triggers an error inside open()
        import os
        result = load_user_wants_data("/")  # open("/") raises IsADirectoryError
        assert result == []

    def test_reads_lines(self, tmp_path):
        f = tmp_path / "wants.txt"
        f.write_text("want a\nwant b\n\n")
        result = load_user_wants_data(str(f))
        assert result == ["want a", "want b"]


# ---------------------------------------------------------------------------
# _load_latest_ready_resume
# ---------------------------------------------------------------------------

class TestLoadLatestReadyResume:
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_no_fingerprint_no_state_returns_error(self, mock_uow):
        repo = MagicMock()
        repo.get_latest_ready_resume_fingerprint.return_value = None
        repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value = _uow(repo)

        ctx = MagicMock()
        fp, data, re_extract, err = _load_latest_ready_resume(ctx)
        assert fp is None
        assert err is not None
        assert err.success is False

    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_fingerprint_with_valid_data_returns_success(self, mock_uow):
        repo = MagicMock()
        repo.get_latest_ready_resume_fingerprint.return_value = "fp-abc123456789ab"
        repo.get_latest_resume_processing_state.return_value = None
        sr = SimpleNamespace(extracted_data={"profile": {}})
        repo.resume.get_structured_resume_by_fingerprint.return_value = sr
        mock_uow.return_value = _uow(repo)

        ctx = MagicMock()
        fp, data, re_extract, err = _load_latest_ready_resume(ctx)
        assert fp == "fp-abc123456789ab"
        assert data == {"profile": {}}
        assert err is None

    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_fingerprint_missing_extracted_data_returns_error(self, mock_uow):
        repo = MagicMock()
        repo.get_latest_ready_resume_fingerprint.return_value = "fp-abc123456789ab"
        repo.get_latest_resume_processing_state.return_value = None
        sr = SimpleNamespace(extracted_data=None)
        repo.resume.get_structured_resume_by_fingerprint.return_value = sr
        mock_uow.return_value = _uow(repo)

        ctx = MagicMock()
        fp, data, re_extract, err = _load_latest_ready_resume(ctx)
        assert err is not None
        assert err.success is False

    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_processing_in_progress_returns_error(self, mock_uow):
        repo = MagicMock()
        repo.get_latest_ready_resume_fingerprint.return_value = None
        state = SimpleNamespace(processing_status="extracting")
        repo.get_latest_resume_processing_state.return_value = state
        mock_uow.return_value = _uow(repo)

        ctx = MagicMock()
        fp, data, re_extract, err = _load_latest_ready_resume(ctx)
        assert err is not None
        assert "processing" in (err.error or "").lower()


# ---------------------------------------------------------------------------
# run_matching_pipeline — error_result and exception paths
# ---------------------------------------------------------------------------

class TestRunMatchingPipelineErrorPaths:
    @patch("services.scorer_matcher.pipeline._load_pipeline_resume")
    @patch("services.scorer_matcher.pipeline._load_user_wants_embeddings")
    def test_error_result_from_load_resume_returned_early(self, mock_wants, mock_load):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True)
        error = MatchingPipelineResult(
            success=False, matches_count=0, saved_count=0, notified_count=0,
            error="Resume not found",
        )
        mock_load.return_value = (None, None, False, error)
        result = run_matching_pipeline(ctx)
        assert result.success is False
        assert "Resume not found" in (result.error or "")

    @patch("services.scorer_matcher.pipeline._load_pipeline_resume")
    @patch("services.scorer_matcher.pipeline._load_user_wants_embeddings")
    def test_exception_in_pipeline_returns_failure(self, mock_wants, mock_load):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True)
        mock_load.side_effect = RuntimeError("unexpected crash")
        result = run_matching_pipeline(ctx)
        assert result.success is False
        assert "unexpected crash" in (result.error or "")

    @patch("services.scorer_matcher.pipeline.send_notifications")
    @patch("services.scorer_matcher.pipeline._save_matches_batch")
    @patch("services.scorer_matcher.pipeline._run_matching_and_scoring")
    @patch("services.scorer_matcher.pipeline._load_pipeline_resume")
    @patch("services.scorer_matcher.pipeline._load_user_wants_embeddings")
    def test_status_callback_invoked(self, mock_wants, mock_load, mock_run, mock_save, mock_notify):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True)
        ctx.notification_service = None

        mock_load.return_value = ({"profile": {}}, "fp-abc", False, None)
        mock_wants.return_value = []
        mock_run.return_value = [_dto()]
        mock_save.return_value = 1
        mock_notify.return_value = 0

        status_calls = []
        run_matching_pipeline(ctx, status_callback=status_calls.append)
        assert "saving_results" in status_calls


# ---------------------------------------------------------------------------
# _log_resume_preparation
# ---------------------------------------------------------------------------

class TestLogResumePreparation:
    def test_structured_resume_logs_loaded(self):
        sr = SimpleNamespace(total_experience_years=5)
        _log_resume_preparation(sr, "fp-abc")  # should not raise

    def test_no_structured_resume_logs_re_extract(self):
        _log_resume_preparation(None, "fp-abc")  # should not raise


# ---------------------------------------------------------------------------
# _prepare_matching_run
# ---------------------------------------------------------------------------

class TestPrepareMatchingRun:
    @patch("services.scorer_matcher.pipeline._prepare_matcher_service")
    @patch("services.scorer_matcher.pipeline._load_structured_resume")
    def test_raises_when_no_resume_and_no_re_extract(self, mock_load, mock_matcher):
        mock_load.return_value = None
        ctx = MagicMock()
        repo = MagicMock()
        config = MagicMock()
        try:
            _prepare_matching_run(ctx, repo, config, "fp-abc", should_re_extract=False)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "Resume not found" in str(e)

    @patch("services.scorer_matcher.pipeline._prepare_matcher_service")
    @patch("services.scorer_matcher.pipeline._load_structured_resume")
    def test_returns_resume_and_matcher_on_success(self, mock_load, mock_matcher):
        sr = SimpleNamespace(extracted_data={"profile": {}}, total_experience_years=3)
        mock_load.return_value = sr
        matcher = MagicMock()
        mock_matcher.return_value = matcher

        ctx = MagicMock()
        repo = MagicMock()
        config = MagicMock()
        result_sr, result_matcher = _prepare_matching_run(ctx, repo, config, "fp-abc", False)
        assert result_sr is sr
        assert result_matcher is matcher


# ---------------------------------------------------------------------------
# _run_preliminary_matching
# ---------------------------------------------------------------------------

class TestRunPreliminaryMatching:
    @patch("services.scorer_matcher.pipeline._run_vector_matching")
    @patch("services.scorer_matcher.pipeline._get_pre_extracted_resume")
    def test_calls_vector_matching_and_returns_results(self, mock_pre, mock_vector):
        mock_pre.return_value = None
        mock_vector.return_value = ["m1", "m2"]

        matcher = MagicMock()
        repo = MagicMock()
        status_calls = []
        results = _run_preliminary_matching(
            matcher, repo, {"profile": {}}, threading.Event(),
            status_calls.append, None, False, "fp-abc",
        )
        assert results == ["m1", "m2"]
        assert "vector_matching" in status_calls


# ---------------------------------------------------------------------------
# _run_matching_and_scoring
# ---------------------------------------------------------------------------

class TestRunMatchingAndScoring:
    def _make_repo_uow(self, repo):
        m = MagicMock()
        m.__enter__.return_value = repo
        m.__exit__.return_value = False
        return m

    @patch("services.scorer_matcher.pipeline._convert_matches_to_dtos")
    @patch("services.scorer_matcher.pipeline._run_scorer_service")
    @patch("services.scorer_matcher.pipeline.ScoringService")
    @patch("services.scorer_matcher.pipeline._build_job_facet_embeddings_map")
    @patch("services.scorer_matcher.pipeline._run_preliminary_matching")
    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_normal_path_returns_dtos(
        self, mock_uow, mock_prepare, mock_prelim, mock_facets,
        mock_scorer_cls, mock_score, mock_convert,
    ):
        repo = MagicMock()
        mock_uow.return_value = self._make_repo_uow(repo)
        sr = SimpleNamespace(extracted_data={}, total_experience_years=2)
        mock_prepare.return_value = (sr, MagicMock())
        mock_prelim.return_value = []
        mock_facets.return_value = {}
        mock_score.return_value = []
        mock_convert.return_value = [_dto()]

        ctx = MagicMock()
        result = _run_matching_and_scoring(
            ctx, {"profile": {}}, "fp-abc", False,
            MagicMock(), [], threading.Event(), None,
        )
        assert result == [_dto()]

    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_stop_after_prepare_returns_empty(self, mock_uow, mock_prepare):
        repo = MagicMock()
        mock_uow.return_value = self._make_repo_uow(repo)
        stop = threading.Event()

        def prepare_and_stop(*args, **kwargs):
            stop.set()
            return (MagicMock(), MagicMock())
        mock_prepare.side_effect = prepare_and_stop

        ctx = MagicMock()
        result = _run_matching_and_scoring(
            ctx, {"profile": {}}, "fp-abc", False,
            MagicMock(), [], stop, None,
        )
        assert result == []  # stopped after prepare

    @patch("services.scorer_matcher.pipeline._convert_matches_to_dtos")
    @patch("services.scorer_matcher.pipeline._run_scorer_service")
    @patch("services.scorer_matcher.pipeline.ScoringService")
    @patch("services.scorer_matcher.pipeline._build_job_facet_embeddings_map")
    @patch("services.scorer_matcher.pipeline._run_preliminary_matching")
    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_stop_after_preliminary_returns_empty(
        self, mock_uow, mock_prepare, mock_prelim, mock_facets,
        mock_scorer_cls, mock_score, mock_convert,
    ):
        repo = MagicMock()
        mock_uow.return_value = self._make_repo_uow(repo)
        stop = threading.Event()

        sr = SimpleNamespace(extracted_data={}, total_experience_years=2)
        mock_prepare.return_value = (sr, MagicMock())

        def prelim_and_stop(*args, **kwargs):
            stop.set()
            return []
        mock_prelim.side_effect = prelim_and_stop

        ctx = MagicMock()
        result = _run_matching_and_scoring(
            ctx, {"profile": {}}, "fp-abc", False,
            MagicMock(), [], stop, None,
        )
        assert result == []  # stopped after preliminary

    @patch("services.scorer_matcher.pipeline._convert_matches_to_dtos")
    @patch("services.scorer_matcher.pipeline._run_scorer_service")
    @patch("services.scorer_matcher.pipeline.ScoringService")
    @patch("services.scorer_matcher.pipeline._build_job_facet_embeddings_map")
    @patch("services.scorer_matcher.pipeline._run_preliminary_matching")
    @patch("services.scorer_matcher.pipeline._prepare_matching_run")
    @patch("services.scorer_matcher.pipeline.job_uow")
    def test_status_callback_invoked(
        self, mock_uow, mock_prepare, mock_prelim, mock_facets,
        mock_scorer_cls, mock_score, mock_convert,
    ):
        repo = MagicMock()
        mock_uow.return_value = self._make_repo_uow(repo)
        sr = SimpleNamespace(extracted_data={}, total_experience_years=2)
        mock_prepare.return_value = (sr, MagicMock())
        mock_prelim.return_value = []
        mock_facets.return_value = {}
        mock_score.return_value = []
        mock_convert.return_value = []

        ctx = MagicMock()
        status_calls = []
        _run_matching_and_scoring(
            ctx, {"profile": {}}, "fp-abc", False,
            MagicMock(), [], threading.Event(), status_calls.append,
        )
        assert "loading_resume" in status_calls
        assert "scoring" in status_calls


# ---------------------------------------------------------------------------
# run_matching_pipeline — matching cancelled path (line 334)
# ---------------------------------------------------------------------------

class TestRunMatchingPipelineCancelledAfterMatching:
    @patch("services.scorer_matcher.pipeline._run_matching_and_scoring")
    @patch("services.scorer_matcher.pipeline._load_pipeline_resume")
    @patch("services.scorer_matcher.pipeline._load_user_wants_embeddings")
    def test_returns_cancelled_when_stop_set_after_matching(self, mock_wants, mock_load, mock_run):
        stop = threading.Event()
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True)

        mock_load.return_value = ({"profile": {}}, "fp-abc", False, None)
        mock_wants.return_value = []

        def run_and_stop(*args, **kwargs):
            stop.set()
            return [_dto()]
        mock_run.side_effect = run_and_stop

        result = run_matching_pipeline(ctx, stop_event=stop)
        assert result.cancelled is True

    @patch("services.scorer_matcher.pipeline.send_notifications")
    @patch("services.scorer_matcher.pipeline._save_matches_batch")
    @patch("services.scorer_matcher.pipeline._run_matching_and_scoring")
    @patch("services.scorer_matcher.pipeline._load_pipeline_resume")
    @patch("services.scorer_matcher.pipeline._load_user_wants_embeddings")
    def test_status_callback_notifying_invoked(
        self, mock_wants, mock_load, mock_run, mock_save, mock_notify
    ):
        ctx = MagicMock()
        ctx.config.matching = SimpleNamespace(enabled=True)
        ctx.notification_service = MagicMock()

        mock_load.return_value = ({"profile": {}}, "fp-abc", False, None)
        mock_wants.return_value = []
        mock_run.return_value = [_dto()]
        mock_save.return_value = 1
        mock_notify.return_value = 1

        status_calls = []
        run_matching_pipeline(ctx, status_callback=status_calls.append)
        assert "notifying" in status_calls


# ---------------------------------------------------------------------------
# _prepare_matcher_service
# ---------------------------------------------------------------------------

class TestPrepareMatcherService:
    @patch("services.scorer_matcher.pipeline.MatcherService")
    @patch("services.scorer_matcher.pipeline.ResumeProfiler")
    @patch("services.scorer_matcher.pipeline.JobRepositoryAdapter")
    def test_creates_matcher_service(self, mock_adapter, mock_profiler, mock_matcher_cls):
        from services.scorer_matcher.pipeline import _prepare_matcher_service
        ctx = MagicMock()
        repo = MagicMock()
        config = SimpleNamespace(matcher=MagicMock())
        _prepare_matcher_service(ctx, repo, config)
        mock_matcher_cls.assert_called_once()


# ---------------------------------------------------------------------------
# _run_vector_matching
# ---------------------------------------------------------------------------

class TestRunVectorMatching:
    def test_delegates_to_matcher(self):
        from services.scorer_matcher.pipeline import _run_vector_matching
        matcher = MagicMock()
        matcher.match_resume_two_stage.return_value = ["m1"]
        repo = MagicMock()
        stop = threading.Event()
        result = _run_vector_matching(matcher, repo, {"profile": {}}, stop, None, "fp-abc")
        assert result == ["m1"]
        matcher.match_resume_two_stage.assert_called_once()
