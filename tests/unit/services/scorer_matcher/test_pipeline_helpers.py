#!/usr/bin/env python3
"""Unit tests for scorer_matcher pipeline helper functions."""

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.scorer_matcher.pipeline import (
    MatchingPipelineResult,
    _load_resume_from_db,
    _load_requested_resume,
    _result_after_matching,
    _result_after_saving,
    _finish_pipeline_result,
    _load_user_wants_embeddings,
    _load_structured_resume,
    _get_pre_extracted_resume,
    _save_matches_batch,
    _log_match_results,
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
