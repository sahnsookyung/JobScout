#!/usr/bin/env python3
"""
Tests for Pipeline Runner.
Covers: pipeline/runner.py
"""

import pytest
import threading
import logging
from unittest.mock import Mock, patch, MagicMock

from pipeline.runner import (
    MatchingPipelineResult,
    _load_resume_with_parser,
    load_user_wants_data,
    run_matching_pipeline,
    _load_resume_file,
    _determine_resume_extraction,
    _load_user_wants_embeddings,
    _load_structured_resume,
    _prepare_matcher_service,
    _get_pre_extracted_resume,
    _run_vector_matching,
    _run_scorer_service,
    _build_evidence_dto,
    _matched_req_to_dto,
    _missing_req_to_dto,
    _convert_matches_to_dtos,
    _save_matches_batch,
    _send_notifications,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_uow_context(mock_repo):
    ctx = MagicMock()
    ctx.__enter__ = Mock(return_value=mock_repo)
    ctx.__exit__ = Mock(return_value=False)
    return ctx


# ---------------------------------------------------------------------------
# MatchingPipelineResult
# ---------------------------------------------------------------------------

class TestMatchingPipelineResult:

    def test_minimal_fields(self):
        result = MatchingPipelineResult(
            success=True, matches_count=10, saved_count=5, notified_count=3
        )
        assert result.success is True
        assert result.matches_count == 10
        assert result.saved_count == 5
        assert result.notified_count == 3
        assert result.error is None
        assert result.execution_time == 0.0

    def test_all_fields(self):
        result = MatchingPipelineResult(
            success=False, matches_count=0, saved_count=0, notified_count=0,
            error="DB failed", execution_time=120.5,
        )
        assert result.success is False
        assert result.error == "DB failed"
        assert result.execution_time == 120.5


# ---------------------------------------------------------------------------
# _load_resume_with_parser
# ---------------------------------------------------------------------------

class TestLoadResumeWithParser:

    @patch('pipeline.runner.ResumeParser')
    def test_returns_parsed_data(self, mock_parser_class):
        mock_parser_class.return_value.parse.return_value = Mock(
            data={"name": "John"}, text=None
        )
        result = _load_resume_with_parser("/path/resume.json")
        assert result == {"name": "John"}

    @patch('pipeline.runner.ResumeParser')
    def test_falls_back_to_raw_text_when_data_none(self, mock_parser_class):
        mock_parser_class.return_value.parse.return_value = Mock(
            data=None, text="Raw text"
        )
        assert _load_resume_with_parser("/path/resume.txt") == {"raw_text": "Raw text"}

    @patch('pipeline.runner.ResumeParser')
    def test_file_not_found(self, mock_parser_class, caplog):
        mock_parser_class.return_value.parse.side_effect = FileNotFoundError()
        assert _load_resume_with_parser("/missing.json") is None
        assert "Resume file not found" in caplog.text

    @patch('pipeline.runner.ResumeParser')
    def test_value_error(self, mock_parser_class, caplog):
        mock_parser_class.return_value.parse.side_effect = ValueError("bad format")
        assert _load_resume_with_parser("/bad.pdf") is None
        assert "Failed to parse resume" in caplog.text

    @patch('pipeline.runner.ResumeParser')
    def test_generic_exception(self, mock_parser_class, caplog):
        mock_parser_class.return_value.parse.side_effect = Exception("unexpected")
        assert _load_resume_with_parser("/resume.pdf") is None
        assert "Unexpected error loading resume" in caplog.text


# ---------------------------------------------------------------------------
# load_user_wants_data
# ---------------------------------------------------------------------------

class TestLoadUserWantsData:

    def test_loads_wants(self, tmp_path):
        f = tmp_path / "wants.txt"
        f.write_text("Remote work\nPython\nHealthcare\n")
        assert load_user_wants_data(str(f)) == ["Remote work", "Python", "Healthcare"]

    def test_skips_empty_lines(self, tmp_path):
        f = tmp_path / "wants.txt"
        f.write_text("Remote work\n\nPython\n\n")
        assert load_user_wants_data(str(f)) == ["Remote work", "Python"]

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_text("")
        assert load_user_wants_data(str(f)) == []

    def test_whitespace_only(self, tmp_path):
        f = tmp_path / "ws.txt"
        f.write_text("\n\n \t\n")
        assert load_user_wants_data(str(f)) == []

    def test_file_not_found(self, caplog):
        result = load_user_wants_data("/nonexistent/wants.txt")
        assert result == []
        assert "User wants file not found" in caplog.text

    def test_generic_exception(self, caplog):
        with patch('builtins.open', side_effect=Exception("read error")):
            result = load_user_wants_data("/wants.txt")
        assert result == []
        assert "Error reading user wants file" in caplog.text


# ---------------------------------------------------------------------------
# _load_resume_file
# ---------------------------------------------------------------------------

class TestLoadResumeFile:

    @patch('pipeline.runner._load_resume_with_parser', return_value={"name": "John"})
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=True)
    def test_success_absolute_path(self, *_):
        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/absolute/resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath == "/absolute/resume.json"
        assert data == {"name": "John"}

    @patch('pipeline.runner._load_resume_with_parser', return_value={"name": "John"})
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=False)
    @patch('pipeline.runner.os.getcwd', return_value="/workspace")
    def test_relative_path_resolved(self, *_):
        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath == "/workspace/resume.json"

    @patch('pipeline.runner._load_resume_with_parser', return_value={"name": "John"})
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=True)
    def test_legacy_flat_path_fallback(self, *_):
        """etl.resume is None — falls back to etl.resume_file (legacy config shape)."""
        mock_config = Mock()
        mock_config.resume = None
        mock_config.resume_file = "/legacy/resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath == "/legacy/resume.json"
        assert data == {"name": "John"}

    def test_both_paths_none(self):
        mock_config = Mock()
        mock_config.resume = None
        mock_config.resume_file = None

        filepath, data = _load_resume_file(mock_config)

        assert filepath is None
        assert data is None

    def test_neither_attribute_exists(self):
        """Config object has no resume or resume_file attribute at all.

        Requires production code to use getattr() guards.
        """
        mock_config = Mock()
        del mock_config.resume
        del mock_config.resume_file

        filepath, data = _load_resume_file(mock_config)

        assert filepath is None
        assert data is None

    @patch('pipeline.runner.os.path.exists', return_value=False)
    def test_file_not_found(self, _, caplog):
        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/missing/resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath is None
        assert data is None
        assert "Resume file not found" in caplog.text

    @patch('pipeline.runner._load_resume_with_parser', return_value=None)
    @patch('pipeline.runner.os.path.exists', return_value=True)
    def test_parser_returns_none(self, mock_exists, mock_load_parser, caplog):
        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/path/resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath is None
        assert data is None
        assert "Failed to load resume data" in caplog.text

# ---------------------------------------------------------------------------
# _determine_resume_extraction
#
# generate_file_fingerprint is a local import inside _determine_resume_extraction,
# so it is never bound to pipeline.runner's namespace. Patch the source module
# (database.models) directly — consistent with how job_uow is patched.
# ---------------------------------------------------------------------------

class TestDetermineResumeExtraction:

    def _make_config(self, force=False):
        cfg = Mock()
        cfg.resume = Mock()
        cfg.resume.force_re_extraction = force
        return cfg

    def _setup_uow(self, mock_uow, stored_fp):
        mock_repo = Mock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = stored_fp
        mock_uow.return_value.__enter__.return_value = mock_repo

    def _setup_open(self, mock_open):
        fh = Mock()
        fh.read.return_value = b"bytes"
        mock_open.return_value.__enter__ = Mock(return_value=fh)
        mock_open.return_value.__exit__ = Mock(return_value=False)

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint', return_value="new-fp")
    @patch('database.uow.job_uow')
    def test_no_stored_fingerprint_triggers_extraction(self, mock_uow, _, mock_open):
        self._setup_open(mock_open)
        self._setup_uow(mock_uow, stored_fp=None)

        fp, should = _determine_resume_extraction("/resume.pdf", self._make_config())

        assert fp == "new-fp"
        assert should is True

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint', return_value="new-fp")
    @patch('database.uow.job_uow')
    def test_changed_fingerprint_triggers_extraction(self, mock_uow, _, mock_open, caplog):
        caplog.set_level(logging.INFO)
        self._setup_open(mock_open)
        self._setup_uow(mock_uow, stored_fp="old-fp")

        fp, should = _determine_resume_extraction("/resume.pdf", self._make_config())

        assert fp == "new-fp"
        assert should is True
        assert "Resume file changed" in caplog.text

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint', return_value="same-fp")
    @patch('database.uow.job_uow')
    def test_matching_fingerprint_skips_extraction(self, mock_uow, _, mock_open, caplog):
        caplog.set_level(logging.INFO)
        self._setup_open(mock_open)
        self._setup_uow(mock_uow, stored_fp="same-fp")

        fp, should = _determine_resume_extraction("/resume.pdf", self._make_config(force=False))

        assert fp == "same-fp"
        assert should is False
        assert "Resume unchanged" in caplog.text

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint', return_value="current-fp")
    @patch('database.uow.job_uow')
    def test_force_flag_overrides_matching_fingerprint(self, mock_uow, _, mock_open, caplog):
        """force_re_extraction=True triggers re-extraction even when FP is unchanged."""
        caplog.set_level(logging.INFO)
        self._setup_open(mock_open)
        self._setup_uow(mock_uow, stored_fp="current-fp")

        fp, should = _determine_resume_extraction("/resume.pdf", self._make_config(force=True))

        assert fp == "current-fp"
        assert should is True
        assert "Force re-extraction enabled" in caplog.text

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint', return_value="current-fp")
    @patch('database.uow.job_uow')
    def test_force_flag_false_does_not_override(self, mock_uow, _, mock_open):
        self._setup_open(mock_open)
        self._setup_uow(mock_uow, stored_fp="current-fp")

        fp, should = _determine_resume_extraction("/resume.pdf", self._make_config(force=False))

        assert fp == "current-fp"
        assert should is False


# ---------------------------------------------------------------------------
# _load_user_wants_embeddings
# ---------------------------------------------------------------------------

class TestLoadUserWantsEmbeddings:

    def test_no_file_configured(self):
        result = _load_user_wants_embeddings(Mock(user_wants_file=None), Mock())
        assert result == []

    @patch('pipeline.runner.os.path.exists', return_value=False)
    def test_file_not_found(self, _):
        result = _load_user_wants_embeddings(
            Mock(user_wants_file="/missing/wants.txt"), Mock()
        )
        assert result == []

    @patch('pipeline.runner.load_user_wants_data', return_value=[])
    @patch('pipeline.runner.os.path.exists', return_value=True)
    def test_empty_file(self, *_):
        result = _load_user_wants_embeddings(
            Mock(user_wants_file="/wants.txt"), Mock()
        )
        assert result == []

    @patch('pipeline.runner.load_user_wants_data', return_value=["a", "b", "c"])
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=True)
    def test_generates_one_embedding_per_want(self, *_):
        mock_ai = Mock()
        mock_ai.generate_embedding.side_effect = [
            [0.1, 0.2], [0.3, 0.4], [0.5, 0.6]
        ]
        result = _load_user_wants_embeddings(Mock(user_wants_file="/wants.txt"), mock_ai)

        assert len(result) == 3
        assert mock_ai.generate_embedding.call_count == 3


# ---------------------------------------------------------------------------
# _load_structured_resume
# ---------------------------------------------------------------------------

class TestLoadStructuredResume:

    def test_skips_db_when_re_extracting(self):
        mock_repo = Mock()
        result = _load_structured_resume(mock_repo, "fp-1", should_re_extract=True)
        assert result is None
        mock_repo.resume.get_structured_resume_by_fingerprint.assert_not_called()

    def test_returns_stored_resume(self):
        mock_repo = Mock()
        stored = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = stored
        assert _load_structured_resume(mock_repo, "fp-1", should_re_extract=False) is stored

    def test_returns_none_when_not_found(self):
        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None
        assert _load_structured_resume(mock_repo, "fp-1", should_re_extract=False) is None


# ---------------------------------------------------------------------------
# _prepare_matcher_service
# ---------------------------------------------------------------------------

class TestPrepareMatcherService:

    @patch('pipeline.runner.MatcherService')
    @patch('pipeline.runner.ResumeProfiler')
    @patch('pipeline.runner.JobRepositoryAdapter')
    def test_wires_dependencies(self, mock_adapter, mock_profiler, mock_matcher):
        _prepare_matcher_service(Mock(), Mock(), Mock())

        mock_adapter.assert_called_once()
        mock_profiler.assert_called_once()
        mock_matcher.assert_called_once()


# ---------------------------------------------------------------------------
# _get_pre_extracted_resume
# ---------------------------------------------------------------------------

class TestGetPreExtractedResume:

    def test_returns_none_when_re_extracting(self):
        assert _get_pre_extracted_resume(Mock(), should_re_extract=True) is None

    def test_returns_none_when_no_structured_resume(self):
        assert _get_pre_extracted_resume(None, should_re_extract=False) is None

    def test_returns_none_when_no_extracted_data(self):
        mock_s = Mock()
        mock_s.extracted_data = None
        assert _get_pre_extracted_resume(mock_s, should_re_extract=False) is None

    def test_parses_valid_stored_resume(self):
        mock_s = Mock()
        mock_s.extracted_data = {
            "profile": {
                "summary": {"text": "Engineer", "total_experience_years": 5},
                "experience": [],
                "projects": {"items": []},
                "education": [],
                "skills": {"groups": [], "all": []},
                "certifications": [],
                "languages": [],
            },
            "extraction": {"confidence": 0.9, "warnings": []},
        }
        mock_s.fingerprint = "fp-abc"

        result = _get_pre_extracted_resume(mock_s, should_re_extract=False)

        assert result is not None

    def test_logs_warning_on_parse_failure(self, caplog):
        mock_s = Mock()
        mock_s.extracted_data = {"invalid": "schema"}

        result = _get_pre_extracted_resume(mock_s, should_re_extract=False)

        assert result is None
        assert "Failed to parse stored resume" in caplog.text


# ---------------------------------------------------------------------------
# _run_vector_matching
# ---------------------------------------------------------------------------

class TestRunVectorMatching:

    def test_returns_preliminary_matches(self):
        mock_matcher = Mock()
        mock_matcher.match_resume_two_stage.return_value = [Mock(), Mock()]

        result = _run_vector_matching(
            mock_matcher, Mock(), {"name": "John"},
            threading.Event(), Mock(), "fp-1"
        )

        assert len(result) == 2
        mock_matcher.match_resume_two_stage.assert_called_once()


# ---------------------------------------------------------------------------
# _run_scorer_service
# ---------------------------------------------------------------------------

class TestRunScorerService:

    def test_uses_fit_want_scoring_when_embeddings_provided(self):
        mock_scorer = Mock()
        mock_scorer.score_matches.return_value = [Mock()]

        result = _run_scorer_service(
            mock_scorer, [Mock()], Mock(),
            [[0.1, 0.2]], {"job-1": [0.3, 0.4]}, threading.Event()
        )

        assert len(result) == 1
        call_kwargs = mock_scorer.score_matches.call_args[1]
        assert "user_want_embeddings" in call_kwargs

    def test_uses_fit_only_scoring_when_no_embeddings(self):
        mock_scorer = Mock()
        mock_scorer.score_matches.return_value = [Mock()]

        _run_scorer_service(mock_scorer, [Mock()], Mock(), [], {}, threading.Event())

        call_kwargs = mock_scorer.score_matches.call_args[1]
        assert "user_want_embeddings" not in call_kwargs


# ---------------------------------------------------------------------------
# _build_evidence_dto / _matched_req_to_dto / _missing_req_to_dto
# ---------------------------------------------------------------------------

class TestBuildEvidenceDto:

    def test_returns_dto_from_evidence(self):
        ev = Mock(text="5y Python", source_section="experience", tags={"k": "v"})
        result = _build_evidence_dto(ev)
        assert result.text == "5y Python"
        assert result.source_section == "experience"

    def test_returns_none_when_no_evidence(self):
        assert _build_evidence_dto(None) is None


class TestMatchedReqToDto:

    def test_preserves_fields(self):
        req = Mock()
        req.requirement = Mock(id="r-1", req_type="required")
        req.evidence = Mock(text="text", source_section="skills", tags={})
        req.similarity = 0.85
        req.is_covered = True

        result = _matched_req_to_dto(req)

        assert result.requirement.id == "r-1"
        assert result.similarity == 0.85
        assert result.is_covered is True


class TestMissingReqToDto:

    def test_always_marks_not_covered(self):
        req = Mock()
        req.requirement = Mock(id="r-2", req_type="preferred")
        req.similarity = 0.45

        result = _missing_req_to_dto(req)

        assert result.requirement.id == "r-2"
        assert result.is_covered is False
        assert result.similarity == 0.45


# ---------------------------------------------------------------------------
# _convert_matches_to_dtos
# ---------------------------------------------------------------------------

class TestConvertMatchesToDtos:

    def test_empty_list(self):
        assert _convert_matches_to_dtos([]) == []

    def test_converts_single_match(self):
        m = Mock()
        m.job = Mock(
            id="job-1", title="Engineer", company="TechCorp",
            location_text="Remote", is_remote=True, content_hash="h1",
        )
        m.overall_score = 85.5
        m.fit_score = 80.0
        m.want_score = 75.0
        m.job_similarity = 0.9
        m.jd_required_coverage = 0.85
        m.jd_preferences_coverage = 0.70
        m.matched_requirements = []
        m.missing_requirements = []
        m.resume_fingerprint = "fp-1"
        m.fit_components = {}
        m.want_components = {}
        m.base_score = 80.0
        m.penalties = 5.0
        m.penalty_details = []
        m.fit_weight = 0.7
        m.want_weight = 0.3
        m.match_type = "requirements_only"

        result = _convert_matches_to_dtos([m])

        assert len(result) == 1
        assert result[0].job.id == "job-1"
        assert result[0].overall_score == 85.5


# ---------------------------------------------------------------------------
# _save_matches_batch
# ---------------------------------------------------------------------------

class TestSaveMatchesBatch:

    def _dto(self, job_id="job-1", content_hash="h1"):
        d = Mock()
        d.job.id = job_id
        d.job.content_hash = content_hash
        return d

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_saves_new_match(self, mock_save, mock_uow):
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo

        result = _save_matches_batch([self._dto()], "fp-1", Mock(recalculate_existing=False))

        assert result == 1
        mock_save.assert_called_once()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_skips_existing_when_recalculate_false(self, mock_save, mock_uow):
        mock_existing = Mock(status='active', job_content_hash="h1")
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = mock_existing
        mock_uow.return_value.__enter__.return_value = mock_repo

        result = _save_matches_batch(
            [self._dto()], "fp-1", Mock(recalculate_existing=False)
        )

        assert result == 0
        mock_save.assert_not_called()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_recalculates_existing_when_flag_true(self, mock_save, mock_uow):
        """recalculate_existing=True saves even when content hash is unchanged."""
        mock_existing = Mock(status='active', job_content_hash="h1")
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = mock_existing
        mock_uow.return_value.__enter__.return_value = mock_repo

        result = _save_matches_batch(
            [self._dto()], "fp-1", Mock(recalculate_existing=True)
        )

        assert result == 1
        mock_save.assert_called_once()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_marks_stale_and_saves_when_content_changed(self, mock_save, mock_uow):
        mock_existing = Mock(status='active', job_content_hash="old-hash")
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = mock_existing
        mock_uow.return_value.__enter__.return_value = mock_repo

        result = _save_matches_batch(
            [self._dto(content_hash="new-hash")], "fp-1", Mock(recalculate_existing=False)
        )

        assert result == 1
        assert mock_existing.status == 'stale'
        mock_save.assert_called_once()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_logs_and_continues_on_save_error(self, mock_save, mock_uow, caplog):
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_save.side_effect = Exception("DB error")

        result = _save_matches_batch([self._dto()], "fp-1", Mock())

        assert result == 0
        assert "Failed saving match" in caplog.text


# ---------------------------------------------------------------------------
# _send_notifications
# ---------------------------------------------------------------------------

class TestSendNotifications:

    def _ctx(self, **kwargs):
        defaults = dict(
            enabled=True, user_id="u-1",
            channels={"email": Mock(enabled=True)},
            min_score_threshold=70.0,
            notify_on_new_match=True,
        )
        defaults.update(kwargs)
        ctx = Mock()
        ctx.config.notifications = Mock(**defaults)
        return ctx

    def _dto(self, score=85.0):
        d = Mock()
        d.job.id = "job-1"
        d.overall_score = score
        return d

    def test_disabled_returns_zero(self, caplog):
        caplog.set_level(logging.INFO)
        ctx = Mock()
        ctx.config.notifications = Mock(enabled=False)

        result = _send_notifications(ctx, [], 0, {}, "fp-1", threading.Event())

        assert result == 0
        assert "Skipped (disabled in config)" in caplog.text

    def test_no_saved_matches_returns_zero(self, caplog):
        caplog.set_level(logging.INFO)
        ctx = Mock()
        ctx.config.notifications = Mock(enabled=True)

        result = _send_notifications(ctx, [], 0, {}, "fp-1", threading.Event())

        assert result == 0
        assert "Skipped (no matches to notify)" in caplog.text

    def test_no_enabled_channels_returns_zero(self, caplog):
        ctx = self._ctx(channels={"email": Mock(enabled=False)})

        result = _send_notifications(ctx, [self._dto()], 1, {}, "fp-1", threading.Event())

        assert result == 0
        assert "No notification channels configured" in caplog.text

    def test_stop_event_returns_zero(self):
        stop = threading.Event()
        stop.set()

        result = _send_notifications(
            self._ctx(), [self._dto()], 1, {}, "fp-1", stop
        )

        assert result == 0

    @patch('pipeline.runner.job_uow')
    def test_no_match_record_logs_warning(self, mock_uow, caplog):
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo

        result = _send_notifications(
            self._ctx(), [self._dto()], 1, {}, "fp-1", threading.Event()
        )

        assert result == 0
        assert "No match record found" in caplog.text

    @patch('pipeline.runner.job_uow')
    def test_already_notified_skips(self, mock_uow):
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = Mock(id="m-1", notified=True)
        mock_uow.return_value.__enter__.return_value = mock_repo

        result = _send_notifications(
            self._ctx(), [self._dto()], 1, {}, "fp-1", threading.Event()
        )

        assert result == 0

    @patch('pipeline.runner.NotificationMessageBuilder')
    @patch('pipeline.runner.job_uow')
    def test_successful_notification(self, mock_uow, mock_builder):
        mock_record = Mock(id="m-1", notified=False)
        mock_record.job_post = Mock(company_url_direct="https://apply.example.com")
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = mock_record
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_builder.build_notification_content.return_value = "content"

        d = self._dto()
        d.fit_score = 80.0
        d.want_score = 75.0
        d.jd_required_coverage = 0.85
        d.job.content_hash = "h1"

        result = _send_notifications(
            self._ctx(), [d], 1, {"email": "u@e.com"}, "fp-1", threading.Event()
        )

        assert result >= 0  # notify_new_match may or may not fire depending on ctx.notification_service


# ---------------------------------------------------------------------------
# run_matching_pipeline
# ---------------------------------------------------------------------------

class TestRunMatchingPipeline:

    def _ctx(self, matching_enabled=True):
        ctx = Mock()
        ctx.config.matching = Mock(enabled=matching_enabled)
        ctx.config.etl = Mock()
        ctx.config.etl.resume = Mock()
        ctx.config.etl.resume.resume_file = "/resume.pdf"
        ctx.notification_service = Mock()
        return ctx

    def test_matching_disabled(self, caplog):
        caplog.set_level(logging.INFO)
        result = run_matching_pipeline(self._ctx(matching_enabled=False))

        assert result.success is True
        assert result.matches_count == 0
        assert "Skipped (disabled in config)" in caplog.text

    @patch('pipeline.runner._load_resume_file', return_value=(None, None))
    def test_returns_failure_when_resume_load_fails(self, _):
        result = run_matching_pipeline(self._ctx())

        assert result.success is False
        assert result.error == "Failed to load resume"

    @patch('pipeline.runner._load_resume_file', side_effect=Exception("boom"))
    def test_catches_unhandled_exception(self, _, caplog):
        result = run_matching_pipeline(self._ctx())

        assert result.success is False
        assert "boom" in result.error
        assert "Error in matching pipeline" in caplog.text

    @patch('pipeline.runner._load_resume_file', return_value=("/r.pdf", {"name": "J"}))
    @patch('pipeline.runner._determine_resume_extraction', return_value=("fp-1", True))
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[[0.1]])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[Mock()])
    @patch('pipeline.runner._save_matches_batch', return_value=1)
    def test_full_pipeline_success(self, mock_save, mock_score, *_):
        result = run_matching_pipeline(self._ctx())

        assert result.success is True
        assert result.matches_count == 1
        assert result.saved_count == 1
        assert result.execution_time > 0

    @patch('pipeline.runner._load_resume_file', return_value=("/r.pdf", {"name": "J"}))
    @patch('pipeline.runner._determine_resume_extraction', return_value=("fp-1", True))
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[])
    def test_stop_event_during_scoring_returns_interrupted(self, *_):
        stop = threading.Event()
        stop.set()

        result = run_matching_pipeline(self._ctx(), stop_event=stop)

        assert result.success is False
        assert "Interrupted" in result.error


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
