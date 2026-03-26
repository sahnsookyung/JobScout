#!/usr/bin/env python3
"""
Tests for Pipeline Runner.
Covers: pipeline/runner.py
"""

import pytest
import threading
import logging
from unittest.mock import Mock, patch, MagicMock, call

from etl.resume.loader import load_resume_with_parser as _load_resume_with_parser
from pipeline.runner import (
    MatchingPipelineResult,
    _load_resume_from_db,           # NEW: was missing from imports
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
    _run_matching_and_scoring,
    _should_terminate_early,
    _build_evidence_dto,
    _matched_req_to_dto,
    _missing_req_to_dto,
    _convert_matches_to_dtos,
    _save_matches_batch,
    _send_notifications,
)


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

    @patch('etl.resume.loader.ResumeParser')
    def test_returns_parsed_data(self, mock_parser_class):
        mock_parser_class.return_value.parse.return_value = Mock(
            data={"name": "John"}, text=None
        )
        assert _load_resume_with_parser("/path/resume.json") == {"name": "John"}

    @patch('etl.resume.loader.ResumeParser')
    def test_falls_back_to_raw_text_when_data_none(self, mock_parser_class):
        mock_parser_class.return_value.parse.return_value = Mock(data=None, text="Raw text")
        assert _load_resume_with_parser("/path/resume.txt") == {"raw_text": "Raw text"}

    @patch('etl.resume.loader.ResumeParser')
    def test_file_not_found(self, mock_parser_class, caplog):
        mock_parser_class.return_value.parse.side_effect = FileNotFoundError()
        assert _load_resume_with_parser("/missing.json") is None
        assert "Resume file not found" in caplog.text

    @patch('etl.resume.loader.ResumeParser')
    def test_value_error(self, mock_parser_class, caplog):
        mock_parser_class.return_value.parse.side_effect = ValueError("bad format")
        assert _load_resume_with_parser("/bad.pdf") is None
        assert "Failed to parse resume" in caplog.text

    @patch('etl.resume.loader.ResumeParser')
    def test_generic_exception(self, mock_parser_class, caplog):
        mock_parser_class.return_value.parse.side_effect = Exception("unexpected")
        assert _load_resume_with_parser("/resume.pdf") is None
        assert "Unexpected error loading resume" in caplog.text


# ---------------------------------------------------------------------------
# _load_resume_from_db  (NEW)
# ---------------------------------------------------------------------------

class TestLoadResumeFromDb:

    @patch('pipeline.runner.job_uow')
    def test_returns_extracted_data(self, mock_uow):
        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = Mock(
            extracted_data={"name": "Jane"}
        )
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        assert _load_resume_from_db("abc123fingerprint16") == {"name": "Jane"}

    @patch('pipeline.runner.job_uow')
    def test_returns_none_when_not_found(self, mock_uow, caplog):
        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        assert _load_resume_from_db("abc123fingerprint16") is None
        assert "No resume found in DB" in caplog.text

    @patch('pipeline.runner.job_uow')
    def test_returns_none_when_no_extracted_data(self, mock_uow, caplog):
        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = Mock(
            extracted_data=None
        )
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        assert _load_resume_from_db("abc123fingerprint16") is None
        assert "no extracted_data" in caplog.text

    @patch('pipeline.runner.job_uow')
    def test_handles_db_exception(self, mock_uow, caplog):
        mock_uow.return_value.__enter__.side_effect = Exception("connection refused")

        assert _load_resume_from_db("abc123fingerprint16") is None
        assert "Error loading resume from DB" in caplog.text


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
        assert load_user_wants_data("/nonexistent/wants.txt") == []
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

    @patch('pipeline.runner.load_resume_with_parser', return_value={"name": "John"})
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=True)
    def test_success_absolute_path(self, *_):
        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/absolute/resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath == "/absolute/resume.json"
        assert data == {"name": "John"}

    @patch('pipeline.runner.load_resume_with_parser', return_value={"name": "John"})
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=False)
    @patch('pipeline.runner.os.getcwd', return_value="/workspace")
    def test_relative_path_resolved(self, *_):
        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "resume.json"

        filepath, _ = _load_resume_file(mock_config)

        assert filepath == "/workspace/resume.json"

    @patch('pipeline.runner.load_resume_with_parser', return_value={"name": "John"})
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

        assert _load_resume_file(mock_config) == (None, None)

    def test_neither_attribute_exists(self):
        """Config object has neither resume nor resume_file — getattr guards handle it."""
        assert _load_resume_file(Mock(spec=[])) == (None, None)

    @patch('pipeline.runner.os.path.exists', return_value=False)
    def test_file_not_found(self, _, caplog):
        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/missing/resume.json"

        assert _load_resume_file(mock_config) == (None, None)
        assert "Resume file not found" in caplog.text

    @patch('pipeline.runner.load_resume_with_parser', return_value=None)
    @patch('pipeline.runner.os.path.exists', return_value=True)
    def test_parser_returns_none(self, mock_exists, mock_load_parser, caplog):
        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/path/resume.json"

        assert _load_resume_file(mock_config) == (None, None)
        assert "Failed to load resume data" in caplog.text


# ---------------------------------------------------------------------------
# _determine_resume_extraction
#
# FIX: patch pipeline.runner.job_uow (module-level name binding), NOT
#      database.uow.job_uow. The old patch never intercepted the call,
#      causing real DB connections to be attempted.
# generate_file_fingerprint is a local import inside the function, so
# patching database.models directly is still correct.
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
        mock_uow.return_value.__exit__.return_value = False

    def _setup_open(self, mock_open):
        fh = Mock()
        fh.read.return_value = b"bytes"
        mock_open.return_value.__enter__ = Mock(return_value=fh)
        mock_open.return_value.__exit__ = Mock(return_value=False)

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint', return_value="new-fp")
    @patch('pipeline.runner.job_uow')                   # FIX: was database.uow.job_uow
    def test_no_stored_fingerprint_triggers_extraction(self, mock_uow, _, mock_open):
        self._setup_open(mock_open)
        self._setup_uow(mock_uow, stored_fp=None)

        fp, should = _determine_resume_extraction("/resume.pdf", self._make_config())

        assert fp == "new-fp"
        assert should is True

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint', return_value="new-fp")
    @patch('pipeline.runner.job_uow')                   # FIX
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
    @patch('pipeline.runner.job_uow')                   # FIX
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
    @patch('pipeline.runner.job_uow')                   # FIX
    def test_force_flag_overrides_matching_fingerprint(self, mock_uow, _, mock_open, caplog):
        caplog.set_level(logging.INFO)
        self._setup_open(mock_open)
        self._setup_uow(mock_uow, stored_fp="current-fp")

        fp, should = _determine_resume_extraction("/resume.pdf", self._make_config(force=True))

        assert fp == "current-fp"
        assert should is True
        assert "Force re-extraction enabled" in caplog.text

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint', return_value="current-fp")
    @patch('pipeline.runner.job_uow')                   # FIX
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
        assert _load_user_wants_embeddings(Mock(user_wants_file=None), Mock()) == []

    @patch('pipeline.runner.os.path.exists', return_value=False)
    def test_file_not_found(self, _):
        assert _load_user_wants_embeddings(
            Mock(user_wants_file="/missing/wants.txt"), Mock()
        ) == []

    @patch('pipeline.runner.load_user_wants_data', return_value=[])
    @patch('pipeline.runner.os.path.exists', return_value=True)
    def test_empty_file_returns_empty(self, *_):
        assert _load_user_wants_embeddings(Mock(user_wants_file="/wants.txt"), Mock()) == []

    @patch('pipeline.runner.load_user_wants_data', return_value=["a", "b", "c"])
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=True)
    def test_uses_batch_embedding_when_available(self, *_):
        """generate_embeddings is called once with the full list when it exists."""
        mock_ai = Mock()
        mock_ai.generate_embeddings.return_value = [[0.1], [0.2], [0.3]]

        result = _load_user_wants_embeddings(Mock(user_wants_file="/wants.txt"), mock_ai)

        assert result == [[0.1], [0.2], [0.3]]
        mock_ai.generate_embeddings.assert_called_once_with(["a", "b", "c"])
        mock_ai.generate_embedding.assert_not_called()

    @patch('pipeline.runner.load_user_wants_data', return_value=["a", "b", "c"])
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=True)
    def test_falls_back_to_per_item_when_batch_raises(self, *_):
        """Batch failure silently falls back to per-item calls."""
        mock_ai = Mock()
        mock_ai.generate_embeddings.side_effect = Exception("batch unavailable")
        mock_ai.generate_embedding.side_effect = [[0.1], [0.2], [0.3]]

        result = _load_user_wants_embeddings(Mock(user_wants_file="/wants.txt"), mock_ai)

        assert len(result) == 3
        assert mock_ai.generate_embedding.call_count == 3

    @patch('pipeline.runner.load_user_wants_data', return_value=["a", "b", "c"])
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=True)
    def test_generates_one_embedding_per_want(self, *_):
        # FIX: use spec to prevent Mock auto-creating generate_embeddings,
        # which previously caused the batch path to return a Mock and len() to fail.
        mock_ai = Mock(spec=['generate_embedding'])
        mock_ai.generate_embedding.side_effect = [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]

        result = _load_user_wants_embeddings(Mock(user_wants_file="/wants.txt"), mock_ai)

        assert len(result) == 3
        assert mock_ai.generate_embedding.call_count == 3

    @patch('pipeline.runner.load_user_wants_data', return_value=["a", "b", "c"])
    @patch('pipeline.runner.os.path.exists', return_value=True)
    @patch('pipeline.runner.os.path.isabs', return_value=True)
    def test_per_item_failure_is_isolated(self, mock_isabs, mock_exists, mock_load_data, caplog):
        """A single bad embedding is skipped; the rest succeed."""
        mock_ai = Mock(spec=['generate_embedding'])
        mock_ai.generate_embedding.side_effect = [
            [0.1, 0.2],
            Exception("model error"),
            [0.5, 0.6],
        ]

        result = _load_user_wants_embeddings(Mock(user_wants_file="/wants.txt"), mock_ai)

        assert len(result) == 2
        assert "Failed to embed want" in caplog.text


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
        assert _get_pre_extracted_resume(
            Mock(extracted_data=None), should_re_extract=False
        ) is None

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
        mock_s.resume_fingerprint = "fp-abc"

        assert _get_pre_extracted_resume(mock_s, should_re_extract=False) is not None

    def test_logs_warning_on_parse_failure(self, caplog):
        with pytest.raises(ValueError, match="Failed to parse stored ready resume"):
            _get_pre_extracted_resume(
                Mock(extracted_data={"invalid": "schema"}), should_re_extract=False
            )


# ---------------------------------------------------------------------------
# _run_vector_matching
# ---------------------------------------------------------------------------

class TestRunVectorMatching:

    def test_returns_preliminary_matches(self):
        mock_matcher = Mock()
        mock_matcher.match_resume_two_stage.return_value = [Mock(), Mock()]

        result = _run_vector_matching(
            mock_matcher, Mock(), {"name": "John"}, threading.Event(), Mock(), "fp-1"
        )

        assert len(result) == 2
        mock_matcher.match_resume_two_stage.assert_called_once()

    def test_passes_correct_kwargs_to_matcher(self):
        mock_matcher = Mock()
        mock_matcher.match_resume_two_stage.return_value = []
        stop = threading.Event()
        pre_extracted = Mock()

        _run_vector_matching(mock_matcher, Mock(), {}, stop, pre_extracted, "fp-abc")

        _, kwargs = mock_matcher.match_resume_two_stage.call_args
        assert kwargs['resume_fingerprint'] == "fp-abc"
        assert kwargs['stop_event'] is stop
        assert kwargs['pre_extracted_resume'] is pre_extracted


# ---------------------------------------------------------------------------
# _run_scorer_service
# ---------------------------------------------------------------------------

class TestRunScorerService:

    def test_uses_fit_want_scoring_when_embeddings_provided(self):
        mock_scorer = Mock()
        mock_scorer.score_matches.return_value = [Mock()]

        result = _run_scorer_service(
            mock_scorer, [Mock()], Mock(),
            [[0.1, 0.2]], {"job-1": [0.3]}, threading.Event()
        )

        assert len(result) == 1
        assert "user_want_embeddings" in mock_scorer.score_matches.call_args[1]

    def test_uses_fit_only_scoring_when_no_embeddings(self):
        mock_scorer = Mock()
        mock_scorer.score_matches.return_value = []

        _run_scorer_service(mock_scorer, [Mock()], Mock(), [], {}, threading.Event())

        assert "user_want_embeddings" not in mock_scorer.score_matches.call_args[1]

    def test_passes_stop_event(self):
        mock_scorer = Mock()
        mock_scorer.score_matches.return_value = []
        stop = threading.Event()

        _run_scorer_service(mock_scorer, [], Mock(), [], {}, stop)

        assert mock_scorer.score_matches.call_args[1]['stop_event'] is stop


# ---------------------------------------------------------------------------
# _build_evidence_dto / _matched_req_to_dto / _missing_req_to_dto
# ---------------------------------------------------------------------------

class TestBuildEvidenceDto:

    def test_returns_dto_from_evidence(self):
        ev = Mock(text="5y Python", source_section="experience", tags={"k": "v"})
        result = _build_evidence_dto(ev)
        assert result.text == "5y Python"
        assert result.source_section == "experience"
        assert result.tags == {"k": "v"}

    def test_returns_none_when_no_evidence(self):
        assert _build_evidence_dto(None) is None


class TestMatchedReqToDto:

    def test_preserves_all_fields(self):
        req = Mock()
        req.requirement = Mock(id="r-1", req_type="required")
        req.evidence = Mock(text="text", source_section="skills", tags={})
        req.similarity = 0.85
        req.is_covered = True

        result = _matched_req_to_dto(req)

        assert result.requirement.id == "r-1"
        assert result.requirement.req_type == "required"
        assert result.similarity == 0.85
        assert result.is_covered is True

    def test_none_evidence_is_handled(self):
        req = Mock()
        req.requirement = Mock(id="r-3", req_type="required")
        req.evidence = None
        req.similarity = 0.5
        req.is_covered = False

        assert _matched_req_to_dto(req).evidence is None


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

    def _make_orm_match(self, job_id="job-1", overall=85.5):
        m = Mock()
        m.job = Mock(
            id=job_id, title="Engineer", company="TechCorp",
            location_text="Remote", is_remote=True, content_hash="h1",
        )
        m.overall_score = overall
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
        return m

    def test_empty_list(self):
        assert _convert_matches_to_dtos([]) == []

    def test_converts_single_match(self):
        result = _convert_matches_to_dtos([self._make_orm_match()])
        assert len(result) == 1
        assert result[0].job.id == "job-1"
        assert result[0].overall_score == 85.5

    def test_converts_multiple_matches(self):
        result = _convert_matches_to_dtos([
            self._make_orm_match("job-1"),
            self._make_orm_match("job-2"),
        ])
        assert {dto.job.id for dto in result} == {"job-1", "job-2"}

    def test_null_scores_default_to_zero(self):
        m = self._make_orm_match()
        m.overall_score = None
        m.fit_score = None
        m.want_score = None
        m.job_similarity = None

        result = _convert_matches_to_dtos([m])

        assert result[0].overall_score == 0.0
        assert result[0].fit_score == 0.0
        assert result[0].want_score == 0.0

    def test_direct_attribute_access_raises_on_schema_drift(self):
        """ORM schema drift (missing field) should raise AttributeError immediately."""
        with pytest.raises(AttributeError):
            _convert_matches_to_dtos([Mock(spec=[])])


# ---------------------------------------------------------------------------
# _save_matches_batch
# ---------------------------------------------------------------------------

class TestSaveMatchesBatch:

    def _dto(self, job_id="job-1", content_hash="h1"):
        d = Mock()
        d.job.id = job_id
        d.job.content_hash = content_hash
        return d

    def _setup_uow(self, mock_uow, existing=None):
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = existing
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False
        return mock_repo

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_saves_new_match(self, mock_save, mock_uow):
        self._setup_uow(mock_uow, existing=None)
        dto = self._dto()

        result = _save_matches_batch([dto], "fp-1", Mock(recalculate_existing=False))

        assert result == 1
        _, kwargs = mock_save.call_args
        assert kwargs['scored_match'] is dto
        assert kwargs['is_stale_replacement'] is False

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_skips_existing_when_recalculate_false(self, mock_save, mock_uow):
        self._setup_uow(mock_uow, existing=Mock(status='active', job_content_hash="h1"))

        result = _save_matches_batch(
            [self._dto()], "fp-1", Mock(recalculate_existing=False)
        )

        assert result == 0
        mock_save.assert_not_called()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_recalculates_existing_when_flag_true(self, mock_save, mock_uow):
        self._setup_uow(mock_uow, existing=Mock(status='active', job_content_hash="h1"))

        result = _save_matches_batch(
            [self._dto()], "fp-1", Mock(recalculate_existing=True)
        )

        assert result == 1
        mock_save.assert_called_once()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_marks_stale_and_inserts_replacement_on_content_change(self, mock_save, mock_uow):
        mock_existing = Mock(status='active', job_content_hash="old-hash")
        self._setup_uow(mock_uow, existing=mock_existing)

        result = _save_matches_batch(
            [self._dto(content_hash="new-hash")], "fp-1", Mock(recalculate_existing=False)
        )

        assert result == 1
        assert mock_existing.status == 'stale'
        assert mock_existing.invalidated_reason == "Job content updated"
        # FIX: is_stale_replacement must be True to signal a new insert, not an update
        _, kwargs = mock_save.call_args
        assert kwargs['is_stale_replacement'] is True

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_logs_and_continues_on_save_error(self, mock_save, mock_uow, caplog):
        self._setup_uow(mock_uow, existing=None)
        mock_save.side_effect = Exception("DB error")

        result = _save_matches_batch([self._dto()], "fp-1", Mock())

        assert result == 0
        assert "Failed saving match" in caplog.text

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_partial_failure_does_not_stop_batch(self, mock_save, mock_uow, caplog):
        """One failing match must not prevent the remaining matches from saving."""
        self._setup_uow(mock_uow, existing=None)
        mock_save.side_effect = [None, Exception("fail mid-batch"), None]

        dtos = [self._dto("j1"), self._dto("j2"), self._dto("j3")]
        result = _save_matches_batch(dtos, "fp-1", Mock())

        assert result == 2

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_saves_multiple_matches(self, mock_save, mock_uow):
        self._setup_uow(mock_uow, existing=None)

        result = _save_matches_batch(
            [self._dto("j1"), self._dto("j2"), self._dto("j3")],
            "fp-1", Mock(recalculate_existing=False),
        )

        assert result == 3
        assert mock_save.call_count == 3


# ---------------------------------------------------------------------------
# _send_notifications
# ---------------------------------------------------------------------------

class TestSendNotifications:

    def _ctx(self, **overrides):
        defaults = dict(
            enabled=True, user_id="u-1",
            channels={"email": Mock(enabled=True)},
            min_score_threshold=70.0,
            notify_on_new_match=True,
            notify_on_batch_complete=False,
        )
        defaults.update(overrides)
        ctx = Mock()
        ctx.config.notifications = Mock(**defaults)
        return ctx

    def _dto(self, score=85.0):
        d = Mock()
        d.job.id = "job-1"
        d.overall_score = score
        d.fit_score = 80.0
        d.want_score = 75.0
        d.jd_required_coverage = 0.85
        return d

    def _setup_uow(self, mock_uow, record):
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = record
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False
        return mock_repo

    def test_disabled_returns_zero(self, caplog):
        caplog.set_level(logging.INFO)
        ctx = Mock()
        ctx.config.notifications = Mock(enabled=False)

        assert _send_notifications(ctx, [], 0, {}, "fp-1", threading.Event()) == 0
        assert "Skipped (disabled in config)" in caplog.text

    def test_no_saved_matches_returns_zero(self, caplog):
        caplog.set_level(logging.INFO)
        ctx = Mock()
        ctx.config.notifications = Mock(enabled=True)

        assert _send_notifications(ctx, [], 0, {}, "fp-1", threading.Event()) == 0
        assert "Skipped (no matches to notify)" in caplog.text

    def test_no_enabled_channels_returns_zero(self, caplog):
        ctx = self._ctx(channels={"email": Mock(enabled=False)})

        assert _send_notifications(ctx, [self._dto()], 1, {}, "fp-1", threading.Event()) == 0
        assert "No notification channels configured" in caplog.text

    def test_stop_event_breaks_loop(self):
        stop = threading.Event()
        stop.set()

        assert _send_notifications(self._ctx(), [self._dto()], 1, {}, "fp-1", stop) == 0

    def test_below_threshold_match_is_skipped(self):
        ctx = self._ctx(min_score_threshold=90.0)

        assert _send_notifications(ctx, [self._dto(score=60.0)], 1, {}, "fp-1", threading.Event()) == 0

    @patch('pipeline.runner.job_uow')
    def test_no_match_record_logs_warning(self, mock_uow, caplog):
        self._setup_uow(mock_uow, record=None)

        result = _send_notifications(self._ctx(), [self._dto()], 1, {}, "fp-1", threading.Event())

        assert result == 0
        assert "No match record found" in caplog.text

    @patch('pipeline.runner.job_uow')
    def test_already_notified_skips(self, mock_uow):
        self._setup_uow(mock_uow, record=Mock(id="m-1", notified=True))

        assert _send_notifications(self._ctx(), [self._dto()], 1, {}, "fp-1", threading.Event()) == 0

    @patch('pipeline.runner.NotificationMessageBuilder')
    @patch('pipeline.runner.job_uow')
    def test_successful_notification_increments_count(self, mock_uow, mock_builder):
        mock_record = Mock(id="m-1", notified=False)
        mock_record.job_post = Mock(company_url_direct="https://apply.example.com")
        self._setup_uow(mock_uow, record=mock_record)
        mock_builder.build_notification_content.return_value = "content"

        ctx = self._ctx()
        dto = self._dto()

        result = _send_notifications(ctx, [dto], 1, {}, "fp-1", threading.Event())

        assert result == 1
        ctx.notification_service.notify_new_match.assert_called_once_with(
            user_id="u-1",
            match_id="m-1",
            content="content",
            channels=["email"],
        )

    @patch('pipeline.runner.NotificationMessageBuilder')
    @patch('pipeline.runner.job_uow')
    def test_notified_flag_persisted_in_same_session(self, mock_uow, mock_builder):
        """notified=True must be set on the record inside the existing session,
        not via a separate UOW open/close."""
        mock_record = Mock(id="m-1", notified=False)
        mock_record.job_post = Mock(company_url_direct="https://apply.example.com")
        self._setup_uow(mock_uow, record=mock_record)
        mock_builder.build_notification_content.return_value = "content"

        _send_notifications(self._ctx(), [self._dto()], 1, {}, "fp-1", threading.Event())

        # One UOW per match (not two — the old bug opened a second one)
        assert mock_uow.call_count == 1
        assert mock_record.notified is True

    @patch('pipeline.runner.NotificationMessageBuilder')
    @patch('pipeline.runner.job_uow')
    def test_notification_send_failure_continues_loop(self, mock_uow, mock_builder, caplog):
        """A failed notify_new_match call must not abort the remaining DTOs."""
        mock_record = Mock(id="m-1", notified=False)
        mock_record.job_post = Mock(company_url_direct="https://apply.example.com")
        self._setup_uow(mock_uow, record=mock_record)
        mock_builder.build_notification_content.return_value = "content"

        ctx = self._ctx()
        ctx.notification_service.notify_new_match.side_effect = Exception("send failed")

        dto1 = self._dto()
        dto1.job.id = "job-1"
        dto2 = self._dto()
        dto2.job.id = "job-2"

        # Both DTOs should be attempted; failures are caught per-iteration
        result = _send_notifications(ctx, [dto1, dto2], 2, {}, "fp-1", threading.Event())

        assert result == 0  # none succeeded

    @patch('pipeline.runner.NotificationMessageBuilder')
    @patch('pipeline.runner.job_uow')
    def test_no_job_post_skips_content_build(self, mock_uow, mock_builder):
        mock_record = Mock(id="m-1", notified=False, job_post=None)
        self._setup_uow(mock_uow, record=mock_record)

        result = _send_notifications(self._ctx(), [self._dto()], 1, {}, "fp-1", threading.Event())

        assert result == 0
        mock_builder.build_notification_content.assert_not_called()

    @patch('pipeline.runner.NotificationMessageBuilder')
    @patch('pipeline.runner.job_uow')
    def test_batch_complete_notification_sent(self, mock_uow, mock_builder):
        mock_record = Mock(id="m-1", notified=False)
        mock_record.job_post = Mock(company_url_direct="https://apply.example.com")
        self._setup_uow(mock_uow, record=mock_record)
        mock_builder.build_notification_content.return_value = "content"

        ctx = self._ctx(notify_on_batch_complete=True)

        _send_notifications(ctx, [self._dto()], 1, {}, "fp-1", threading.Event())

        ctx.notification_service.notify_batch_complete.assert_called_once_with(
            user_id="u-1",
            total_matches=1,
            high_score_matches=1,
            channels=["email"],
        )

    @patch('pipeline.runner.NotificationMessageBuilder')
    @patch('pipeline.runner.job_uow')
    def test_batch_complete_failure_does_not_raise(self, mock_uow, mock_builder, caplog):
        mock_record = Mock(id="m-1", notified=False)
        mock_record.job_post = Mock(company_url_direct="https://apply.example.com")
        self._setup_uow(mock_uow, record=mock_record)
        mock_builder.build_notification_content.return_value = "content"

        ctx = self._ctx(notify_on_batch_complete=True)
        ctx.notification_service.notify_batch_complete.side_effect = Exception("batch failed")

        result = _send_notifications(ctx, [self._dto()], 1, {}, "fp-1", threading.Event())

        assert result == 1  # individual notification still succeeded
        assert "Failed to send batch summary" in caplog.text

    @patch('pipeline.runner.job_uow')
    def test_uow_exception_logs_and_continues(self, mock_uow, caplog):
        """An exception inside the UOW context is caught per-DTO."""
        mock_uow.return_value.__enter__.side_effect = Exception("DB down")

        result = _send_notifications(
            self._ctx(), [self._dto()], 1, {}, "fp-1", threading.Event()
        )

        assert result == 0
        # The per-iteration except block swallows it — pipeline does not crash
        assert "Failed to process notification" in caplog.text

    def test_user_id_falls_back_to_resume_email(self):
        ctx = self._ctx(user_id=None)
        resume_data = {"email": "user@example.com"}

        # No channel enabled — just verify it reaches the channel check without error
        ctx.config.notifications.channels = {"email": Mock(enabled=False)}

        result = _send_notifications(
            ctx, [self._dto()], 1, resume_data, "fp-1", threading.Event()
        )

        assert result == 0  # no channels, but no crash either

    def test_user_id_falls_back_to_default_user(self):
        ctx = self._ctx(user_id=None)
        ctx.config.notifications.channels = {"email": Mock(enabled=False)}

        result = _send_notifications(
            ctx, [self._dto()], 1, {}, "fp-1", threading.Event()
        )

        assert result == 0

    def test_notification_config_none_returns_zero(self, caplog):
        """When notification_config is None, returns 0."""
        caplog.set_level(logging.INFO)
        ctx = Mock()
        ctx.config.notifications = None

        result = _send_notifications(ctx, [], 1, {}, "fp-1", threading.Event())

        assert result == 0
        assert "Skipped (disabled in config)" in caplog.text

    @patch('pipeline.runner.job_uow')
    def test_notify_on_new_match_false_skips_notification(self, mock_uow):
        """When notify_on_new_match=False, no notification is sent even for high-score matches."""
        ctx = self._ctx(notify_on_new_match=False)
        mock_record = Mock(id="m-1", notified=False)
        mock_record.job_post = Mock(company_url_direct="https://apply.example.com")
        self._setup_uow(mock_uow, record=mock_record)

        result = _send_notifications(ctx, [self._dto()], 1, {}, "fp-1", threading.Event())

        assert result == 0
        ctx.notification_service.notify_new_match.assert_not_called()


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

    def test_matching_disabled_returns_success(self, caplog):
        caplog.set_level(logging.INFO)
        result = run_matching_pipeline(self._ctx(matching_enabled=False))

        assert result.success is True
        assert result.matches_count == 0
        assert "Skipped (disabled in config)" in caplog.text

    def test_matching_config_none_returns_success(self):
        ctx = self._ctx()
        ctx.config.matching = None

        result = run_matching_pipeline(ctx)

        assert result.success is True

    @patch('pipeline.runner._load_configured_resume_fallback', return_value=(None, None, "Failed to load resume"))
    @patch('pipeline.runner.job_uow')
    def test_returns_failure_when_resume_load_fails(self, mock_uow, _):
        mock_repo = Mock()
        mock_repo.get_latest_ready_resume_fingerprint.return_value = None
        mock_repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        result = run_matching_pipeline(self._ctx())

        assert result.success is False
        assert result.error == "Failed to load resume"

    @patch('pipeline.runner._load_resume_from_db', return_value=None)
    def test_returns_failure_when_db_resume_not_found(self, _):
        result = run_matching_pipeline(self._ctx(), resume_fingerprint="fp" * 8)

        assert result.success is False
        assert "Resume not found in DB" in result.error

    @patch('pipeline.runner._load_resume_from_db', return_value={"name": "Jane"})
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[Mock()])
    @patch('pipeline.runner._save_matches_batch', return_value=1)
    def test_fingerprint_path_skips_file_loading(self, *_):
        """When resume_fingerprint is provided, _load_resume_file must not be called."""
        with patch('pipeline.runner._load_resume_file') as mock_load_file:
            run_matching_pipeline(self._ctx(), resume_fingerprint="fp" * 8)
            mock_load_file.assert_not_called()

    @patch('pipeline.runner._load_configured_resume_fallback', side_effect=Exception("boom"))
    @patch('pipeline.runner.job_uow')
    def test_catches_unhandled_exception(self, mock_uow, _, caplog):
        mock_repo = Mock()
        mock_repo.get_latest_ready_resume_fingerprint.return_value = None
        mock_repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        result = run_matching_pipeline(self._ctx())

        assert result.success is False
        assert "boom" in result.error
        assert "Error in matching pipeline" in caplog.text

    @patch('pipeline.runner._load_configured_resume_fallback', return_value=("fp-1", {"name": "J"}, None))
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[[0.1]])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[Mock()])
    @patch('pipeline.runner._save_matches_batch', return_value=1)
    @patch('pipeline.runner.job_uow')
    def test_full_pipeline_success(self, mock_uow, *_):
        mock_repo = Mock()
        mock_repo.get_latest_ready_resume_fingerprint.return_value = None
        mock_repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        result = run_matching_pipeline(self._ctx())

        assert result.success is True
        assert result.matches_count == 1
        assert result.saved_count == 1
        assert result.execution_time > 0

    @patch('pipeline.runner._load_configured_resume_fallback', return_value=("fp-1", {"name": "J"}, None))
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[])
    @patch('pipeline.runner.job_uow')
    def test_stop_event_during_scoring_returns_interrupted(self, mock_uow, *_):
        mock_repo = Mock()
        mock_repo.get_latest_ready_resume_fingerprint.return_value = None
        mock_repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        stop = threading.Event()
        stop.set()

        result = run_matching_pipeline(self._ctx(), stop_event=stop)

        assert result.success is False
        assert result.cancelled is True
        assert "Cancelled" in result.error

    @patch('pipeline.runner._load_configured_resume_fallback', return_value=("fp-1", {"name": "J"}, None))
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[])
    @patch('pipeline.runner._save_matches_batch', return_value=0)
    @patch('pipeline.runner.job_uow')
    def test_notification_skipped_when_no_matches_saved(self, mock_uow, *_):
        mock_repo = Mock()
        mock_repo.get_latest_ready_resume_fingerprint.return_value = None
        mock_repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        ctx = self._ctx()

        run_matching_pipeline(ctx)

        ctx.notification_service.notify_new_match.assert_not_called()

    @patch('pipeline.runner._load_configured_resume_fallback', return_value=("fp-1", {"name": "J"}, None))
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[Mock()])
    @patch('pipeline.runner._save_matches_batch', return_value=1)
    @patch('pipeline.runner.job_uow')
    def test_notification_skipped_when_no_notification_service(self, mock_uow, *_):
        mock_repo = Mock()
        mock_repo.get_latest_ready_resume_fingerprint.return_value = None
        mock_repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        ctx = self._ctx()
        ctx.notification_service = None

        result = run_matching_pipeline(ctx)

        assert result.success is True
        assert result.notified_count == 0

    @patch('pipeline.runner._load_configured_resume_fallback', return_value=("fp-1", {"name": "J"}, None))
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[Mock()])
    @patch('pipeline.runner._save_matches_batch', return_value=1)
    @patch('pipeline.runner.job_uow')
    def test_execution_time_is_recorded(self, mock_uow, *_):
        mock_repo = Mock()
        mock_repo.get_latest_ready_resume_fingerprint.return_value = None
        mock_repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False

        result = run_matching_pipeline(self._ctx())

        assert result.execution_time >= 0.0

    @patch('pipeline.runner._load_configured_resume_fallback', return_value=("fp-1", {"name": "J"}, None))
    @patch('pipeline.runner._load_user_wants_embeddings', return_value=[])
    @patch('pipeline.runner._run_matching_and_scoring', return_value=[Mock()])
    @patch('pipeline.runner._save_matches_batch', return_value=1)
    @patch('pipeline.runner.job_uow')
    def test_status_callback_is_invoked(self, mock_uow, *_):
        """Status callback is invoked for the major pipeline phases."""
        mock_repo = Mock()
        mock_repo.get_latest_ready_resume_fingerprint.return_value = None
        mock_repo.get_latest_resume_processing_state.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False
        callback = Mock()

        run_matching_pipeline(self._ctx(), status_callback=callback)

        assert callback.call_args_list == [
            call("saving_results"),
            call("notifying"),
        ]

    def test_default_stop_event_created_when_none_provided(self):
        """Passing stop_event=None must not raise."""
        ctx = self._ctx(matching_enabled=False)

        result = run_matching_pipeline(ctx, stop_event=None)

        assert result.success is True


# ---------------------------------------------------------------------------
# _should_terminate_early
# ---------------------------------------------------------------------------

class TestShouldTerminateEarly:

    def test_returns_true_when_stop_event_set(self):
        stop = threading.Event()
        stop.set()
        assert _should_terminate_early(stop, None) is True

    def test_returns_false_when_stop_not_set_no_callback(self):
        assert _should_terminate_early(threading.Event(), None) is False

    def test_returns_false_when_stop_not_set_with_callback(self):
        assert _should_terminate_early(threading.Event(), lambda s: None) is False


# ---------------------------------------------------------------------------
# _run_matching_and_scoring
# ---------------------------------------------------------------------------

class TestRunMatchingAndScoring:

    def _setup_uow(self, mock_uow, structured_resume=None):
        mock_repo = Mock()
        mock_repo.get_job_facet_embeddings.return_value = []
        mock_uow.return_value.__enter__.return_value = mock_repo
        mock_uow.return_value.__exit__.return_value = False
        return mock_repo

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner._load_structured_resume', return_value=None)
    @patch('pipeline.runner._prepare_matcher_service')
    def test_returns_empty_when_no_resume_and_not_re_extracting(
        self, mock_prep, mock_load, mock_uow, caplog
    ):
        """No structured resume + should_re_extract=False raises a clear error."""
        self._setup_uow(mock_uow)

        with pytest.raises(ValueError, match="Resume not found in database"):
            _run_matching_and_scoring(
                Mock(), {"name": "J"}, "fp-1", should_re_extract=False,
                matching_config=Mock(), user_want_embeddings=[],
                stop_event=threading.Event(), status_callback=None,
            )

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner._load_structured_resume')
    @patch('pipeline.runner._prepare_matcher_service')
    @patch('pipeline.runner._should_terminate_early', return_value=True)
    def test_returns_empty_when_terminate_early_first_check(
        self, mock_terminate, mock_prep, mock_load, mock_uow
    ):
        """_should_terminate_early True on first check → returns []."""
        mock_structured = Mock(total_experience_years=3)
        mock_load.return_value = mock_structured
        self._setup_uow(mock_uow, structured_resume=mock_structured)

        result = _run_matching_and_scoring(
            Mock(), {"name": "J"}, "fp-1", should_re_extract=False,
            matching_config=Mock(), user_want_embeddings=[],
            stop_event=threading.Event(), status_callback=None,
        )

        assert result == []

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner._load_structured_resume', return_value=None)
    @patch('pipeline.runner._prepare_matcher_service')
    @patch('pipeline.runner._should_terminate_early', side_effect=[False, True])
    @patch('pipeline.runner._run_vector_matching', return_value=[])
    def test_returns_empty_when_terminate_early_after_vector_match(
        self, mock_vmatch, mock_terminate, mock_prep, mock_load, mock_uow
    ):
        """_should_terminate_early True on second check → returns []."""
        self._setup_uow(mock_uow)

        result = _run_matching_and_scoring(
            Mock(), {"name": "J"}, "fp-1", should_re_extract=True,
            matching_config=Mock(), user_want_embeddings=[],
            stop_event=threading.Event(), status_callback=None,
        )

        assert result == []

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner._load_structured_resume')
    @patch('pipeline.runner._prepare_matcher_service')
    @patch('pipeline.runner._should_terminate_early', return_value=False)
    @patch('pipeline.runner._get_pre_extracted_resume', return_value=Mock())
    @patch('pipeline.runner._run_vector_matching')
    @patch('pipeline.runner.ScoringService')
    @patch('pipeline.runner._run_scorer_service', return_value=[Mock()])
    @patch('pipeline.runner._convert_matches_to_dtos')
    def test_success_with_top5_log(
        self, mock_convert, mock_scorer_svc, mock_score_run, mock_vmatch,
        mock_pre, mock_terminate, mock_prep, mock_load, mock_uow, caplog
    ):
        """Successful run with matches logs 'Top 5 Matches'."""
        caplog.set_level(logging.INFO)
        mock_structured = Mock(total_experience_years=5, resume_fingerprint="fp-1")
        mock_load.return_value = mock_structured
        self._setup_uow(mock_uow, structured_resume=mock_structured)

        dto1 = Mock()
        dto1.job.title = "Engineer"
        dto1.job.company = "TechCo"
        dto1.overall_score = 85.0
        dto1.fit_score = 80.0
        dto1.want_score = 75.0
        mock_convert.return_value = [dto1]

        result = _run_matching_and_scoring(
            Mock(), {"name": "J"}, "fp-1", should_re_extract=False,
            matching_config=Mock(), user_want_embeddings=[],
            stop_event=threading.Event(), status_callback=None,
        )

        assert len(result) == 1
        assert "Top 5 Matches" in caplog.text

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner._load_structured_resume', return_value=None)
    @patch('pipeline.runner._prepare_matcher_service')
    @patch('pipeline.runner._should_terminate_early', return_value=False)
    @patch('pipeline.runner._run_vector_matching', return_value=[])
    @patch('pipeline.runner.ScoringService')
    @patch('pipeline.runner._run_scorer_service', return_value=[])
    @patch('pipeline.runner._convert_matches_to_dtos', return_value=[])
    def test_status_callback_invoked_for_matching_steps(
        self, mock_convert, mock_scorer_svc, mock_score_run, mock_vmatch,
        mock_terminate, mock_prep, mock_load, mock_uow
    ):
        """Status callback emits each major matching phase in order."""
        self._setup_uow(mock_uow)
        callback = Mock()

        _run_matching_and_scoring(
            Mock(), {"name": "J"}, "fp-1", should_re_extract=True,
            matching_config=Mock(), user_want_embeddings=[],
            stop_event=threading.Event(), status_callback=callback,
        )

        assert callback.call_args_list == [
            call("loading_resume"),
            call("vector_matching"),
            call("scoring"),
        ]

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner._load_structured_resume', return_value=None)
    @patch('pipeline.runner._prepare_matcher_service')
    @patch('pipeline.runner._should_terminate_early', return_value=False)
    @patch('pipeline.runner._run_vector_matching')
    @patch('pipeline.runner.ScoringService')
    @patch('pipeline.runner._run_scorer_service', return_value=[])
    @patch('pipeline.runner._convert_matches_to_dtos', return_value=[])
    def test_facet_embeddings_populated_per_job(
        self, mock_convert, mock_scorer_svc, mock_score_run, mock_vmatch,
        mock_terminate, mock_prep, mock_load, mock_uow
    ):
        """Job facet embeddings map is populated for each unique job in preliminary_matches."""
        mock_repo = self._setup_uow(mock_uow)
        job1 = Mock()
        job1.job.id = "j-1"
        job2 = Mock()
        job2.job.id = "j-1"  # same id — should only query once
        mock_vmatch.return_value = [job1, job2]

        _run_matching_and_scoring(
            Mock(), {"name": "J"}, "fp-1", should_re_extract=True,
            matching_config=Mock(), user_want_embeddings=[],
            stop_event=threading.Event(), status_callback=None,
        )

        # Same job_id → get_job_facet_embeddings called exactly once
        assert mock_repo.get_job_facet_embeddings.call_count == 1

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner._load_structured_resume')
    @patch('pipeline.runner._prepare_matcher_service')
    @patch('pipeline.runner._should_terminate_early', return_value=False)
    @patch('pipeline.runner._run_vector_matching', return_value=[])
    @patch('pipeline.runner.ScoringService')
    @patch('pipeline.runner._run_scorer_service', return_value=[])
    @patch('pipeline.runner._convert_matches_to_dtos', return_value=[])
    def test_re_extract_true_logs_will_re_extract(
        self, mock_convert, mock_scorer_svc, mock_score_run, mock_vmatch,
        mock_terminate, mock_prep, mock_load, mock_uow, caplog
    ):
        """When should_re_extract=True and no structured resume, logs 'Will re-extract'."""
        caplog.set_level(logging.INFO)
        mock_load.return_value = None
        self._setup_uow(mock_uow)

        _run_matching_and_scoring(
            Mock(), {"name": "J"}, "fp-abc", should_re_extract=True,
            matching_config=Mock(), user_want_embeddings=[],
            stop_event=threading.Event(), status_callback=None,
        )

        assert "Will re-extract resume" in caplog.text


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
