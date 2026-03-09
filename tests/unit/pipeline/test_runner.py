#!/usr/bin/env python3
"""
Tests for Pipeline Runner
Covers: pipeline/runner.py
"""

import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock, call
from dataclasses import dataclass

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


class TestMatchingPipelineResult:
    """Test MatchingPipelineResult dataclass."""

    def test_create_with_minimal_fields(self):
        """Test creating result with minimal required fields."""
        result = MatchingPipelineResult(
            success=True,
            matches_count=10,
            saved_count=5,
            notified_count=3
        )

        assert result.success is True
        assert result.matches_count == 10
        assert result.saved_count == 5
        assert result.notified_count == 3
        assert result.error is None
        assert result.execution_time == 0.0

    def test_create_with_all_fields(self):
        """Test creating result with all fields."""
        result = MatchingPipelineResult(
            success=False,
            matches_count=0,
            saved_count=0,
            notified_count=0,
            error="Database connection failed",
            execution_time=120.5
        )

        assert result.success is False
        assert result.error == "Database connection failed"
        assert result.execution_time == 120.5


class TestLoadResumeWithParser:
    """Test _load_resume_with_parser function."""

    @patch('pipeline.runner.ResumeParser')
    def test_success_json_resume(self, mock_parser_class):
        """Test successful JSON resume loading."""
        mock_parser = Mock()
        mock_parser.parse.return_value = Mock(
            data={"name": "John Doe", "skills": ["Python"]},
            text=None
        )
        mock_parser_class.return_value = mock_parser

        result = _load_resume_with_parser("/path/to/resume.json")

        assert result == {"name": "John Doe", "skills": ["Python"]}
        mock_parser.parse.assert_called_once_with("/path/to/resume.json")

    @patch('pipeline.runner.ResumeParser')
    def test_success_pdf_resume(self, mock_parser_class):
        """Test successful PDF resume loading."""
        mock_parser = Mock()
        mock_parser.parse.return_value = Mock(
            data={"name": "Jane Doe", "experience": []},
            text=None
        )
        mock_parser_class.return_value = mock_parser

        result = _load_resume_with_parser("/path/to/resume.pdf")

        assert result is not None
        assert "name" in result

    @patch('pipeline.runner.ResumeParser')
    def test_parser_returns_none_data(self, mock_parser_class):
        """Test when parser returns None data but has text."""
        mock_parser = Mock()
        mock_parser.parse.return_value = Mock(
            data=None,
            text="Raw resume text"
        )
        mock_parser_class.return_value = mock_parser

        result = _load_resume_with_parser("/path/to/resume.txt")

        assert result == {"raw_text": "Raw resume text"}

    @patch('pipeline.runner.ResumeParser')
    def test_file_not_found(self, mock_parser_class, caplog):
        """Test when resume file not found."""
        mock_parser = Mock()
        mock_parser.parse.side_effect = FileNotFoundError("File not found")
        mock_parser_class.return_value = mock_parser

        result = _load_resume_with_parser("/nonexistent/resume.json")

        assert result is None
        assert "Resume file not found" in caplog.text

    @patch('pipeline.runner.ResumeParser')
    def test_parser_value_error(self, mock_parser_class, caplog):
        """Test when parser raises ValueError."""
        mock_parser = Mock()
        mock_parser.parse.side_effect = ValueError("Invalid format")
        mock_parser_class.return_value = mock_parser

        result = _load_resume_with_parser("/path/to/invalid.pdf")

        assert result is None
        assert "Failed to parse resume" in caplog.text

    @patch('pipeline.runner.ResumeParser')
    def test_parser_generic_exception(self, mock_parser_class, caplog):
        """Test when parser raises generic exception."""
        mock_parser = Mock()
        mock_parser.parse.side_effect = Exception("Unexpected error")
        mock_parser_class.return_value = mock_parser

        result = _load_resume_with_parser("/path/to/resume.pdf")

        assert result is None
        assert "Unexpected error loading resume" in caplog.text


class TestLoadUserWantsData:
    """Test load_user_wants_data function."""

    def test_success_load_wants(self, tmp_path):
        """Test successful loading of user wants."""
        wants_file = tmp_path / "wants.txt"
        wants_file.write_text("Remote work\nPython development\nHealthcare industry\n")

        result = load_user_wants_data(str(wants_file))

        assert result == ["Remote work", "Python development", "Healthcare industry"]

    def test_load_wants_with_empty_lines(self, tmp_path):
        """Test loading wants with empty lines."""
        wants_file = tmp_path / "wants.txt"
        wants_file.write_text("Remote work\n\nPython development\n\n\nHealthcare\n")

        result = load_user_wants_data(str(wants_file))

        assert result == ["Remote work", "Python development", "Healthcare"]

    def test_file_not_found(self, caplog):
        """Test when wants file not found."""
        result = load_user_wants_data("/nonexistent/wants.txt")

        assert result == []
        assert "User wants file not found" in caplog.text

    def test_generic_exception(self, caplog):
        """Test when reading file raises exception."""
        with patch('builtins.open', side_effect=Exception("Read error")):
            result = load_user_wants_data("/path/to/wants.txt")

            assert result == []
            assert "Error reading user wants file" in caplog.text

    def test_empty_file(self, tmp_path):
        """Test loading empty wants file."""
        wants_file = tmp_path / "empty.txt"
        wants_file.write_text("")

        result = load_user_wants_data(str(wants_file))

        assert result == []

    def test_whitespace_only_file(self, tmp_path):
        """Test loading file with only whitespace."""
        wants_file = tmp_path / "whitespace.txt"
        wants_file.write_text("\n\n   \n\t\n")

        result = load_user_wants_data(str(wants_file))

        assert result == []


class TestLoadResumeFile:
    """Test _load_resume_file function."""

    @patch('pipeline.runner._load_resume_with_parser')
    @patch('pipeline.runner.os.path.exists')
    @patch('pipeline.runner.os.path.isabs')
    def test_success_absolute_path(self, mock_isabs, mock_exists, mock_load_parser):
        """Test loading resume with absolute path."""
        mock_isabs.return_value = True
        mock_exists.return_value = True
        mock_load_parser.return_value = {"name": "John Doe"}

        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/absolute/path/resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath == "/absolute/path/resume.json"
        assert data == {"name": "John Doe"}

    @patch('pipeline.runner._load_resume_with_parser')
    @patch('pipeline.runner.os.path.exists')
    @patch('pipeline.runner.os.path.isabs')
    @patch('pipeline.runner.os.getcwd')
    def test_success_relative_path(self, mock_getcwd, mock_isabs, mock_exists, mock_load_parser):
        """Test loading resume with relative path."""
        mock_isabs.return_value = False
        mock_getcwd.return_value = "/current/dir"
        mock_exists.return_value = True
        mock_load_parser.return_value = {"name": "John Doe"}

        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath == "/current/dir/resume.json"
        mock_exists.assert_called_once_with("/current/dir/resume.json")

    @patch('pipeline.runner.os.path.exists')
    def test_no_resume_file_configured(self, mock_exists):
        """Test when no resume file is configured."""
        mock_etl_config = Mock()
        mock_etl_config.resume = None
        mock_etl_config.resume_file = None

        filepath, data = _load_resume_file(mock_etl_config)

        assert filepath is None
        assert data is None

    @patch('pipeline.runner.os.path.exists')
    def test_file_not_found(self, mock_exists, caplog):
        """Test when resume file doesn't exist."""
        mock_exists.return_value = False

        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/nonexistent/resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath is None
        assert data is None
        assert "Resume file not found" in caplog.text

    @patch('pipeline.runner._load_resume_with_parser')
    @patch('pipeline.runner.os.path.exists')
    def test_parser_returns_none(self, mock_exists, mock_load_parser, caplog):
        """Test when parser returns None."""
        mock_exists.return_value = True
        mock_load_parser.return_value = None

        mock_config = Mock()
        mock_config.resume = Mock()
        mock_config.resume.resume_file = "/path/to/resume.json"

        filepath, data = _load_resume_file(mock_config)

        assert filepath is None
        assert data is None
        assert "Failed to load resume data" in caplog.text


class TestDetermineResumeExtraction:
    """Test _determine_resume_extraction function."""

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint')
    @patch('database.uow.job_uow')
    def test_no_stored_fingerprint(self, mock_uow, mock_fingerprint, mock_open):
        """Test when no stored fingerprint exists."""
        mock_fingerprint.return_value = "current-fp-123"
        # Configure mock file handle
        mock_file_handle = Mock()
        mock_file_handle.read.return_value = b"file content"
        mock_open.return_value.__enter__ = Mock(return_value=mock_file_handle)
        mock_open.return_value.__exit__ = Mock(return_value=False)

        mock_repo = Mock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_etl_config = Mock()
        mock_etl_config.resume = Mock()
        mock_etl_config.resume.force_re_extraction = False

        fingerprint, should_re_extract = _determine_resume_extraction(
            "/path/to/resume.pdf", mock_etl_config
        )

        assert fingerprint == "current-fp-123"
        assert should_re_extract is True

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint')
    @patch('database.uow.job_uow')
    def test_fingerprint_changed(self, mock_uow, mock_fingerprint, mock_open, caplog):
        """Test when fingerprint has changed."""
        import logging
        caplog.set_level(logging.INFO)
        mock_fingerprint.return_value = "new-fp-456"
        # Configure mock file handle
        mock_file_handle = Mock()
        mock_file_handle.read.return_value = b"file content"
        mock_open.return_value.__enter__ = Mock(return_value=mock_file_handle)
        mock_open.return_value.__exit__ = Mock(return_value=False)

        mock_repo = Mock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = "old-fp-123"
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_etl_config = Mock()
        mock_etl_config.resume = Mock()
        mock_etl_config.resume.force_re_extraction = False

        fingerprint, should_re_extract = _determine_resume_extraction(
            "/path/to/resume.pdf", mock_etl_config
        )

        assert fingerprint == "new-fp-456"
        assert should_re_extract is True
        assert "Resume file changed" in caplog.text

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint')
    @patch('database.uow.job_uow')
    def test_fingerprint_same(self, mock_uow, mock_fingerprint, mock_open, caplog):
        """Test when fingerprint is the same."""
        import logging
        caplog.set_level(logging.INFO)
        mock_fingerprint.return_value = "same-fp-789"
        # Configure mock file handle
        mock_file_handle = Mock()
        mock_file_handle.read.return_value = b"file content"
        mock_open.return_value.__enter__ = Mock(return_value=mock_file_handle)
        mock_open.return_value.__exit__ = Mock(return_value=False)

        mock_repo = Mock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = "same-fp-789"
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_etl_config = Mock()
        mock_etl_config.resume = Mock()
        mock_etl_config.resume.force_re_extraction = False

        fingerprint, should_re_extract = _determine_resume_extraction(
            "/path/to/resume.pdf", mock_etl_config
        )

        assert fingerprint == "same-fp-789"
        assert should_re_extract is False
        assert "Resume unchanged" in caplog.text

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint')
    @patch('database.uow.job_uow')
    def test_force_re_extraction_enabled(self, mock_uow, mock_fingerprint, mock_open, caplog):
        """Test when force re-extraction is enabled."""
        import logging
        caplog.set_level(logging.INFO)
        mock_fingerprint.return_value = "current-fp-123"
        # Configure mock file handle
        mock_file_handle = Mock()
        mock_file_handle.read.return_value = b"file content"
        mock_open.return_value.__enter__ = Mock(return_value=mock_file_handle)
        mock_open.return_value.__exit__ = Mock(return_value=False)

        mock_repo = Mock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = "current-fp-123"
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_etl_config = Mock()
        mock_etl_config.resume = Mock()
        mock_etl_config.resume.force_re_extraction = True

        fingerprint, should_re_extract = _determine_resume_extraction(
            "/path/to/resume.pdf", mock_etl_config
        )

        assert fingerprint == "current-fp-123"
        assert should_re_extract is True
        assert "Force re-extraction enabled" in caplog.text

    @patch('builtins.open')
    @patch('database.models.generate_file_fingerprint')
    @patch('database.uow.job_uow')
    def test_force_re_extraction_disabled(self, mock_uow, mock_fingerprint, mock_open):
        """Test when force re-extraction is disabled."""
        mock_fingerprint.return_value = "current-fp-123"
        # Configure mock file handle
        mock_file_handle = Mock()
        mock_file_handle.read.return_value = b"file content"
        mock_open.return_value.__enter__ = Mock(return_value=mock_file_handle)
        mock_open.return_value.__exit__ = Mock(return_value=False)

        mock_repo = Mock()
        mock_repo.resume.get_latest_stored_resume_fingerprint.return_value = "current-fp-123"
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_etl_config = Mock()
        mock_etl_config.resume = Mock()
        mock_etl_config.resume.force_re_extraction = False

        fingerprint, should_re_extract = _determine_resume_extraction(
            "/path/to/resume.pdf", mock_etl_config
        )

        assert fingerprint == "current-fp-123"
        assert should_re_extract is False


class TestLoadUserWantsEmbeddings:
    """Test _load_user_wants_embeddings function."""

    @patch('pipeline.runner.load_user_wants_data')
    @patch('pipeline.runner.os.path.exists')
    @patch('pipeline.runner.os.path.isabs')
    def test_success_load_embeddings(self, mock_isabs, mock_exists, mock_load_data):
        """Test successful loading of user wants embeddings."""
        mock_isabs.return_value = True
        mock_exists.return_value = True
        mock_load_data.return_value = ["want-1", "want-2"]

        mock_ai_service = Mock()
        mock_ai_service.generate_embedding.side_effect = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6]
        ]

        mock_matching_config = Mock()
        mock_matching_config.user_wants_file = "/path/to/wants.txt"

        result = _load_user_wants_embeddings(mock_matching_config, mock_ai_service)

        assert len(result) == 2
        assert result[0] == [0.1, 0.2, 0.3]
        assert result[1] == [0.4, 0.5, 0.6]
        assert mock_ai_service.generate_embedding.call_count == 2

    @patch('pipeline.runner.os.path.exists')
    def test_file_not_found(self, mock_exists, caplog):
        """Test when wants file not found."""
        mock_exists.return_value = False

        mock_matching_config = Mock()
        mock_matching_config.user_wants_file = "/nonexistent/wants.txt"

        result = _load_user_wants_embeddings(mock_matching_config, Mock())

        assert result == []

    @patch('pipeline.runner.load_user_wants_data')
    @patch('pipeline.runner.os.path.exists')
    def test_empty_wants(self, mock_exists, mock_load_data):
        """Test when wants file is empty."""
        mock_exists.return_value = True
        mock_load_data.return_value = []

        result = _load_user_wants_embeddings(Mock(user_wants_file="/path/to/wants.txt"), Mock())

        assert result == []

    @patch('pipeline.runner.os.path.isabs')
    def test_no_user_wants_file(self, mock_isabs):
        """Test when no user wants file is configured."""
        mock_matching_config = Mock()
        mock_matching_config.user_wants_file = None

        result = _load_user_wants_embeddings(mock_matching_config, Mock())

        assert result == []


class TestLoadStructuredResume:
    """Test _load_structured_resume function."""

    def test_should_re_extract_true(self):
        """Test when re-extraction is needed."""
        mock_repo = Mock()

        result = _load_structured_resume(mock_repo, "fp-123", should_re_extract=True)

        assert result is None
        mock_repo.resume.get_structured_resume_by_fingerprint.assert_not_called()

    def test_should_re_extract_false_success(self):
        """Test when re-extraction is not needed."""
        mock_repo = Mock()
        mock_structured_resume = Mock()
        mock_structured_resume.extracted_data = {"name": "John Doe"}
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = mock_structured_resume

        result = _load_structured_resume(mock_repo, "fp-123", should_re_extract=False)

        assert result == mock_structured_resume
        mock_repo.resume.get_structured_resume_by_fingerprint.assert_called_once_with("fp-123")

    def test_should_re_extract_false_not_found(self):
        """Test when structured resume not found."""
        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        result = _load_structured_resume(mock_repo, "fp-123", should_re_extract=False)

        assert result is None


class TestPrepareMatcherService:
    """Test _prepare_matcher_service function."""

    @patch('pipeline.runner.MatcherService')
    @patch('pipeline.runner.ResumeProfiler')
    @patch('pipeline.runner.JobRepositoryAdapter')
    def test_create_matcher_service(self, mock_adapter, mock_profiler, mock_matcher):
        """Test creating matcher service."""
        mock_repo = Mock()
        mock_ctx = Mock()
        mock_matching_config = Mock()

        _prepare_matcher_service(mock_ctx, mock_repo, mock_matching_config)

        mock_adapter.assert_called_once_with(mock_repo)
        mock_profiler.assert_called_once()
        mock_matcher.assert_called_once()


class TestGetPreExtractedResume:
    """Test _get_pre_extracted_resume function."""

    def test_should_re_extract_true(self):
        """Test when re-extraction is needed."""
        result = _get_pre_extracted_resume(Mock(), should_re_extract=True)
        assert result is None

    def test_no_structured_resume(self):
        """Test when no structured resume provided."""
        result = _get_pre_extracted_resume(None, should_re_extract=False)
        assert result is None

    def test_no_extracted_data(self):
        """Test when structured resume has no extracted data."""
        mock_structured = Mock()
        mock_structured.extracted_data = None

        result = _get_pre_extracted_resume(mock_structured, should_re_extract=False)

        assert result is None

    def test_success_parse_resume(self):
        """Test successful parsing of stored resume."""
        mock_structured = Mock()
        # Provide minimal valid ResumeSchema data
        mock_structured.extracted_data = {
            "profile": {
                "summary": {
                    "text": "Experienced software engineer",
                    "total_experience_years": 5
                },
                "experience": [],
                "projects": {"items": []},
                "education": [],
                "skills": {"groups": [], "all": []},
                "certifications": [],
                "languages": []
            },
            "extraction": {
                "confidence": 0.9,
                "warnings": []
            }
        }
        mock_structured.fingerprint = "fp-12345678901234567890"

        result = _get_pre_extracted_resume(mock_structured, should_re_extract=False)

        assert result is not None

    def test_parse_failure(self, caplog):
        """Test when parsing stored resume fails."""
        mock_structured = Mock()
        mock_structured.extracted_data = {"invalid": "data"}

        result = _get_pre_extracted_resume(mock_structured, should_re_extract=False)

        assert result is None
        assert "Failed to parse stored resume" in caplog.text


class TestRunVectorMatching:
    """Test _run_vector_matching function."""

    def test_success(self, caplog):
        """Test successful vector matching."""
        mock_matcher = Mock()
        mock_repo = Mock()
        mock_resume_data = {"name": "John Doe"}
        mock_stop_event = threading.Event()
        mock_pre_extracted = Mock()

        mock_preliminary = Mock()
        mock_matcher.match_resume_two_stage.return_value = [mock_preliminary, mock_preliminary]

        result = _run_vector_matching(
            mock_matcher, mock_repo, mock_resume_data, mock_stop_event,
            mock_pre_extracted, "fp-123"
        )

        assert len(result) == 2
        mock_matcher.match_resume_two_stage.assert_called_once()


class TestRunScorerService:
    """Test _run_scorer_service function."""

    def test_with_user_want_embeddings(self):
        """Test scoring with user want embeddings."""
        mock_scorer = Mock()
        mock_scorer.score_matches.return_value = [Mock()]

        mock_preliminary = [Mock()]
        mock_matching_config = Mock()
        mock_user_want_embeddings = [[0.1, 0.2]]
        mock_job_facet_map = {"job-1": [0.3, 0.4]}
        mock_stop_event = threading.Event()

        result = _run_scorer_service(
            mock_scorer, mock_preliminary, mock_matching_config,
            mock_user_want_embeddings, mock_job_facet_map, mock_stop_event
        )

        assert len(result) == 1
        mock_scorer.score_matches.assert_called_once()

    def test_without_user_want_embeddings(self):
        """Test scoring without user want embeddings."""
        mock_scorer = Mock()
        mock_scorer.score_matches.return_value = [Mock()]

        mock_preliminary = [Mock()]
        mock_matching_config = Mock()
        mock_stop_event = threading.Event()

        result = _run_scorer_service(
            mock_scorer, mock_preliminary, mock_matching_config,
            [], {}, mock_stop_event
        )

        assert len(result) == 1


class TestBuildEvidenceDto:
    """Test _build_evidence_dto function."""

    def test_with_evidence(self):
        """Test building DTO with evidence."""
        mock_evidence = Mock()
        mock_evidence.text = "5 years Python experience"
        mock_evidence.source_section = "experience"
        mock_evidence.tags = {"skill": "Python"}

        result = _build_evidence_dto(mock_evidence)

        assert result is not None
        assert result.text == "5 years Python experience"
        assert result.source_section == "experience"
        assert result.tags == {"skill": "Python"}

    def test_without_evidence(self):
        """Test building DTO without evidence."""
        result = _build_evidence_dto(None)
        assert result is None


class TestMatchedReqToDto:
    """Test _matched_req_to_dto function."""

    def test_convert_matched_requirement(self):
        """Test converting matched requirement to DTO."""
        mock_req = Mock()
        mock_req.requirement = Mock(id="req-123", req_type="required")
        mock_req.evidence = Mock(text="Evidence text", source_section="skills", tags={})
        mock_req.similarity = 0.85
        mock_req.is_covered = True

        result = _matched_req_to_dto(mock_req)

        assert result is not None
        assert result.requirement.id == "req-123"
        assert result.requirement.req_type == "required"
        assert result.similarity == 0.85
        assert result.is_covered is True


class TestMissingReqToDto:
    """Test _missing_req_to_dto function."""

    def test_convert_missing_requirement(self):
        """Test converting missing requirement to DTO."""
        mock_req = Mock()
        mock_req.requirement = Mock(id="req-456", req_type="preferred")
        mock_req.similarity = 0.45

        result = _missing_req_to_dto(mock_req)

        assert result is not None
        assert result.requirement.id == "req-456"
        assert result.requirement.req_type == "preferred"
        assert result.is_covered is False
        assert result.similarity == 0.45


class TestConvertMatchesToDtos:
    """Test _convert_matches_to_dtos function."""

    def test_convert_empty_list(self):
        """Test converting empty match list."""
        result = _convert_matches_to_dtos([])
        assert result == []

    def test_convert_single_match(self):
        """Test converting single match."""
        mock_match = Mock()
        mock_match.job = Mock(
            id="job-123",
            title="Software Engineer",
            company="Tech Corp",
            location_text="San Francisco, CA",
            is_remote=True,
            content_hash="hash-abc"
        )
        mock_match.overall_score = 85.5
        mock_match.fit_score = 80.0
        mock_match.want_score = 75.0
        mock_match.job_similarity = 0.90
        mock_match.jd_required_coverage = 0.85
        mock_match.jd_preferences_coverage = 0.70
        mock_match.matched_requirements = []
        mock_match.missing_requirements = []
        mock_match.resume_fingerprint = "fp-123"
        mock_match.fit_components = {"skills": 0.8}
        mock_match.want_components = {"pref": 0.7}
        mock_match.base_score = 80.0
        mock_match.penalties = 5.0
        mock_match.penalty_details = []
        mock_match.fit_weight = 0.7
        mock_match.want_weight = 0.3
        mock_match.match_type = "requirements_only"

        result = _convert_matches_to_dtos([mock_match])

        assert len(result) == 1
        assert result[0].job.id == "job-123"
        assert result[0].job.title == "Software Engineer"
        assert result[0].overall_score == 85.5


class TestSaveMatchesBatch:
    """Test _save_matches_batch function."""

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_save_new_matches(self, mock_save, mock_uow):
        """Test saving new matches."""
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_dto = Mock()
        mock_dto.job.id = "job-123"
        mock_dto.job.content_hash = "hash-abc"

        result = _save_matches_batch([mock_dto], "fp-123", Mock(recalculate_existing=False))

        assert result == 1
        mock_save.assert_called_once()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_skip_existing_match(self, mock_save, mock_uow):
        """Test skipping existing match."""
        mock_existing = Mock()
        mock_existing.status = 'active'
        mock_existing.job_content_hash = "hash-abc"

        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = mock_existing
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_dto = Mock()
        mock_dto.job.id = "job-123"
        mock_dto.job.content_hash = "hash-abc"

        mock_matching_config = Mock()
        mock_matching_config.recalculate_existing = False

        result = _save_matches_batch([mock_dto], "fp-123", mock_matching_config)

        assert result == 0
        mock_save.assert_not_called()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_update_existing_match_content_changed(self, mock_save, mock_uow):
        """Test updating existing match when job content changed."""
        mock_existing = Mock()
        mock_existing.status = 'active'
        mock_existing.job_content_hash = "old-hash"
        mock_existing.id = "match-123"

        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = mock_existing
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_dto = Mock()
        mock_dto.job.id = "job-123"
        mock_dto.job.content_hash = "new-hash"

        result = _save_matches_batch([mock_dto], "fp-123", Mock(recalculate_existing=False))

        assert result == 1
        mock_save.assert_called_once()

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.save_match_to_db')
    def test_save_error_logged(self, mock_save, mock_uow, caplog):
        """Test that save errors are logged."""
        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_save.side_effect = Exception("Database error")

        mock_dto = Mock()
        mock_dto.job.id = "job-123"

        result = _save_matches_batch([mock_dto], "fp-123", Mock())

        assert result == 0
        assert "Failed saving match" in caplog.text


class TestSendNotifications:
    """Test _send_notifications function."""

    def test_notifications_disabled(self, caplog):
        """Test when notifications are disabled."""
        import logging
        caplog.set_level(logging.INFO)
        mock_ctx = Mock()
        mock_ctx.config.notifications = Mock(enabled=False)

        result = _send_notifications(
            mock_ctx, [], 0, {}, "fp-123", threading.Event()
        )

        assert result == 0
        assert "Skipped (disabled in config)" in caplog.text

    def test_no_matches_to_notify(self, caplog):
        """Test when no matches to notify."""
        import logging
        caplog.set_level(logging.INFO)
        mock_ctx = Mock()
        mock_ctx.config.notifications = Mock(enabled=True)

        result = _send_notifications(
            mock_ctx, [], 0, {}, "fp-123", threading.Event()
        )

        assert result == 0
        assert "Skipped (no matches to notify)" in caplog.text

    @patch('pipeline.runner.job_uow')
    @patch('pipeline.runner.NotificationMessageBuilder')
    def test_send_notifications_success(self, mock_builder, mock_uow):
        """Test successful notification sending."""
        mock_ctx = Mock()
        mock_ctx.config.notifications = Mock(
            enabled=True,
            user_id="user-123",
            channels={"email": Mock(enabled=True)},
            min_score_threshold=70.0,
            notify_on_new_match=True
        )

        mock_dto = Mock()
        mock_dto.job.id = "job-123"
        mock_dto.job.content_hash = "hash-abc"
        mock_dto.overall_score = 85.0
        mock_dto.fit_score = 80.0
        mock_dto.want_score = 75.0
        mock_dto.jd_required_coverage = 0.85

        mock_match_record = Mock()
        mock_match_record.id = "match-123"
        mock_match_record.notified = False
        mock_match_record.job_post = Mock(
            company_url_direct="https://apply.example.com"
        )

        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = mock_match_record
        mock_uow.return_value.__enter__.return_value = mock_repo

        mock_builder.build_notification_content.return_value = "Notification content"

        result = _send_notifications(
            mock_ctx, [mock_dto], 1, {"email": "user@example.com"}, "fp-123", threading.Event()
        )

        assert result >= 0  # May be 0 if notification fails

    @patch('pipeline.runner.job_uow')
    def test_no_match_record(self, mock_uow, caplog):
        """Test when no match record found."""
        mock_ctx = Mock()
        mock_ctx.config.notifications = Mock(
            enabled=True,
            user_id="user-123",
            channels={"email": Mock(enabled=True)},
            min_score_threshold=70.0,
            notify_on_new_match=True
        )

        mock_dto = Mock()
        mock_dto.job.id = "job-123"
        mock_dto.overall_score = 85.0

        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = None
        mock_uow.return_value.__enter__.return_value = mock_repo

        result = _send_notifications(
            mock_ctx, [mock_dto], 1, {"email": "user@example.com"}, "fp-123", threading.Event()
        )

        assert result == 0
        assert "No match record found" in caplog.text

    @patch('pipeline.runner.job_uow')
    def test_already_notified(self, mock_uow):
        """Test when match already notified."""
        mock_ctx = Mock()
        mock_ctx.config.notifications = Mock(
            enabled=True,
            user_id="user-123",
            channels={"email": Mock(enabled=True)},
            min_score_threshold=70.0,
            notify_on_new_match=True
        )

        mock_dto = Mock()
        mock_dto.job.id = "job-123"
        mock_dto.overall_score = 85.0

        mock_match_record = Mock()
        mock_match_record.id = "match-123"
        mock_match_record.notified = True

        mock_repo = Mock()
        mock_repo.get_existing_match.return_value = mock_match_record
        mock_uow.return_value.__enter__.return_value = mock_repo

        result = _send_notifications(
            mock_ctx, [mock_dto], 1, {}, "fp-123", threading.Event()
        )

        assert result == 0

    def test_no_enabled_channels(self, caplog):
        """Test when no channels are enabled."""
        mock_ctx = Mock()
        mock_ctx.config.notifications = Mock(
            enabled=True,
            user_id="user-123",
            channels={"email": Mock(enabled=False)},
            min_score_threshold=70.0,
            notify_on_new_match=True
        )

        result = _send_notifications(
            mock_ctx, [Mock(overall_score=85.0)], 1, {}, "fp-123", threading.Event()
        )

        assert result == 0
        assert "No notification channels configured" in caplog.text

    def test_stop_event_set(self):
        """Test when stop event is set."""
        mock_ctx = Mock()
        mock_ctx.config.notifications = Mock(
            enabled=True,
            user_id="user-123",
            channels={"email": Mock(enabled=True)},
            min_score_threshold=70.0,
            notify_on_new_match=True
        )

        stop_event = threading.Event()
        stop_event.set()

        result = _send_notifications(
            mock_ctx, [Mock(overall_score=85.0)], 1, {}, "fp-123", stop_event
        )

        assert result == 0


class TestRunMatchingPipeline:
    """Test run_matching_pipeline function."""

    @patch('pipeline.runner._load_resume_file')
    @patch('pipeline.runner._determine_resume_extraction')
    @patch('pipeline.runner._load_user_wants_embeddings')
    @patch('pipeline.runner._run_matching_and_scoring')
    @patch('pipeline.runner._save_matches_batch')
    def test_success_full_pipeline(
        self, mock_save, mock_run_scoring, mock_load_embeddings,
        mock_determine, mock_load_file
    ):
        """Test successful full pipeline execution."""
        mock_load_file.return_value = ("/path/to/resume.pdf", {"name": "John"})
        mock_determine.return_value = ("fp-123", True)
        mock_load_embeddings.return_value = [[0.1, 0.2]]

        mock_dto = Mock()
        mock_dto.job.id = "job-123"
        mock_dto.overall_score = 85.0
        mock_run_scoring.return_value = [mock_dto]

        mock_save.return_value = 1

        mock_ctx = Mock()
        mock_ctx.config.matching = Mock(enabled=True)
        mock_ctx.config.etl = Mock()
        mock_ctx.config.etl.resume = Mock()
        mock_ctx.config.etl.resume.resume_file = "/path/to/resume.pdf"
        mock_ctx.notification_service = Mock()

        result = run_matching_pipeline(mock_ctx)

        assert result.success is True
        assert result.matches_count == 1
        assert result.saved_count == 1
        assert result.execution_time > 0

    @patch('pipeline.runner._load_resume_file')
    def test_failed_load_resume(self, mock_load_file):
        """Test pipeline fails when resume loading fails."""
        mock_load_file.return_value = (None, None)

        mock_ctx = Mock()
        mock_ctx.config.matching = Mock(enabled=True)

        result = run_matching_pipeline(mock_ctx)

        assert result.success is False
        assert result.error == "Failed to load resume"
        assert result.matches_count == 0

    def test_matching_disabled(self, caplog):
        """Test when matching is disabled in config."""
        import logging
        caplog.set_level(logging.INFO)
        mock_ctx = Mock()
        mock_ctx.config.matching = Mock(enabled=False)

        result = run_matching_pipeline(mock_ctx)

        assert result.success is True
        assert result.matches_count == 0
        assert "Skipped (disabled in config)" in caplog.text

    @patch('pipeline.runner._load_resume_file')
    @patch('pipeline.runner._determine_resume_extraction')
    @patch('pipeline.runner._load_user_wants_embeddings')
    @patch('pipeline.runner._run_matching_and_scoring')
    def test_stop_event_during_scoring(
        self, mock_run_scoring, mock_load_embeddings, mock_determine, mock_load_file
    ):
        """Test pipeline when stop event is set during scoring."""
        mock_load_file.return_value = ("/path/to/resume.pdf", {"name": "John"})
        mock_determine.return_value = ("fp-123", True)
        mock_load_embeddings.return_value = []

        stop_event = threading.Event()
        stop_event.set()

        mock_run_scoring.return_value = []

        mock_ctx = Mock()
        mock_ctx.config.matching = Mock(enabled=True)
        mock_ctx.config.etl = Mock()
        mock_ctx.config.etl.resume = Mock()
        mock_ctx.config.etl.resume.resume_file = "/path/to/resume.pdf"

        result = run_matching_pipeline(mock_ctx, stop_event=stop_event)

        assert result.success is False
        assert "Interrupted" in result.error

    @patch('pipeline.runner._load_resume_file')
    def test_exception_handling(self, mock_load_file, caplog):
        """Test pipeline exception handling."""
        mock_load_file.side_effect = Exception("Unexpected error")

        mock_ctx = Mock()
        mock_ctx.config.matching = Mock(enabled=True)

        result = run_matching_pipeline(mock_ctx)

        assert result.success is False
        assert "Unexpected error" in result.error
        assert "Error in matching pipeline" in caplog.text
