#!/usr/bin/env python3
"""
Unit Tests: ETL Orchestrator (JobETLService)

Tests the ETL orchestrator service functionality without requiring
running services. Tests job ingestion, extraction, embedding, and
resume processing.

Usage:
    uv run pytest tests/unit/etl/test_orchestrator.py -v
"""

import pytest
from unittest.mock import Mock, patch, mock_open


class TestJobETLServiceInitialization:
    """Test JobETLService initialization."""

    def test_init_with_ai_service(self):
        """Test JobETLService initializes with AI service."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        assert service.ai is mock_ai


class TestIngestOne:
    """Test ingest_one method."""

    def test_ingest_new_job(self):
        """Test ingesting a new job (not duplicate)."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.get_by_source.return_value = None
        mock_repo.get_by_fingerprint.return_value = None

        job_data = {
            "title": "Software Engineer",
            "company_name": "Tech Corp",
            "location": "San Francisco, CA",
            "description": "Job description here",
        }

        mock_job_post = Mock()
        mock_job_post.id = "job-123"
        mock_repo.create_job_post.return_value = mock_job_post

        service.ingest_one(mock_repo, job_data, "linkedin")

        mock_repo.get_by_fingerprint.assert_called_once()
        mock_repo.create_job_post.assert_called_once()
        mock_repo.get_or_create_source.assert_called_once()
        mock_repo.save_job_content.assert_called_once()

    def test_ingest_duplicate_job(self):
        """Test ingesting a duplicate job."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.get_by_source.return_value = None
        mock_job_post = Mock()
        mock_job_post.id = "job-456"
        mock_repo.get_by_fingerprint.return_value = mock_job_post

        job_data = {
            "title": "Software Engineer",
            "company_name": "Tech Corp",
            "location": "San Francisco, CA",
        }

        service.ingest_one(mock_repo, job_data, "indeed")

        mock_repo.update_timestamp.assert_called_once_with(mock_job_post)
        mock_repo.create_job_post.assert_not_called()

    def test_ingest_missing_title(self):
        """Test ingesting job with missing title."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.get_by_source.return_value = None

        job_data = {"company_name": "Tech Corp", "location": "San Francisco, CA"}

        service.ingest_one(mock_repo, job_data, "linkedin")

        mock_repo.get_by_fingerprint.assert_not_called()
        mock_repo.create_job_post.assert_not_called()

    def test_ingest_missing_company(self):
        """Test ingesting job with missing company."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.get_by_source.return_value = None

        job_data = {"title": "Software Engineer", "location": "San Francisco, CA"}

        service.ingest_one(mock_repo, job_data, "linkedin")

        mock_repo.get_by_fingerprint.assert_not_called()
        mock_repo.create_job_post.assert_not_called()


class TestExtractOne:
    """Test extract_one method."""

    def test_extract_success(self):
        """Test successful extraction."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.extract_requirements_data.return_value = {
            "requirements": [
                {"text": "5 years Python experience", "category": "skill"},
                {"text": "BS in Computer Science", "category": "education"},
            ],
            "benefits": [{"text": "Health insurance", "type": "medical"}],
        }

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.title = "Software Engineer"
        mock_job.description = (
            "Job description with Python APIs, distributed systems, and production support. " * 4
        )

        service.extract_one(mock_repo, mock_job)

        mock_ai.extract_requirements_data.assert_called_once_with(mock_job.description)
        mock_repo.update_job_metadata.assert_called_once()
        mock_repo.save_requirements.assert_called_once()
        mock_repo.save_benefits.assert_called_once()
        mock_repo.save_job_offerings_profile.assert_called_once()
        mock_repo.mark_as_extracted.assert_called_once()

        update_payload = mock_repo.update_content_metadata.call_args[0][1]
        assert "canonical_job_summary" in update_payload
        assert "canonical_job_summary_version" in update_payload
        assert "canonical_job_summary_hash" in update_payload
        offerings_payload = mock_repo.save_job_offerings_profile.call_args.args[1]
        assert offerings_payload["schema_version"] == 1
        assert offerings_payload["confidence"] <= 0.4

    def test_extract_empty_requirements_saves_minimal_extraction(self):
        """Empty requirement extraction should not block later jobs."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.extract_requirements_data.return_value = {"requirements": [], "benefits": []}

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.title = "Project Manager"
        mock_job.company = "MoneyForward"
        mock_job.description = (
            "This is a detailed job description that is long enough to attempt extraction. " * 4
        )

        service.extract_one(mock_repo, mock_job)

        mock_ai.extract_requirements_data.assert_called_once_with(mock_job.description)
        mock_repo.save_requirements.assert_called_once_with(mock_job, [])
        mock_repo.save_benefits.assert_called_once_with(mock_job, [])
        mock_repo.save_job_offerings_profile.assert_called_once()
        mock_repo.mark_as_extracted.assert_called_once_with(mock_job)

        update_payload = mock_repo.update_content_metadata.call_args[0][1]
        assert update_payload["extraction_quality"] == "minimal"
        assert update_payload["extraction_warning"] == "empty_requirements_extraction"
        assert "canonical_job_summary" in update_payload

    def test_sparse_job_offerings_profile_uses_existing_metadata(self):
        from core.llm.schema_models import JobOfferingsProfile
        from etl.orchestrator import JobETLService

        profile = JobETLService._sparse_job_offerings_profile(
            Mock(
                location_text="Tokyo, Japan",
                work_from_home_type="Remote",
                is_remote=True,
                skills_raw="Python, Java",
            ),
            reason="short_description",
        )

        validated = JobOfferingsProfile.model_validate(profile)
        assert validated.work_arrangement == "Remote"
        assert validated.location_timezone[0].label == "location"
        assert validated.tech_environment[0].label == "tech stack"
        assert "Python" in validated.tech_environment[0].evidence
        assert validated.negative_signals[0].label == "low detail description"
        assert "short_description" in validated.negative_signals[0].evidence

    def test_extract_normalizes_top_level_required_alias(self):
        """Provider outputs using required/preferred aliases become requirements."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.extract_requirements_data.return_value = {
            "required": [
                {
                    "text": "Minimum 3 years of customer success experience",
                    "section": "WHAT YOU NEED",
                }
            ],
            "preferred": [
                "Experience with partner ecosystem operations",
            ],
            "benefits": [],
        }

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-alias"
        mock_job.title = "Customer Activation"
        mock_job.company = "Ramp"
        mock_job.description = (
            "Customer activation, partnerships, technical feedback, and product operations. " * 4
        )

        service.extract_one(mock_repo, mock_job)

        saved_requirements = mock_repo.save_requirements.call_args[0][1]
        assert saved_requirements == [
            {
                "req_type": "must_have",
                "category": "domain_knowledge",
                "text": "Minimum 3 years of customer success experience",
                "related_skills": [],
                "proficiency": None,
            },
            {
                "req_type": "nice_to_have",
                "category": "domain_knowledge",
                "text": "Experience with partner ecosystem operations",
                "related_skills": [],
                "proficiency": None,
            },
        ]
        update_payload = mock_repo.update_content_metadata.call_args[0][1]
        assert "required" not in update_payload
        assert "preferred" not in update_payload
        assert update_payload["tech_stack"] == []

    def test_extract_validation_error_uses_raw_data(self):
        """Test extraction handles validation errors gracefully."""
        from etl.orchestrator import JobETLService
        from pydantic import ValidationError

        mock_ai = Mock()
        mock_ai.extract_requirements_data.return_value = {
            "requirements": [{"text": "Test requirement"}],
            "benefits": [],
        }

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-raw"
        mock_job.title = "Data Engineer"
        mock_job.company = "Example"
        mock_job.description = (
            "Long enough description for extraction validation error coverage. " * 4
        )

        with patch("etl.orchestrator.JobExtraction") as mock_model:
            mock_model.model_validate.side_effect = ValidationError.from_exception_data(
                "JobExtraction", []
            )

            service.extract_one(mock_repo, mock_job)

            mock_repo.update_job_metadata.assert_called_once()

    def test_extract_empty_result_saves_minimal_extraction(self):
        """Empty extraction result should become sparse extracted metadata."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.extract_requirements_data.return_value = {"requirements": [], "benefits": []}

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.title = "Backend Engineer"
        mock_job.company = "Example"
        mock_job.description = "Backend APIs, Python services, and production operations. " * 4

        service.extract_one(mock_repo, mock_job)

        mock_repo.mark_as_extracted.assert_called_once_with(mock_job)
        update_payload = mock_repo.update_content_metadata.call_args[0][1]
        assert update_payload["requirements"] == []
        assert update_payload["benefits"] == []
        assert update_payload["extraction_quality"] == "minimal"

    def test_extract_short_description_skips_llm(self):
        """Low-detail placeholder rows should be extracted without LLM retries."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-short"
        mock_job.title = "Project Manager (CTO Office), Tokyo"
        mock_job.company = "MoneyForward"
        mock_job.description = "tokyodev listing: Project Manager (CTO Office), Tokyo"

        service.extract_one(mock_repo, mock_job)

        mock_ai.extract_requirements_data.assert_not_called()
        mock_repo.save_requirements.assert_called_once_with(mock_job, [])
        mock_repo.save_benefits.assert_called_once_with(mock_job, [])
        mock_repo.mark_as_extracted.assert_called_once_with(mock_job)

        update_payload = mock_repo.update_content_metadata.call_args[0][1]
        assert update_payload["extraction_quality"] == "minimal"
        assert update_payload["extraction_warning"] == "description_too_short_for_llm_extraction"


class TestEmbedJobOne:
    """Test embed_job_one method."""

    def test_embed_job_with_requirements(self):
        """Test job embedding with requirements."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, 0.2, 0.3]

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.canonical_job_summary = None
        mock_job.raw_payload = {}

        mock_req1 = Mock()
        mock_req1.text = "Requirement 1"
        mock_req2 = Mock()
        mock_req2.text = "Requirement 2"
        mock_job.requirements = [mock_req1, mock_req2]
        mock_job.benefits = []

        service.embed_job_one(mock_repo, mock_job)

        mock_ai.generate_embedding.assert_called_once()
        mock_repo.save_job_embedding.assert_called_once()

    def test_embed_job_with_benefits(self):
        """Test job embedding with benefits."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, 0.2, 0.3]

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.canonical_job_summary = None
        mock_job.raw_payload = {}
        mock_job.requirements = []

        mock_benefit = Mock()
        mock_benefit.text = "Health insurance"
        mock_job.benefits = [mock_benefit]

        service.embed_job_one(mock_repo, mock_job)

        mock_ai.generate_embedding.assert_called_once()

    def test_embed_job_no_requirements_or_benefits(self):
        """Test job embedding without requirements or benefits."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, 0.2, 0.3]

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.canonical_job_summary = None
        mock_job.raw_payload = {}
        mock_job.requirements = []
        mock_job.benefits = []
        mock_job.description = "Job description text" * 100

        service.embed_job_one(mock_repo, mock_job)

        mock_ai.generate_embedding.assert_called_once()
        mock_repo.save_job_embedding.assert_called_once()

    def test_embed_job_no_data_at_all(self):
        """Test job embedding with no data."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, 0.2, 0.3]

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.canonical_job_summary = None
        mock_job.raw_payload = {}
        mock_job.requirements = []
        mock_job.benefits = []
        mock_job.description = ""

        service.embed_job_one(mock_repo, mock_job)

        mock_ai.generate_embedding.assert_called_once()

    def test_embed_job_rejects_invalid_vector(self):
        """Test job embedding rejects invalid vectors."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = []

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.canonical_job_summary = None
        mock_job.raw_payload = {}
        mock_job.requirements = []
        mock_job.benefits = []
        mock_job.description = "description"

        with pytest.raises(ValueError, match="Invalid embedding vector"):
            service.embed_job_one(mock_repo, mock_job)

        mock_repo.save_job_embedding.assert_not_called()

    def test_embed_job_prefers_canonical_summary(self):
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, 0.2, 0.3]

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.canonical_job_summary = "Role: Senior backend engineer"
        mock_job.raw_payload = {"ai_job_summary": "legacy"}
        mock_job.requirements = []
        mock_job.benefits = []
        mock_job.description = "fallback"

        service.embed_job_one(mock_repo, mock_job)

        mock_ai.generate_embedding.assert_called_once_with("Role: Senior backend engineer")


class TestEmbedRequirementOne:
    """Test embed_requirement_one method."""

    def test_embed_requirement_success(self):
        """Test successful requirement embedding."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, 0.2, 0.3]

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_req = Mock()
        mock_req.id = "req-123"
        mock_req.text = "Python programming skills"

        service.embed_requirement_one(mock_repo, mock_req)

        mock_ai.generate_embedding.assert_called_once_with(mock_req.text)
        mock_repo.save_requirement_embedding.assert_called_once()

    def test_embed_requirement_rejects_invalid_vector(self):
        """Test requirement embedding rejects invalid vectors."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, float("nan")]

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_req = Mock()
        mock_req.id = "req-123"
        mock_req.text = "Python programming skills"

        with pytest.raises(ValueError, match="Invalid embedding vector"):
            service.embed_requirement_one(mock_repo, mock_req)

        mock_repo.save_requirement_embedding.assert_not_called()


class TestExtractAndEmbedResume:
    """Test extract_and_embed_resume method (full ETL: extract + embed)."""

    def test_extract_and_embed_resume_file_not_found(self):
        """Test processing resume with missing file."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()

        result = service.extract_and_embed_resume(mock_repo, "nonexistent.json")

        assert result == (False, "", None)

    def test_extract_and_embed_resume_unchanged(self):
        """Test processing unchanged resume."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_existing = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = mock_existing

        with patch("etl.orchestrator.generate_file_fingerprint", return_value="fp123"):
            with patch("os.path.exists", return_value=True):
                with patch("builtins.open", mock_open(read_data=b"{}")):
                    result = service.extract_and_embed_resume(mock_repo, "resume.json")

                    assert result == (False, "fp123", None)

    def test_extract_and_embed_resume_changed(self):
        """Test processing changed resume."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        mock_parsed = Mock()
        mock_parsed.data = {"skills": ["Python"]}
        mock_parsed.text = "Resume text"

        with patch("etl.orchestrator.generate_file_fingerprint", return_value="fp123"):
            with patch("os.path.exists", return_value=True):
                with patch("builtins.open", mock_open(read_data=b"{}")):
                    with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
                        mock_parser = Mock()
                        mock_parser.parse.return_value = mock_parsed
                        mock_parser_class.return_value = mock_parser

                        with patch.object(
                            service,
                            "_extract_resume_data",
                            return_value=(True, "fp123", {"skills": ["Python"]}),
                        ):
                            with patch.object(
                                service, "embed_resume", return_value=(True, "fp123")
                            ):
                                result = service.extract_and_embed_resume(mock_repo, "resume.json")

                                assert result[0] is True
                            assert result[1] == "fp123"
                            assert result[2] is not None

    def test_extract_and_embed_resume_parse_error(self):
        """Test processing resume with parse error."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        with patch("etl.orchestrator.generate_file_fingerprint", return_value="fp123"):
            with patch("os.path.exists", return_value=True):
                with patch("builtins.open", mock_open(read_data=b"{}")):
                    with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
                        mock_parser = Mock()
                        mock_parser.parse.side_effect = ValueError("Parse error")
                        mock_parser_class.return_value = mock_parser

                        result = service.extract_and_embed_resume(mock_repo, "resume.json")

                        assert result == (False, "fp123", None)

    def test_extract_and_embed_resume_file_read_error(self):
        """Test processing resume with file read error."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()

        def raise_io_error(*args, **kwargs):
            raise IOError("Read error")

        with patch("builtins.open", side_effect=raise_io_error):
            with patch("os.path.exists", return_value=True):
                result = service.extract_and_embed_resume(mock_repo, "resume.json")

                assert result == (False, "", None)


class TestExtractResume:
    """Test extract_resume method."""

    def test_extract_resume_file_not_found(self):
        """Test extracting resume with missing file."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()

        result = service.extract_resume(mock_repo, "nonexistent.json")

        assert result == (False, "", None)

    def test_extract_resume_unchanged(self):
        """Test extracting unchanged resume."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_existing = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = mock_existing

        with patch("etl.orchestrator.generate_file_fingerprint", return_value="fp123"):
            with patch("os.path.exists", return_value=True):
                with patch("builtins.open", mock_open(read_data=b"{}")):
                    result = service.extract_resume(mock_repo, "resume.json")

                    assert result == (False, "fp123", None)

    def test_extract_resume_changed(self):
        """Test extracting changed resume."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        mock_parsed = Mock()
        mock_parsed.data = {"skills": ["Python"]}
        mock_parsed.text = "Resume text"

        mock_schema = Mock()
        mock_schema.claimed_total_years = 5
        mock_schema.extraction.confidence = 0.9
        mock_schema.extraction.warnings = []
        mock_schema.model_dump.return_value = {"skills": ["Python"]}

        with patch("etl.orchestrator.generate_file_fingerprint", return_value="fp123"):
            with patch("os.path.exists", return_value=True):
                with patch("builtins.open", mock_open(read_data=b"{}")):
                    with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
                        mock_parser = Mock()
                        mock_parser.parse.return_value = mock_parsed
                        mock_parser_class.return_value = mock_parser

                        with patch("etl.orchestrator.ResumeProfiler") as mock_profiler_class:
                            mock_profiler = Mock()
                            mock_profiler.extract_only.return_value = mock_schema
                            mock_profiler_class.return_value = mock_profiler

                            result = service.extract_resume(mock_repo, "resume.json")

                            assert result[0] is True
                        assert result[1] == "fp123"
                        mock_repo.save_structured_resume.assert_called_once()

    def test_extract_resume_no_schema(self):
        """Test extracting resume with no schema result."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        mock_parsed = Mock()
        mock_parsed.data = {"skills": ["Python"]}

        with patch("etl.orchestrator.generate_file_fingerprint", return_value="fp123"):
            with patch("builtins.open", mock_open(read_data=b"{}")):
                with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
                    mock_parser = Mock()
                    mock_parser.parse.return_value = mock_parsed
                    mock_parser_class.return_value = mock_parser

                    with patch("etl.orchestrator.ResumeProfiler") as mock_profiler_class:
                        mock_profiler = Mock()
                        mock_profiler.extract_only.return_value = None
                        mock_profiler_class.return_value = mock_profiler

                        result = service.extract_resume(mock_repo, "resume.json")

                        assert result[0] is False

    def test_extract_resume_parse_error(self):
        """Test extracting resume with parse error."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        with patch("etl.orchestrator.generate_file_fingerprint", return_value="fp123"):
            with patch("os.path.exists", return_value=True):
                with patch("builtins.open", mock_open(read_data=b"{}")):
                    with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
                        mock_parser = Mock()
                        mock_parser.parse.side_effect = ValueError("Parse error")
                        mock_parser_class.return_value = mock_parser

                        result = service.extract_resume(mock_repo, "resume.json")

                        assert result == (False, "fp123", None)

    def test_extract_resume_file_read_error(self):
        """Test extracting resume with file read error."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()

        def raise_io_error(*args, **kwargs):
            raise IOError("Read error")

        with patch("builtins.open", side_effect=raise_io_error):
            with patch("os.path.exists", return_value=True):
                result = service.extract_resume(mock_repo, "resume.json")

                assert result == (False, "", None)


class TestEmbedResume:
    """Test embed_resume method."""

    def test_embed_resume_success(self):
        """Test successful resume embedding."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_existing = Mock()
        mock_existing.extracted_data = {
            "profile": {
                "contact": {"name": "Test Candidate"},
                "summary": {"text": "Experienced developer", "total_experience_years": 5},
                "experience": [],
                "projects": {"items": []},
                "education": [],
                "skills": {"groups": [], "all": []},
                "certifications": [],
                "languages": [],
            },
            "extraction": {"confidence": 0.9, "warnings": []},
        }
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = mock_existing

        with patch.object(service, "ensure_resume_ready") as mock_ensure_ready:
            result = service.embed_resume(mock_repo, "fp123")

            assert result == (True, "fp123")
            mock_ensure_ready.assert_called_once()

    def test_embed_resume_not_found(self):
        """Test embedding resume not found in DB."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        result = service.embed_resume(mock_repo, "fp123")

        assert result == (False, "fp123")

    def test_embed_resume_no_extracted_data(self):
        """Test embedding resume with no extracted data."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_existing = Mock()
        mock_existing.extracted_data = None
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = mock_existing

        result = service.embed_resume(mock_repo, "fp123")

        assert result == (False, "fp123")

    def test_embed_resume_exception(self):
        """Test embedding resume with exception."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_existing = Mock()
        mock_existing.extracted_data = {
            "profile": {
                "contact": {"name": "Test Candidate"},
                "summary": {"text": "Experienced developer", "total_experience_years": 5},
                "experience": [],
                "projects": {"items": []},
                "education": [],
                "skills": {"groups": [], "all": []},
                "certifications": [],
                "languages": [],
            },
            "extraction": {"confidence": 0.9, "warnings": []},
        }
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = mock_existing

        with patch.object(service, "ensure_resume_ready", side_effect=Exception("Embedding error")):
            with pytest.raises(Exception, match="Embedding error"):
                service.embed_resume(mock_repo, "fp123")


class TestUnloadModels:
    """Test unload_models method."""

    def test_unload_models_supported(self):
        """Test unloading models when provider supports it."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.unload_model = Mock()
        mock_ai.extraction_model = "extraction-model"
        mock_ai.embedding_model = "embedding-model"

        service = JobETLService(mock_ai)
        service.unload_models()

        assert mock_ai.unload_model.call_count == 2

    def test_unload_models_not_supported(self):
        """Test unloading models when provider doesn't support it."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        del mock_ai.unload_model

        service = JobETLService(mock_ai)
        service.unload_models()

        # Should not raise any error


class TestLoadAndCheckResumeAdditional:
    """Additional coverage tests for _load_and_check_resume."""

    def test_known_fingerprint_skips_file_read_when_unchanged(self):
        """When known_fingerprint is provided and DB has a match, returns unchanged."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = Mock()

        changed, fp, data = service._load_and_check_resume(
            mock_repo, "resume.pdf", known_fingerprint="fp-known"
        )

        assert changed is False
        assert fp == "fp-known"
        assert data is None
        # File should never have been opened
        mock_repo.resume.get_structured_resume_by_fingerprint.assert_called_once_with("fp-known")

    def test_known_fingerprint_parses_file_when_changed(self):
        """When known_fingerprint is provided but DB has no match, parses the file."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        mock_parsed = Mock()
        mock_parsed.data = {"name": "Jane"}
        mock_parsed.text = "resume text"

        with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
            mock_parser_class.return_value.parse.return_value = mock_parsed

            changed, fp, data = service._load_and_check_resume(
                mock_repo, "resume.pdf", known_fingerprint="fp-known"
            )

        assert changed is True
        assert fp == "fp-known"
        assert data == {"name": "Jane"}

    def test_parsed_data_none_falls_back_to_raw_text(self):
        """When parser returns data=None, resume_data uses raw_text from parsed.text."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        mock_parsed = Mock()
        mock_parsed.data = None
        mock_parsed.text = "raw resume content"

        with patch("os.path.exists", return_value=True):
            with patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-abc"):
                with patch("builtins.open", mock_open(read_data=b"bytes")):
                    with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
                        mock_parser_class.return_value.parse.return_value = mock_parsed

                        changed, _, data = service._load_and_check_resume(mock_repo, "resume.pdf")

        assert changed is True
        assert data == {"raw_text": "raw resume content"}


class TestExtractAndEmbedResumeAdditional:
    """Additional coverage for extract_and_embed_resume edge cases."""

    def test_extraction_returns_false_returns_failure_tuple(self):
        """When _extract_resume_data returns extracted=False, returns (False, fp, None)."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        with patch.object(
            service,
            "_load_and_check_resume",
            return_value=(True, "fp-123", {"name": "J"}),
        ):
            with patch.object(
                service,
                "_extract_resume_data",
                return_value=(False, "fp-123", None),
            ):
                result = service.extract_and_embed_resume(mock_repo, "resume.pdf")

        assert result == (False, "fp-123", None)

    def test_exception_in_extract_is_reraised(self):
        """An exception raised by _extract_resume_data propagates out."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()

        with patch.object(
            service,
            "_load_and_check_resume",
            return_value=(True, "fp-123", {"name": "J"}),
        ):
            with patch.object(
                service,
                "_extract_resume_data",
                side_effect=RuntimeError("extraction boom"),
            ):
                with pytest.raises(RuntimeError, match="extraction boom"):
                    service.extract_and_embed_resume(mock_repo, "resume.pdf")


class TestExtractResumeDataAdditional:
    """Additional coverage for _extract_resume_data exception path."""

    def test_profiler_exception_is_reraised(self):
        """An exception from ResumeProfiler.extract_only propagates out."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()

        with patch("etl.orchestrator.ResumeProfiler") as mock_profiler_class:
            mock_profiler_class.return_value.extract_only.side_effect = RuntimeError("LLM down")

            with pytest.raises(RuntimeError, match="LLM down"):
                service._extract_resume_data(mock_repo, "fp-123", {"name": "J"})


class TestExtractResumeWithKnownFingerprint:
    """Tests for extract_resume when known_fingerprint is supplied."""

    def test_known_fingerprint_existing_returns_unchanged(self):
        """When known_fingerprint is provided and DB already has it, returns unchanged."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = Mock()

        result = service.extract_resume(mock_repo, "resume.pdf", known_fingerprint="fp-known")

        assert result == (False, "fp-known", None)

    def test_known_fingerprint_not_in_db_parses_and_extracts(self):
        """When known_fingerprint not in DB, parses file and runs extraction."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        mock_parsed = Mock()
        mock_parsed.data = {"skills": ["Python"]}
        mock_parsed.text = "text"

        with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
            mock_parser_class.return_value.parse.return_value = mock_parsed
            with patch.object(
                service,
                "_extract_resume_data",
                return_value=(True, "fp-known", {"skills": ["Python"]}),
            ) as mock_extract:
                result = service.extract_resume(
                    mock_repo, "resume.pdf", known_fingerprint="fp-known"
                )

        mock_extract.assert_called_once()
        assert result == (True, "fp-known", {"skills": ["Python"]})

    def test_known_fingerprint_parse_error_returns_failure(self):
        """When known_fingerprint provided but parse fails, returns (False, fp, None)."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        with patch("etl.orchestrator.ResumeParser") as mock_parser_class:
            mock_parser_class.return_value.parse.side_effect = ValueError("bad file")

            result = service.extract_resume(mock_repo, "resume.pdf", known_fingerprint="fp-known")

        assert result == (False, "fp-known", None)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
