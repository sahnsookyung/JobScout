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
import os
import json
from unittest.mock import Mock, MagicMock, patch, mock_open
from pathlib import Path


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
        mock_repo.get_by_fingerprint.return_value = None

        job_data = {
            'title': 'Software Engineer',
            'company_name': 'Tech Corp',
            'location': 'San Francisco, CA',
            'description': 'Job description here'
        }

        mock_job_post = Mock()
        mock_job_post.id = "job-123"
        mock_repo.create_job_post.return_value = mock_job_post

        service.ingest_one(mock_repo, job_data, 'linkedin')

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
        mock_job_post = Mock()
        mock_job_post.id = "job-456"
        mock_repo.get_by_fingerprint.return_value = mock_job_post

        job_data = {
            'title': 'Software Engineer',
            'company_name': 'Tech Corp',
            'location': 'San Francisco, CA'
        }

        service.ingest_one(mock_repo, job_data, 'indeed')

        mock_repo.update_timestamp.assert_called_once_with(mock_job_post)
        mock_repo.create_job_post.assert_not_called()

    def test_ingest_missing_title(self):
        """Test ingesting job with missing title."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()

        job_data = {
            'company_name': 'Tech Corp',
            'location': 'San Francisco, CA'
        }

        service.ingest_one(mock_repo, job_data, 'linkedin')

        mock_repo.get_by_fingerprint.assert_not_called()
        mock_repo.create_job_post.assert_not_called()

    def test_ingest_missing_company(self):
        """Test ingesting job with missing company."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()

        job_data = {
            'title': 'Software Engineer',
            'location': 'San Francisco, CA'
        }

        service.ingest_one(mock_repo, job_data, 'linkedin')

        mock_repo.get_by_fingerprint.assert_not_called()
        mock_repo.create_job_post.assert_not_called()


class TestExtractOne:
    """Test extract_one method."""

    def test_extract_success(self):
        """Test successful extraction."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.extract_requirements_data.return_value = {
            'requirements': [
                {'text': '5 years Python experience', 'category': 'skill'},
                {'text': 'BS in Computer Science', 'category': 'education'}
            ],
            'benefits': [
                {'text': 'Health insurance', 'type': 'medical'}
            ]
        }

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.title = "Software Engineer"
        mock_job.description = "Job description"

        service.extract_one(mock_repo, mock_job)

        mock_ai.extract_requirements_data.assert_called_once_with(mock_job.description)
        mock_repo.update_job_metadata.assert_called_once()
        mock_repo.save_requirements.assert_called_once()
        mock_repo.save_benefits.assert_called_once()
        mock_repo.mark_as_extracted.assert_called_once()

    def test_extract_empty_requirements(self):
        """Test extraction with empty requirements raises error."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.extract_requirements_data.return_value = {
            'requirements': [],
            'benefits': []
        }

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"

        with pytest.raises(ValueError, match="Empty requirements"):
            service.extract_one(mock_repo, mock_job)

    def test_extract_validation_error_uses_raw_data(self):
        """Test extraction handles validation errors gracefully."""
        from etl.orchestrator import JobETLService
        from pydantic import ValidationError

        mock_ai = Mock()
        mock_ai.extract_requirements_data.return_value = {
            'requirements': [{'text': 'Test requirement'}],
            'benefits': []
        }

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()

        with patch('etl.orchestrator.JobExtraction') as mock_model:
            mock_model.model_validate.side_effect = ValidationError.from_exception_data(
                "JobExtraction", []
            )

            service.extract_one(mock_repo, mock_job)

            mock_repo.update_job_metadata.assert_called_once()

    def test_extract_empty_result(self):
        """Test extraction with empty result."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        # Return empty requirements to trigger the ValueError
        mock_ai.extract_requirements_data.return_value = {
            'requirements': [],
            'benefits': []
        }

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"

        with pytest.raises(ValueError, match="Empty requirements"):
            service.extract_one(mock_repo, mock_job)


class TestExtractFacetsOne:
    """Test extract_facets_one method."""

    def test_extract_facets_success(self):
        """Test successful facet extraction."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        # Use actual FACET_KEYS from core.scorer.want_score
        mock_ai.extract_facet_data.return_value = {
            'remote_flexibility': 'Remote work available',
            'compensation': 'Competitive salary',
            'learning_growth': 'Learning budget',
            'company_culture': 'Great culture',
            'work_life_balance': 'Flexible hours',
            'tech_stack': 'Python, AWS',
            'visa_sponsorship': 'Visa sponsorship available',
        }

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.title = "Software Engineer"
        mock_job.description = "Job description"
        mock_job.content_hash = "hash123"

        service.extract_facets_one(mock_repo, mock_job)

        mock_ai.extract_facet_data.assert_called_once()
        # save_job_facet_embedding is called for each non-empty facet (7 facets)
        assert mock_repo.save_job_facet_embedding.call_count == 7
        mock_repo.mark_job_facets_extracted.assert_called_once()

    def test_extract_facets_empty_facets(self):
        """Test facet extraction with empty facets."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.extract_facet_data.return_value = {}

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.content_hash = "hash123"

        mock_repo.get_facets_for_job.return_value = []

        service.extract_facets_one(mock_repo, mock_job)

        mock_repo.save_job_facet_embedding.assert_not_called()

    def test_extract_facets_exception_handling(self):
        """Test facet extraction handles exceptions."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.extract_facet_data.side_effect = Exception("AI error")

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"

        with pytest.raises(Exception, match="AI error"):
            service.extract_facets_one(mock_repo, mock_job)

        mock_repo.mark_job_facets_failed.assert_called_once()


class TestEmbedFacetsOne:
    """Test embed_facets_one method."""

    def test_embed_facets_success(self):
        """Test successful facet embedding."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, 0.2, 0.3]

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job.content_hash = "hash123"

        mock_facet1 = Mock()
        mock_facet1.id = "facet-1"
        mock_facet1.text = "Python skills"
        mock_facet1.embedding = None

        mock_facet2 = Mock()
        mock_facet2.id = "facet-2"
        mock_facet2.text = "SQL skills"
        mock_facet2.embedding = [0.5, 0.6]

        mock_repo.get_facets_for_job.return_value = [mock_facet1, mock_facet2]

        service.embed_facets_one(mock_repo, mock_job)

        assert mock_ai.generate_embedding.call_count == 1
        mock_repo.update_facet_embedding.assert_called_once()

    def test_embed_facets_no_facets(self):
        """Test embedding with no facets."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"

        mock_repo.get_facets_for_job.return_value = []

        service.embed_facets_one(mock_repo, mock_job)

        mock_ai.generate_embedding.assert_not_called()

    def test_embed_facets_exception_handling(self):
        """Test facet embedding handles exceptions."""
        from etl.orchestrator import JobETLService

        mock_ai = Mock()
        mock_ai.generate_embedding.side_effect = Exception("Embedding error")

        service = JobETLService(mock_ai)

        mock_repo = Mock()
        mock_job = Mock()
        mock_job.id = "job-123"

        mock_facet = Mock()
        mock_facet.embedding = None
        mock_repo.get_facets_for_job.return_value = [mock_facet]

        with pytest.raises(Exception, match="Embedding error"):
            service.embed_facets_one(mock_repo, mock_job)


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
        mock_job.requirements = []
        mock_job.benefits = []
        mock_job.description = ""

        service.embed_job_one(mock_repo, mock_job)

        mock_ai.generate_embedding.assert_called_once()


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

        with patch('etl.orchestrator.generate_file_fingerprint', return_value="fp123"):
            with patch('builtins.open', mock_open(read_data=b'{}')):
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

        with patch('etl.orchestrator.generate_file_fingerprint', return_value="fp123"):
            with patch('builtins.open', mock_open(read_data=b'{}')):
                with patch('etl.orchestrator.ResumeParser') as mock_parser_class:
                    mock_parser = Mock()
                    mock_parser.parse.return_value = mock_parsed
                    mock_parser_class.return_value = mock_parser

                    with patch.object(service, '_extract_resume_data', return_value=(True, "fp123", {"skills": ["Python"]})):
                        with patch.object(service, 'embed_resume', return_value=(True, "fp123")):
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

        with patch('etl.orchestrator.generate_file_fingerprint', return_value="fp123"):
            with patch('builtins.open', mock_open(read_data=b'{}')):
                with patch('etl.orchestrator.ResumeParser') as mock_parser_class:
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

        with patch('builtins.open', side_effect=raise_io_error):
            with patch('os.path.exists', return_value=True):
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

        with patch('etl.orchestrator.generate_file_fingerprint', return_value="fp123"):
            with patch('builtins.open', mock_open(read_data=b'{}')):
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

        with patch('etl.orchestrator.generate_file_fingerprint', return_value="fp123"):
            with patch('builtins.open', mock_open(read_data=b'{}')):
                with patch('etl.orchestrator.ResumeParser') as mock_parser_class:
                    mock_parser = Mock()
                    mock_parser.parse.return_value = mock_parsed
                    mock_parser_class.return_value = mock_parser

                    with patch('etl.orchestrator.ResumeProfiler') as mock_profiler_class:
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

        with patch('etl.orchestrator.generate_file_fingerprint', return_value="fp123"):
            with patch('builtins.open', mock_open(read_data=b'{}')):
                with patch('etl.orchestrator.ResumeParser') as mock_parser_class:
                    mock_parser = Mock()
                    mock_parser.parse.return_value = mock_parsed
                    mock_parser_class.return_value = mock_parser

                    with patch('etl.orchestrator.ResumeProfiler') as mock_profiler_class:
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

        with patch('etl.orchestrator.generate_file_fingerprint', return_value="fp123"):
            with patch('builtins.open', mock_open(read_data=b'{}')):
                with patch('etl.orchestrator.ResumeParser') as mock_parser_class:
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

        with patch('builtins.open', side_effect=raise_io_error):
            with patch('os.path.exists', return_value=True):
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
                "summary": {"text": "Experienced developer", "total_experience_years": 5},
                "experience": [],
                "projects": {"items": []},
                "education": [],
                "skills": {"groups": [], "all": []},
                "certifications": [],
                "languages": []
            },
            "extraction": {"confidence": 0.9, "warnings": []}
        }
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = mock_existing

        with patch('etl.orchestrator.ResumeProfiler') as mock_profiler_class:
            mock_profiler = Mock()
            mock_profiler.embed_only.return_value = None
            mock_profiler_class.return_value = mock_profiler

            result = service.embed_resume(mock_repo, "fp123")

            assert result == (True, "fp123")

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
                "summary": {"text": "Experienced developer", "total_experience_years": 5},
                "experience": [],
                "projects": {"items": []},
                "education": [],
                "skills": {"groups": [], "all": []},
                "certifications": [],
                "languages": []
            },
            "extraction": {"confidence": 0.9, "warnings": []}
        }
        mock_repo.resume.get_structured_resume_by_fingerprint.return_value = mock_existing

        # Mock ResumeProfiler to raise exception during embed_only
        with patch('etl.orchestrator.ResumeProfiler') as mock_profiler_class:
            mock_profiler = Mock()
            mock_profiler.embed_only.side_effect = Exception("Embedding error")
            mock_profiler_class.return_value = mock_profiler

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
