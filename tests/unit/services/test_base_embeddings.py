#!/usr/bin/env python3
"""
Unit Tests: Base Embeddings Module

Tests the services/base/embeddings.py module functions.

Usage:
    uv run pytest tests/unit/services/test_base_embeddings.py -v
"""

import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch, MagicMock


class TestRunEmbeddingBatch:
    """Test _run_embedding_batch function."""

    def test_build_job_embedding_text_uses_card_metadata_without_description(self):
        """Imported jobs without descriptions still get useful initial vectors."""
        from services.base.embeddings import _build_job_embedding_text

        job = SimpleNamespace(
            title="Frontend Engineer",
            company="Acme",
            location_text="Tokyo, Japan",
            is_remote=True,
            job_type=None,
            job_level="Junior/Mid",
            experience_range=None,
            skills_raw="TypeScript, React",
            canonical_job_summary=None,
            requirements=[],
            benefits=[],
            description=None,
            company_description=None,
            raw_payload={"tags": ["Vue.js", "Elixir"], "summary": "Build web UI"},
        )

        text = _build_job_embedding_text(job)

        assert "Frontend Engineer" in text
        assert "Acme" in text
        assert "Tokyo, Japan" in text
        assert "TypeScript, React" in text
        assert "Vue.js" in text
        assert "Build web UI" in text

    def test_embedding_batch_returns_count(self):
        """Test embedding batch returns count of processed items using batch API."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_job.requirements = []
            mock_job.benefits = []
            mock_job.description = "test job description"
            mock_req = MagicMock()
            mock_req.id = "req-1"
            mock_req.text = "Python experience required"
            mock_repo.get_unembedded_jobs.return_value = [mock_job]
            mock_repo.get_unembedded_requirements.return_value = [mock_req]
            mock_repo.get_by_id.return_value = mock_job

            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            # generate_embeddings_batch called twice: once for jobs, once for reqs
            mock_ctx.job_etl_service.ai.generate_embeddings_batch.side_effect = [
                [[0.1] * 1024],  # job vectors
                [[0.2] * 1024],  # requirement vectors
            ]
            stop_event = threading.Event()

            result = _run_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 2
            assert mock_ctx.job_etl_service.ai.generate_embeddings_batch.call_count == 2

    def test_embedding_batch_handles_stop_event(self):
        """Test embedding batch respects stop event."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_repo.get_unembedded_jobs.return_value = [mock_job]
            mock_repo.get_unembedded_requirements.return_value = []

            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            stop_event = threading.Event()
            stop_event.set()

            result = _run_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0
            # Batch API should not be called when stop event is set
            mock_ctx.job_etl_service.ai.generate_embeddings_batch.assert_not_called()

    def test_embedding_batch_handles_job_api_exception(self):
        """Test embedding batch handles batch API failure by marking jobs retryable."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_job.requirements = []
            mock_job.benefits = []
            mock_job.description = "test description"
            mock_repo.get_unembedded_jobs.return_value = [mock_job]
            mock_repo.get_unembedded_requirements.return_value = []

            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.ai.generate_embeddings_batch.side_effect = Exception("API failed")
            stop_event = threading.Event()

            result = _run_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0
            assert mock_repo.mark_embedding_retryable_failed.called

    def test_embedding_batch_handles_missing_job_on_writeback(self):
        """Test embedding batch skips write-back when job is not found."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_job.requirements = []
            mock_job.benefits = []
            mock_job.description = "test"
            mock_repo.get_unembedded_jobs.return_value = [mock_job]
            mock_repo.get_unembedded_requirements.return_value = []
            mock_repo.get_by_id.return_value = None  # not found on write-back

            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.ai.generate_embeddings_batch.return_value = [[0.1] * 1024]
            stop_event = threading.Event()

            result = _run_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0


    def test_embedding_batch_requirements_api_failure_falls_back_to_per_item(self):
        """When batch requirement embedding fails, fall back to per-item embedding."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_req = MagicMock()
            mock_req.id = "req-1"
            mock_req.text = "Python required"
            mock_repo.get_unembedded_jobs.return_value = []
            mock_repo.get_unembedded_requirements.return_value = [mock_req]
            mock_repo.get_requirement_by_id.return_value = mock_req

            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.ai.generate_embeddings_batch.side_effect = Exception("Batch API failed")
            mock_ctx.job_etl_service.embed_requirement_one = Mock()
            stop_event = threading.Event()

            _run_embedding_batch(mock_ctx, stop_event, limit=10)

            # Per-item fallback should have been called
            mock_ctx.job_etl_service.embed_requirement_one.assert_called_once()

    def test_embedding_batch_job_writeback_exception_marks_failed(self):
        """When write-back for a job fails, it should be marked retryable."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_job.requirements = []
            mock_job.benefits = []
            mock_job.description = "desc"
            mock_repo.get_unembedded_jobs.return_value = [mock_job]
            mock_repo.get_unembedded_requirements.return_value = []
            mock_repo.get_by_id.return_value = mock_job
            mock_repo.save_job_embedding.side_effect = Exception("Write failed")

            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.ai.generate_embeddings_batch.return_value = [[0.1] * 1024]
            stop_event = threading.Event()

            result = _run_embedding_batch(mock_ctx, stop_event, limit=10)

            # Job failed to write back, should be marked retryable
            assert result == 0

    def test_embedding_batch_no_jobs_and_no_requirements(self):
        """When there are no jobs or requirements, returns 0 without API calls."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_repo.get_unembedded_jobs.return_value = []
            mock_repo.get_unembedded_requirements.return_value = []

            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            stop_event = threading.Event()

            result = _run_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0
            mock_ctx.job_etl_service.ai.generate_embeddings_batch.assert_not_called()


class TestRunEmbeddingExtraction:
    """Test run_embedding_extraction function."""

    @patch('services.base.embeddings._run_embedding_batch')
    def test_run_embedding_extraction_returns_total(self, mock_embed_batch):
        """Test run_embedding_extraction returns embedding batch count."""
        from services.base.embeddings import run_embedding_extraction

        mock_embed_batch.return_value = 10

        mock_ctx = Mock()
        stop_event = threading.Event()

        result = run_embedding_extraction(mock_ctx, stop_event, limit=100)

        assert result == 10


class TestGenerateResumeEmbedding:
    """Test generate_resume_embedding function."""

    def test_generate_resume_embedding_returns_true(self):
        """Test generate_resume_embedding returns True on success."""
        from services.base.embeddings import generate_resume_embedding
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = MagicMock()
            mock_ctx.job_etl_service.embed_resume_stage = Mock(return_value=(True, "fingerprint123"))

            result = generate_resume_embedding(mock_ctx, "fingerprint123")

            assert result is True
            mock_ctx.job_etl_service.embed_resume_stage.assert_called_once()

    def test_generate_resume_embedding_returns_false(self):
        """Test generate_resume_embedding returns False when not embedded."""
        from services.base.embeddings import generate_resume_embedding
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = MagicMock()
            mock_ctx.job_etl_service.embed_resume_stage = Mock(return_value=(False, "fingerprint123"))

            result = generate_resume_embedding(mock_ctx, "fingerprint123")

            assert result is False
            mock_ctx.job_etl_service.embed_resume_stage.assert_called_once()
