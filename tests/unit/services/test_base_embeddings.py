#!/usr/bin/env python3
"""
Unit Tests: Base Embeddings Module

Tests the services/base/embeddings.py module functions.

Usage:
    uv run pytest tests/unit/services/test_base_embeddings.py -v
"""

import pytest
import threading
from unittest.mock import Mock, patch, MagicMock


class TestRunFacetEmbeddingBatch:
    """Test _run_facet_embedding_batch function."""

    def test_facet_embedding_batch_returns_count(self):
        """Test facet embedding batch returns count of processed jobs."""
        from services.base.embeddings import _run_facet_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_job.facet_status = "done"
            mock_repo.get_jobs_needing_facet_embedding.return_value = [mock_job]
            mock_repo.get_by_id.return_value = mock_job
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.embed_facets_one = Mock()
            stop_event = threading.Event()

            result = _run_facet_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 1

    def test_facet_embedding_batch_handles_stop_event(self):
        """Test facet embedding batch respects stop event."""
        from services.base.embeddings import _run_facet_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_job.facet_status = "done"
            mock_repo.get_jobs_needing_facet_embedding.return_value = [mock_job]
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            stop_event = threading.Event()
            stop_event.set()

            result = _run_facet_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0

    def test_facet_embedding_batch_handles_exception(self):
        """Test facet embedding batch handles exceptions gracefully."""
        from services.base.embeddings import _run_facet_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_job.facet_status = "done"
            mock_repo.get_jobs_needing_facet_embedding.return_value = [mock_job]
            mock_repo.get_by_id.return_value = mock_job
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.embed_facets_one.side_effect = Exception("Embedding error")
            stop_event = threading.Event()

            result = _run_facet_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0

    def test_facet_embedding_batch_skips_job_not_done(self):
        """Test facet embedding batch skips jobs not in done status."""
        from services.base.embeddings import _run_facet_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_job.facet_status = "pending"
            mock_repo.get_jobs_needing_facet_embedding.return_value = [mock_job]
            mock_repo.get_by_id.return_value = mock_job
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.embed_facets_one = Mock()
            stop_event = threading.Event()

            result = _run_facet_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0
            mock_ctx.job_etl_service.embed_facets_one.assert_not_called()

    def test_facet_embedding_batch_handles_missing_job(self):
        """Test facet embedding batch handles missing job."""
        from services.base.embeddings import _run_facet_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_repo.get_by_id.return_value = None
            mock_repo.get_jobs_needing_facet_embedding.return_value = []
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.embed_facets_one = Mock()
            stop_event = threading.Event()

            result = _run_facet_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0


class TestRunEmbeddingBatch:
    """Test _run_embedding_batch function."""

    def test_embedding_batch_returns_count(self):
        """Test embedding batch returns count of processed items."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_req = MagicMock()
            mock_req.id = "req-1"
            mock_repo.get_unembedded_jobs.return_value = [mock_job]
            mock_repo.get_unembedded_requirements.return_value = [mock_req]
            mock_repo.get_by_id.return_value = mock_job
            mock_repo.get_requirement_by_id.return_value = mock_req
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.embed_job_one = Mock()
            mock_ctx.job_etl_service.embed_requirement_one = Mock()
            stop_event = threading.Event()

            result = _run_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 2

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

    def test_embedding_batch_handles_job_exception(self):
        """Test embedding batch handles job embedding exceptions."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_job = MagicMock()
            mock_job.id = "job-1"
            mock_repo.get_unembedded_jobs.return_value = [mock_job]
            mock_repo.get_unembedded_requirements.return_value = []
            mock_repo.get_by_id.return_value = mock_job
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = Mock()
            mock_ctx.job_etl_service.embed_job_one.side_effect = Exception("Embedding failed")
            stop_event = threading.Event()

            result = _run_embedding_batch(mock_ctx, stop_event, limit=10)

            assert result == 0

    def test_embedding_batch_handles_missing_job(self):
        """Test embedding batch handles missing job."""
        from services.base.embeddings import _run_embedding_batch
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_repo.get_by_id.return_value = None
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


class TestRunEmbeddingExtraction:
    """Test run_embedding_extraction function."""

    @patch('services.base.embeddings._run_embedding_batch')
    @patch('services.base.embeddings._run_facet_embedding_batch')
    def test_run_embedding_extraction_returns_total(self, mock_facet_batch, mock_embed_batch):
        """Test run_embedding_extraction returns combined count."""
        from services.base.embeddings import run_embedding_extraction

        mock_facet_batch.return_value = 5
        mock_embed_batch.return_value = 10

        mock_ctx = Mock()
        stop_event = threading.Event()

        result = run_embedding_extraction(mock_ctx, stop_event, limit=100)

        assert result == 15


class TestGenerateResumeEmbedding:
    """Test generate_resume_embedding function."""

    def test_generate_resume_embedding_returns_true(self):
        """Test generate_resume_embedding returns True on success."""
        from services.base.embeddings import generate_resume_embedding
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_repo.embed_resume = Mock(return_value=(True, "fingerprint123"))
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = MagicMock()
            mock_ctx.job_etl_service.embed_resume = Mock(return_value=(True, "fingerprint123"))

            result = generate_resume_embedding(mock_ctx, "fingerprint123")

            assert result is True

    def test_generate_resume_embedding_returns_false(self):
        """Test generate_resume_embedding returns False when not embedded."""
        from services.base.embeddings import generate_resume_embedding
        with patch('services.base.embeddings.job_uow') as mock_job_uow:
            mock_repo = MagicMock()
            mock_repo.embed_resume = Mock(return_value=(False, "fingerprint123"))
            
            mock_context = MagicMock()
            mock_context.__enter__ = Mock(return_value=mock_repo)
            mock_context.__exit__ = Mock(return_value=False)
            mock_job_uow.return_value = mock_context

            mock_ctx = MagicMock()
            mock_ctx.job_etl_service.embed_resume = Mock(return_value=(False, "fingerprint123"))

            result = generate_resume_embedding(mock_ctx, "fingerprint123")

            assert result is False
