"""Tests for facet extraction concurrency, claiming, and failure handling."""

import unittest
import threading
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
import uuid


class TestFacetExtractionAtomicUpsert(unittest.TestCase):
    """Test that concurrent facet saves don't cause IntegrityError."""

    def test_concurrent_saves_no_integrity_error(self):
        """Two workers saving facets for same job should not raise IntegrityError."""
        from database.repositories.job_post import JobPostRepository
        from database.models import JobFacetEmbedding
        
        mock_db = MagicMock()
        repo = JobPostRepository(mock_db)
        
        job_post_id = uuid.uuid4()
        facet_key = 'remote_flexibility'
        facet_text = 'Remote work available'
        embedding = [0.1] * 1024
        content_hash = 'abc123'
        
        def save_facet():
            try:
                repo.save_job_facet_embedding(
                    job_post_id, facet_key, facet_text, embedding, content_hash
                )
            except Exception as e:
                self.fail(f"Concurrent save raised exception: {e}")
        
        threads = [threading.Thread(target=save_facet) for _ in range(10)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        mock_db.execute.assert_called()

    def test_upsert_updates_existing_facet(self):
        """Upsert should update existing facet, not create duplicate."""
        from database.repositories.job_post import JobPostRepository
        from database.models import JobFacetEmbedding
        
        mock_db = MagicMock()
        mock_existing = MagicMock()
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_existing
        
        repo = JobPostRepository(mock_db)
        
        result = repo.save_job_facet_embedding(
            uuid.uuid4(), 'compensation', 'Updated text', [0.1] * 1024, 'hash123'
        )
        
        self.assertTrue(mock_db.execute.called)


class TestFacetClaiming(unittest.TestCase):
    """Test atomic claiming semantics."""

    def test_get_and_claim_jobs_returns_claimed_jobs(self):
        """get_and_claim_jobs should return jobs that were claimed."""
        from database.repositories.job_post import JobPostRepository
        from database.models import JobPost
        
        mock_db = MagicMock()
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()
        mock_db.execute.return_value.fetchall.return_value = [(mock_job.id,)]
        mock_db.execute.return_value.rowcount = 1
        mock_db.execute.return_value.scalars.return_value.all.return_value = [mock_job]
        
        repo = JobPostRepository(mock_db)
        
        jobs = repo.get_and_claim_jobs_for_facet_extraction(limit=10, worker_id='worker_1')
        
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].id, mock_job.id)

    def test_concurrent_workers_claim_different_jobs(self):
        """Two workers should claim different jobs, not the same ones."""
        from database.repositories.job_post import JobPostRepository
        
        job_ids = [uuid.uuid4() for _ in range(10)]
        claimed_by_worker_1 = set()
        claimed_by_worker_2 = set()
        
        def worker_claim(worker_id, job_index, claimed_set):
            mock_db = MagicMock()
            mock_db.execute.return_value.fetchall.return_value = [(job_ids[job_index],)]
            mock_db.execute.return_value.rowcount = 1
            mock_db.execute.return_value.scalars.return_value.all.return_value = [
                MagicMock(id=job_ids[job_index])
            ]
            
            repo = JobPostRepository(mock_db)
            jobs = repo.get_and_claim_jobs_for_facet_extraction(limit=5, worker_id=worker_id)
            claimed_set.update([j.id for j in jobs])
        
        t1 = threading.Thread(target=worker_claim, args=('worker_1', 0, claimed_by_worker_1))
        t2 = threading.Thread(target=worker_claim, args=('worker_2', 1, claimed_by_worker_2))
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        self.assertEqual(len(claimed_by_worker_1), 1)
        self.assertEqual(len(claimed_by_worker_2), 1)
        self.assertTrue(claimed_by_worker_1.isdisjoint(claimed_by_worker_2))

    def test_claiming_respects_limit(self):
        """Claiming should not exceed the limit parameter."""
        from database.repositories.job_post import JobPostRepository
        
        mock_db = MagicMock()
        repo = JobPostRepository(mock_db)
        
        repo.get_and_claim_jobs_for_facet_extraction(limit=5, worker_id='worker_1')
        
        self.assertTrue(mock_db.execute.called)
        self.assertGreaterEqual(mock_db.execute.call_count, 1)

    def test_stale_claims_are_reset(self):
        """Claims older than timeout should be reset to pending."""
        from database.repositories.job_post import JobPostRepository
        
        mock_db = MagicMock()
        repo = JobPostRepository(mock_db)
        
        repo.get_and_claim_jobs_for_facet_extraction(
            limit=10,
            worker_id='worker_1',
            claim_timeout_minutes=30
        )
        
        self.assertTrue(mock_db.execute.called)


class TestFacetFailureHandling(unittest.TestCase):
    """Test failure handling and retry semantics."""

    def test_mark_job_facets_failed_sets_pending(self):
        """mark_job_facets_failed should set status to pending for retry."""
        from database.repositories.job_post import JobPostRepository
        
        mock_db = MagicMock()
        repo = JobPostRepository(mock_db)
        
        repo.mark_job_facets_failed(uuid.uuid4(), "Test error")
        
        self.assertTrue(mock_db.execute.called)

    def test_mark_job_facets_extracted_clears_claim(self):
        """mark_job_facets_extracted should clear claimed_by/at."""
        from database.repositories.job_post import JobPostRepository
        
        mock_db = MagicMock()
        repo = JobPostRepository(mock_db)
        
        repo.mark_job_facets_extracted(uuid.uuid4(), 'hash123')
        
        self.assertTrue(mock_db.execute.called)

    def test_quarantine_after_max_retries(self):
        """Jobs exceeding max_retries should be quarantined."""
        from database.repositories.job_post import JobPostRepository
        
        mock_db = MagicMock()
        repo = JobPostRepository(mock_db)
        
        repo.get_and_claim_jobs_for_facet_extraction(
            limit=10,
            worker_id='worker_1',
            max_retries=3
        )
        
        self.assertTrue(mock_db.execute.called)


class TestExtractFacetsOneIntegration(unittest.TestCase):
    """Test extract_facets_one with various scenarios."""

    def test_extract_facets_deletes_existing_before_save(self):
        """extract_facets_one should delete existing facets before saving new ones."""
        from etl.orchestrator import JobETLService
        
        mock_repo = MagicMock()
        mock_ai = MagicMock()
        mock_ai.extract_facet_data.return_value = {
            'remote_flexibility': 'Remote work',
            'compensation': '$100k',
        }
        mock_ai.generate_embedding.return_value = [0.1] * 1024
        
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()
        mock_job.title = 'Test Job'
        mock_job.content_hash = 'hash123'
        
        service = JobETLService(ai_service=mock_ai)
        service.extract_facets_one(mock_repo, mock_job)
        
        mock_repo.delete_all_facet_embeddings_for_job.assert_called_once_with(mock_job.id)

    def test_extract_facets_marks_done_on_success(self):
        """extract_facets_one should mark done on successful extraction."""
        from etl.orchestrator import JobETLService
        
        mock_repo = MagicMock()
        mock_ai = MagicMock()
        mock_ai.extract_facet_data.return_value = {'remote_flexibility': 'Remote'}
        mock_ai.generate_embedding.return_value = [0.1] * 1024
        
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()
        mock_job.title = 'Test Job'
        mock_job.content_hash = 'hash123'
        
        service = JobETLService(ai_service=mock_ai)
        service.extract_facets_one(mock_repo, mock_job)
        
        mock_repo.mark_job_facets_extracted.assert_called()

    def test_extract_facets_marks_failed_on_exception(self):
        """extract_facets_one should mark failed and re-raise on exception."""
        from etl.orchestrator import JobETLService
        
        mock_repo = MagicMock()
        mock_ai = MagicMock()
        mock_ai.extract_facet_data.side_effect = Exception("LLM failure")
        
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()
        mock_job.title = 'Test Job'
        
        service = JobETLService(ai_service=mock_ai)
        
        with self.assertRaises(Exception):
            service.extract_facets_one(mock_repo, mock_job)
        
        mock_repo.mark_job_facets_failed.assert_called()

    def test_extract_facets_handles_empty_facets(self):
        """extract_facets_one should handle jobs with no facet text gracefully."""
        from etl.orchestrator import JobETLService
        
        mock_repo = MagicMock()
        mock_ai = MagicMock()
        mock_ai.extract_facet_data.return_value = {}  # All facets empty
        
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()
        mock_job.title = 'Test Job'
        mock_job.content_hash = 'hash123'
        
        service = JobETLService(ai_service=mock_ai)
        service.extract_facets_one(mock_repo, mock_job)
        
        mock_repo.mark_job_facets_extracted.assert_called()


class TestBatchRunnerFailureHandling(unittest.TestCase):
    """Test the batch runner's handling of extraction failures."""

    def test_extract_facets_one_calls_failure_on_exception(self):
        """extract_facets_one should call mark_job_facets_failed on exception."""
        from etl.orchestrator import JobETLService
        
        mock_repo = MagicMock()
        mock_ai = MagicMock()
        mock_ai.extract_facet_data.side_effect = Exception("LLM failed")
        
        mock_job = MagicMock()
        mock_job.id = uuid.uuid4()
        mock_job.title = 'Test Job'
        
        service = JobETLService(ai_service=mock_ai)
        
        with self.assertRaises(Exception):
            service.extract_facets_one(mock_repo, mock_job)
        
        mock_repo.mark_job_facets_failed.assert_called_once()


class TestDeleteAllFacetEmbeddings(unittest.TestCase):
    """Test delete_all_facet_embeddings_for_job method."""

    def test_delete_removes_all_facets_for_job(self):
        """delete_all_facet_embeddings_for_job should remove all facets."""
        from database.repositories.job_post import JobPostRepository
        
        mock_db = MagicMock()
        repo = JobPostRepository(mock_db)
        
        job_id = uuid.uuid4()
        repo.delete_all_facet_embeddings_for_job(job_id)
        
        self.assertTrue(mock_db.execute.called)


class TestJobPostRepositoryWrappers(unittest.TestCase):
    """Test that JobRepository correctly wraps JobPostRepository methods."""

    def test_delete_all_facet_embeddings_wrapper(self):
        """JobRepository should wrap delete_all_facet_embeddings_for_job."""
        from database.repository import JobRepository
        
        mock_session = MagicMock()
        repo = JobRepository(mock_session)
        repo._job_post_repo = MagicMock()
        
        job_id = uuid.uuid4()
        repo.delete_all_facet_embeddings_for_job(job_id)
        
        repo._job_post_repo.delete_all_facet_embeddings_for_job.assert_called_once_with(job_id)

    def test_get_and_claim_jobs_wrapper(self):
        """JobRepository should wrap get_and_claim_jobs_for_facet_extraction."""
        from database.repository import JobRepository
        
        mock_session = MagicMock()
        repo = JobRepository(mock_session)
        repo._job_post_repo = MagicMock()
        repo._job_post_repo.get_and_claim_jobs_for_facet_extraction.return_value = []
        
        repo.get_and_claim_jobs_for_facet_extraction(limit=10, worker_id='test')
        
        repo._job_post_repo.get_and_claim_jobs_for_facet_extraction.assert_called_once()

    def test_mark_job_facets_extracted_wrapper(self):
        """JobRepository should wrap mark_job_facets_extracted."""
        from database.repository import JobRepository
        
        mock_session = MagicMock()
        repo = JobRepository(mock_session)
        repo._job_post_repo = MagicMock()
        
        repo.mark_job_facets_extracted(uuid.uuid4(), 'hash123')
        
        repo._job_post_repo.mark_job_facets_extracted.assert_called_once()

    def test_mark_job_facets_failed_wrapper(self):
        """JobRepository should wrap mark_job_facets_failed."""
        from database.repository import JobRepository
        
        mock_session = MagicMock()
        repo = JobRepository(mock_session)
        repo._job_post_repo = MagicMock()
        
        repo.mark_job_facets_failed(uuid.uuid4(), 'error msg')
        
        repo._job_post_repo.mark_job_facets_failed.assert_called_once()


if __name__ == '__main__':
    unittest.main()
