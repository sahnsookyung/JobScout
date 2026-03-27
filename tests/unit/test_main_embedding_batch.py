#!/usr/bin/env python3
"""Unit tests for embedding batch helpers in main.py."""

import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import main


def _mock_uow(repo):
    context_manager = MagicMock()
    context_manager.__enter__.return_value = repo
    context_manager.__exit__.return_value = False
    return context_manager


class TestMainEmbeddingBatch(unittest.TestCase):
    """Tests for embedding batch helper functions."""

    def test_collect_unembedded_job_ids_reads_ids_from_repo(self):
        repo = MagicMock()
        repo.get_unembedded_jobs.return_value = [
            SimpleNamespace(id="job-1"),
            SimpleNamespace(id="job-2"),
        ]

        with patch("main.job_uow", return_value=_mock_uow(repo)):
            job_ids = main._collect_unembedded_job_ids(5)

        self.assertEqual(job_ids, ["job-1", "job-2"])
        repo.get_unembedded_jobs.assert_called_once_with(5)

    def test_embed_pending_jobs_skips_missing_jobs(self):
        ctx = SimpleNamespace(job_etl_service=MagicMock())
        existing_job = SimpleNamespace(id="job-1")
        present_repo = MagicMock()
        present_repo.get_by_id.return_value = existing_job
        missing_repo = MagicMock()
        missing_repo.get_by_id.return_value = None

        with patch(
            "main.job_uow",
            side_effect=[_mock_uow(present_repo), _mock_uow(missing_repo)],
        ):
            processed = main._embed_pending_jobs(
                ctx,
                threading.Event(),
                ["job-1", "job-missing"],
            )

        self.assertEqual(processed, 1)
        ctx.job_etl_service.embed_job_one.assert_called_once_with(present_repo, existing_job)
        present_repo.get_by_id.assert_called_once_with("job-1")
        missing_repo.get_by_id.assert_called_once_with("job-missing")

    def test_run_embedding_batch_uses_collection_and_processing_helpers(self):
        ctx = SimpleNamespace(job_etl_service=MagicMock())
        stop_event = threading.Event()

        with patch("main._collect_unembedded_job_ids", return_value=["job-1"]) as collect_jobs:
            with patch(
                "main._collect_unembedded_requirement_ids",
                return_value=["req-1", "req-2"],
            ) as collect_requirements:
                with patch("main._embed_pending_jobs", return_value=1) as embed_jobs:
                    with patch("main._embed_pending_requirements", return_value=2) as embed_requirements:
                        main._run_embedding_batch(ctx, stop_event, limit=25)

        collect_jobs.assert_called_once_with(25)
        collect_requirements.assert_called_once_with(25)
        embed_jobs.assert_called_once_with(ctx, stop_event, ["job-1"])
        embed_requirements.assert_called_once_with(ctx, stop_event, ["req-1", "req-2"])


if __name__ == "__main__":
    unittest.main()
