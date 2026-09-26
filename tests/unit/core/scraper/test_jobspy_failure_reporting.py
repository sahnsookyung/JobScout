"""Scrape failures must remain distinct from a completed, empty search."""

import threading
from unittest.mock import MagicMock

import pytest

from core.scraper.jobspy_client import JobSpyClient, JobSpyTaskError


def test_strict_polling_preserves_terminal_failure_and_does_not_repoll() -> None:
    with JobSpyClient() as client:
        client._poll_status = MagicMock(return_value={
            "status": "failed",
            "error": "scrape exceeded request timeout of 45 seconds",
        })

        with pytest.raises(JobSpyTaskError, match="request timeout of 45 seconds"):
            client.wait_for_result("failed-task", raise_on_failure=True)

        client._poll_status.assert_called_once()


def test_strict_polling_reports_an_unfinished_task_as_failure() -> None:
    with JobSpyClient(job_timeout_seconds=0) as client:
        client._poll_status = MagicMock(return_value={"status": "processing"})

        with pytest.raises(JobSpyTaskError, match="timed out"):
            client.wait_for_result("unfinished-task", raise_on_failure=True)


def test_strict_polling_accepts_successful_empty_results() -> None:
    with JobSpyClient() as client:
        client._poll_status = MagicMock(return_value={"status": "completed", "data": []})

        assert client.wait_for_result("empty-task", raise_on_failure=True) == []


def test_strict_polling_preserves_cooperative_cancellation() -> None:
    stop_event = threading.Event()
    stop_event.set()
    with JobSpyClient() as client:
        client._poll_status = MagicMock()

        assert client.wait_for_result(
            "cancelled-task", stop_event=stop_event, raise_on_failure=True,
        ) is None
        client._poll_status.assert_not_called()
