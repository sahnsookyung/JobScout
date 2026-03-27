"""Unit tests for core/scraper/jobspy_client.py"""

import threading
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

import requests

from core.scraper.jobspy_client import JobSpyClient, _is_retryable_error
from core.config_loader import ScraperConfig


# ---------------------------------------------------------------------------
# _is_retryable_error
# ---------------------------------------------------------------------------

class TestIsRetryableError:
    def test_timeout_is_retryable(self):
        assert _is_retryable_error(requests.Timeout()) is True

    def test_http_error_5xx_is_retryable(self):
        exc = requests.HTTPError()
        exc.response = MagicMock()
        exc.response.status_code = 503
        assert _is_retryable_error(exc) is True

    def test_http_error_4xx_is_not_retryable(self):
        exc = requests.HTTPError()
        exc.response = MagicMock()
        exc.response.status_code = 404
        assert _is_retryable_error(exc) is False

    def test_http_error_no_response_is_retryable(self):
        exc = requests.HTTPError()
        exc.response = None
        assert _is_retryable_error(exc) is True

    def test_request_exception_no_response_is_retryable(self):
        exc = requests.RequestException()
        exc.response = None
        assert _is_retryable_error(exc) is True

    def test_request_exception_4xx_response_not_retryable(self):
        exc = requests.RequestException()
        exc.response = MagicMock()
        exc.response.status_code = 400
        assert _is_retryable_error(exc) is False

    def test_request_exception_5xx_response_is_retryable(self):
        exc = requests.RequestException()
        exc.response = MagicMock()
        exc.response.status_code = 500
        assert _is_retryable_error(exc) is True

    def test_unrelated_exception_not_retryable(self):
        assert _is_retryable_error(ValueError("nope")) is False
        assert _is_retryable_error(RuntimeError("boom")) is False


# ---------------------------------------------------------------------------
# JobSpyClient initialization
# ---------------------------------------------------------------------------

class TestJobSpyClientInit:
    def test_default_base_url(self):
        client = JobSpyClient()
        assert client.base_url == "http://localhost:8000"

    def test_custom_base_url(self):
        client = JobSpyClient(base_url="http://custom:9000")
        assert client.base_url == "http://custom:9000"

    def test_default_intervals(self):
        client = JobSpyClient()
        assert client.poll_interval_seconds == 10
        assert client.job_timeout_seconds == 300
        assert client.request_timeout_seconds == 30

    def test_custom_intervals(self):
        client = JobSpyClient(
            poll_interval_seconds=5,
            job_timeout_seconds=120,
            request_timeout_seconds=15,
        )
        assert client.poll_interval_seconds == 5
        assert client.job_timeout_seconds == 120
        assert client.request_timeout_seconds == 15

    def test_session_created(self):
        client = JobSpyClient()
        assert client.session is not None


# ---------------------------------------------------------------------------
# submit_scrape
# ---------------------------------------------------------------------------

class TestSubmitScrape:
    def _make_scraper_config(self, **kwargs):
        return ScraperConfig(
            site_type=["linkedin"],
            search_term="Python engineer",
            **kwargs
        )

    def test_submit_returns_task_id(self):
        client = JobSpyClient(base_url="http://test:8000")
        mock_response = MagicMock()
        mock_response.json.return_value = {"task_id": "task-abc"}
        mock_response.raise_for_status.return_value = None
        client.session = MagicMock()
        client.session.post.return_value = mock_response

        cfg = self._make_scraper_config()
        result = client.submit_scrape(cfg)
        assert result == "task-abc"

    def test_submit_calls_scrape_endpoint(self):
        client = JobSpyClient(base_url="http://test:8000")
        mock_response = MagicMock()
        mock_response.json.return_value = {"task_id": "tid"}
        mock_response.raise_for_status.return_value = None
        client.session = MagicMock()
        client.session.post.return_value = mock_response

        cfg = self._make_scraper_config()
        client.submit_scrape(cfg)

        client.session.post.assert_called_once()
        call_args = client.session.post.call_args
        assert call_args[0][0] == "http://test:8000/scrape"

    def test_submit_raises_on_error_status(self):
        client = JobSpyClient(base_url="http://test:8000")
        mock_response = MagicMock()
        http_err = requests.HTTPError()
        http_err.response = MagicMock()
        http_err.response.status_code = 400
        mock_response.raise_for_status.side_effect = http_err
        client.session = MagicMock()
        client.session.post.return_value = mock_response

        cfg = self._make_scraper_config()
        with pytest.raises(requests.HTTPError):
            client.submit_scrape(cfg)

    def test_submit_returns_none_when_no_task_id(self):
        client = JobSpyClient(base_url="http://test:8000")
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None
        client.session = MagicMock()
        client.session.post.return_value = mock_response

        cfg = self._make_scraper_config()
        result = client.submit_scrape(cfg)
        assert result is None


# ---------------------------------------------------------------------------
# _poll_status
# ---------------------------------------------------------------------------

class TestPollStatus:
    def test_200_returns_json(self):
        client = JobSpyClient()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "pending"}
        client.session = MagicMock()
        client.session.get.return_value = mock_response

        result = client._poll_status.__wrapped__(client, "task-1")
        assert result == {"status": "pending"}

    def test_404_raises_value_error(self):
        client = JobSpyClient()
        mock_response = MagicMock()
        mock_response.status_code = 404
        client.session = MagicMock()
        client.session.get.return_value = mock_response

        with pytest.raises(ValueError, match="not found"):
            client._poll_status.__wrapped__(client, "task-missing")

    def test_5xx_raises_http_error(self):
        client = JobSpyClient()
        mock_response = MagicMock()
        mock_response.status_code = 503
        client.session = MagicMock()
        client.session.get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            client._poll_status.__wrapped__(client, "task-error")

    def test_other_status_returns_none(self):
        client = JobSpyClient()
        mock_response = MagicMock()
        mock_response.status_code = 202
        client.session = MagicMock()
        client.session.get.return_value = mock_response

        result = client._poll_status.__wrapped__(client, "task-pending")
        assert result is None


# ---------------------------------------------------------------------------
# wait_for_result
# ---------------------------------------------------------------------------

class TestWaitForResult:
    def test_returns_data_on_completed(self):
        client = JobSpyClient(poll_interval_seconds=1, job_timeout_seconds=30)
        client._poll_status = MagicMock(return_value={
            "status": "completed",
            "count": 2,
            "data": [{"id": "job-1"}, {"id": "job-2"}],
        })

        with patch("time.sleep"):
            result = client.wait_for_result("task-done")

        assert result == [{"id": "job-1"}, {"id": "job-2"}]

    def test_returns_none_on_failed(self):
        client = JobSpyClient(poll_interval_seconds=1, job_timeout_seconds=30)
        client._poll_status = MagicMock(return_value={
            "status": "failed",
            "error": "Something went wrong",
        })

        with patch("time.sleep"):
            result = client.wait_for_result("task-failed")

        assert result is None

    def test_returns_none_on_timeout(self):
        client = JobSpyClient(poll_interval_seconds=1, job_timeout_seconds=2)
        # Always return pending
        client._poll_status = MagicMock(return_value={"status": "pending"})

        with patch("time.sleep"):
            result = client.wait_for_result(
                "task-timeout",
                poll_interval_s=1,
                job_timeout_s=2,
            )

        assert result is None

    def test_returns_none_when_stop_event_set(self):
        client = JobSpyClient()
        stop_event = threading.Event()
        stop_event.set()

        result = client.wait_for_result("task-cancel", stop_event=stop_event)
        assert result is None

    def test_stop_event_checked_each_iteration(self):
        """Stop event set after first poll cancels the wait."""
        client = JobSpyClient(poll_interval_seconds=0, job_timeout_seconds=60)
        stop_event = threading.Event()

        call_count = 0

        def poll_side_effect(task_id, req_timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                stop_event.set()
            return {"status": "pending"}

        client._poll_status = MagicMock(side_effect=poll_side_effect)

        result = client.wait_for_result("task-cancel", stop_event=stop_event)
        assert result is None

    def test_polling_error_logged_and_continues(self):
        """Exception during _poll_status is logged and loop continues."""
        client = JobSpyClient(poll_interval_seconds=1, job_timeout_seconds=2)
        call_count = 0

        def poll_side_effect(task_id, req_timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("transient error")
            return {"status": "completed", "data": []}

        client._poll_status = MagicMock(side_effect=poll_side_effect)

        with patch("time.sleep"):
            result = client.wait_for_result("task-err")

        assert result == []

    def test_custom_poll_and_timeout_params(self):
        client = JobSpyClient()
        client._poll_status = MagicMock(return_value={"status": "completed", "data": []})

        with patch("time.sleep"):
            result = client.wait_for_result(
                "task-1",
                poll_interval_s=2,
                job_timeout_s=10,
                request_timeout_s=5,
            )

        assert result == []


# ---------------------------------------------------------------------------
# close / context manager
# ---------------------------------------------------------------------------

class TestCloseAndContextManager:
    def test_close_closes_session(self):
        client = JobSpyClient()
        client.session = MagicMock()
        client.close()
        client.session.close.assert_called_once()

    def test_context_manager_closes_on_exit(self):
        with JobSpyClient() as client:
            client.session = MagicMock()
            mock_session = client.session

        mock_session.close.assert_called_once()

    def test_context_manager_returns_self(self):
        with JobSpyClient() as client:
            assert isinstance(client, JobSpyClient)
