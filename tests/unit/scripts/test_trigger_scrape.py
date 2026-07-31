from unittest.mock import MagicMock, patch

import httpx

from scripts.trigger_scrape import trigger_scrape


def _client_raising(exc: Exception) -> MagicMock:
    client = MagicMock()
    client.__enter__.return_value.post.side_effect = exc
    return client


def test_trigger_scrape_reports_connection_failure(capsys) -> None:
    client = _client_raising(httpx.ConnectError("unavailable"))

    with patch("scripts.trigger_scrape.httpx.Client", return_value=client):
        exit_code = trigger_scrape("http://orchestrator:8084")

    assert exit_code == 1
    output = capsys.readouterr().out
    assert "Could not connect" in output
    assert "Make sure the orchestrator service is running" in output


def test_trigger_scrape_reports_timeout(capsys) -> None:
    client = _client_raising(httpx.TimeoutException("slow"))

    with patch("scripts.trigger_scrape.httpx.Client", return_value=client):
        exit_code = trigger_scrape("http://orchestrator:8084")

    assert exit_code == 1
    assert "Scrape request timed out" in capsys.readouterr().out
