from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from core.config_loader import ScraperConfig, load_config
from core.scraper.jobspy_client import JobSpyClient


def test_scraper_config_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        ScraperConfig(site_type=["tokyodev"], unexpected_option=True)


def test_jobspy_client_reports_missing_url_without_using_localhost() -> None:
    client = JobSpyClient(base_url=None)
    try:
        health = client.check_health()
        assert health["status"] == "not_configured"
        with pytest.raises(RuntimeError, match="not configured"):
            client.submit_scrape(ScraperConfig(site_type=["tokyodev"]))
    finally:
        client.close()


def test_jobspy_client_authenticates_and_omits_scheduler_fields() -> None:
    client = JobSpyClient(
        base_url="http://jobspy:8000",
        api_token="internal-token",
    )
    response = Mock()
    response.json.return_value = {"task_id": "task-1"}
    response.raise_for_status.return_value = None
    scraper = ScraperConfig(
        site_type=["tokyodev"],
        enabled=True,
        fetch_mode="jobspy_api",
        request_timeout=45,
    )
    try:
        with patch.object(client.session, "post", return_value=response) as post:
            assert client.submit_scrape(scraper) == "task-1"

        assert client.session.headers["X-JobSpy-Token"] == "internal-token"
        payload = post.call_args.kwargs["json"]
        assert payload["request_timeout"] == 45
        assert "enabled" not in payload
        assert "fetch_mode" not in payload
    finally:
        client.close()


def test_default_scraper_rollout_is_allowlisted_and_scheduled_every_12_hours() -> None:
    config = load_config()

    enabled = [source for source in config.scrapers if source.enabled]
    assert [source.site_type for source in enabled] == [["tokyodev"], ["japandev"]]
    assert all(source.fetch_mode == "jobspy_api" for source in enabled)
    assert all(source.results_wanted == 25 for source in enabled)
    assert all(source.request_timeout == 45 for source in enabled)
    assert config.orchestrator.scraper_interval_hours == 12
