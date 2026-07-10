from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from core.config_loader import ScraperConfig
from services.orchestrator.main import _validate_enabled_jobspy_scrapers
from services.orchestrator.scrape_pipeline import ScrapePipelineService


def _service() -> ScrapePipelineService:
    return ScrapePipelineService(
        redis_url="redis://example",
        lock_ttl_seconds=60,
        retry_intervals=[1],
        extraction_limit=10,
        embedding_limit=10,
        embedding_max_batches=1,
        batch_stage_timeout_seconds=1,
        scraper_interval_hours=1,
        release_lock_lua="return 1",
        logger=Mock(),
    )


@pytest.mark.asyncio
async def test_disabled_scrapers_are_skipped_without_submission() -> None:
    disabled = ScraperConfig(
        site_type=["linkedin"],
        enabled=False,
        fetch_mode="jobspy_api",
    )
    enabled = ScraperConfig(
        site_type=["tokyodev"],
        enabled=True,
        fetch_mode="jobspy_api",
    )
    service = _service()
    service.scrape_single_scraper = AsyncMock(
        return_value={
            "scraper_id": "tokyodev",
            "jobs_scraped": 0,
            "jobs_imported": 0,
            "ingest_failed": 0,
            "ingest_errors": [],
            "error": None,
        }
    )
    ctx = SimpleNamespace(config=SimpleNamespace(scrapers=[disabled, enabled]))

    result = await service.run_all_scrapers(ctx, AsyncMock())

    assert result["results_by_scraper"][0]["skipped"] is True
    service.scrape_single_scraper.assert_awaited_once()


def test_enabled_jobspy_sources_require_url_and_token() -> None:
    scraper = ScraperConfig(
        site_type=["tokyodev"],
        enabled=True,
        fetch_mode="jobspy_api",
    )
    missing = SimpleNamespace(
        config=SimpleNamespace(scrapers=[scraper], jobspy=None)
    )
    with pytest.raises(RuntimeError, match="JOBSPY_URL and JOBSPY_API_TOKEN"):
        _validate_enabled_jobspy_scrapers(missing)

    configured = SimpleNamespace(
        config=SimpleNamespace(
            scrapers=[scraper],
            jobspy=SimpleNamespace(url="http://jobspy:8000", api_token="token"),
        )
    )
    _validate_enabled_jobspy_scrapers(configured)
