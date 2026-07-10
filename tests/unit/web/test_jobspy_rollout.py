from types import SimpleNamespace

from core.config_loader import ScraperConfig
from web.backend.models.responses import FetchSourceHealthResponse
from web.backend.routers.pipeline import _build_fetch_source_response


def test_tokyodev_is_an_enabled_jobspy_source_in_production(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_ENV", "production")
    source = _build_fetch_source_response(
        ScraperConfig(
            site_type=["tokyodev"],
            enabled=True,
            fetch_mode="jobspy_api",
            results_wanted=25,
        ),
        api_health=FetchSourceHealthResponse(available=True, status="available"),
    )

    assert source.fetch_mode == "jobspy_api"
    assert source.enabled is True
    assert source.deployment_allowed is True
    assert source.api_fetch_available is True


def test_disabled_provider_is_not_reported_as_available(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_ENV", "production")
    source = _build_fetch_source_response(
        ScraperConfig(
            site_type=["linkedin"],
            enabled=False,
            fetch_mode="jobspy_api",
        ),
        api_health=FetchSourceHealthResponse(available=True, status="available"),
    )

    assert source.enabled is False
    assert source.disabled_reason == "source_disabled"
    assert source.api_fetch_available is False
    assert source.availability_status == "disabled"
