from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

from core.config_loader import LlmJudgeProviderRuntimeConfig
from core.llm import provider_health


class _Provider:
    def __init__(self, response=None, error=None):
        self.response = response or {"ok": True, "echo": "jobscout-canary"}
        self.error = error

    def extract_structured_data(self, *_args, **_kwargs):
        if self.error is not None:
            raise self.error
        return self.response


class _Circuit:
    def __init__(self):
        self.successes = []
        self.failures = []

    def status(self, provider, model=None):
        return {
            "provider": provider,
            "model": model,
            "circuit_open": False,
            "circuit_retry_after_seconds": None,
            "circuit_failure_count": 0,
        }

    def assert_available(self, provider, model=None):
        return None

    def record_success(self, provider, model=None):
        self.successes.append((provider, model))

    def record_failure(self, provider, model=None):
        self.failures.append((provider, model))


class _Redis:
    def __init__(self):
        self.value = None
        self.ttl = None

    def get(self, _key):
        return self.value

    def setex(self, _key, ttl, value):
        self.ttl = ttl
        self.value = value


def _entry() -> LlmJudgeProviderRuntimeConfig:
    return LlmJudgeProviderRuntimeConfig(
        name="nvidia",
        provider="nvidia",
        api_key="secret",
        model="nvidia-model",
        requests_per_minute=40,
        rate_limit_max_wait_seconds=5,
    )


def test_configured_provider_status_returns_circuit_metadata():
    config = SimpleNamespace(
        matching=SimpleNamespace(
            llm_judge=SimpleNamespace(runtime=SimpleNamespace(providers=[_entry()])),
        ),
    )

    with patch("core.llm.provider_health.load_config", return_value=config), patch(
        "core.llm.provider_health.ProviderCircuitBreaker",
        return_value=_Circuit(),
    ):
        result = provider_health.configured_llm_provider_status()

    assert result["count"] == 1
    assert result["providers"][0]["name"] == "nvidia"
    assert result["providers"][0]["circuit_open"] is False
    assert "api_key" not in result["providers"][0]


def test_run_provider_canaries_records_success_without_secrets():
    config = SimpleNamespace(
        matching=SimpleNamespace(
            llm_judge=SimpleNamespace(runtime=SimpleNamespace(providers=[_entry()])),
        ),
    )
    circuit = _Circuit()
    limiter = Mock()

    with patch("core.llm.provider_health.load_config", return_value=config), patch(
        "core.llm.provider_health.ProviderCircuitBreaker",
        return_value=circuit,
    ), patch("core.llm.provider_health.ProviderRateLimiter", return_value=limiter), patch(
        "core.llm.provider_health.build_llm_provider",
        return_value=_Provider(),
    ):
        result = provider_health.run_llm_provider_canaries()

    assert result["results"][0]["status"] == "succeeded"
    assert circuit.successes == [("nvidia", "nvidia-model")]
    limiter.wait_for_slot.assert_called_once_with(
        provider_name="nvidia",
        requests_per_minute=40,
        max_wait_seconds=5,
    )
    assert "secret" not in str(result)


def test_run_provider_canaries_persists_last_status_for_provider_status():
    config = SimpleNamespace(
        matching=SimpleNamespace(
            llm_judge=SimpleNamespace(runtime=SimpleNamespace(providers=[_entry()])),
        ),
        orchestrator=SimpleNamespace(redis_url="redis://test/0"),
    )
    redis = _Redis()

    with patch("core.llm.provider_health.load_config", return_value=config), patch(
        "core.llm.provider_health.ProviderCircuitBreaker",
        return_value=_Circuit(),
    ), patch("core.llm.provider_health.ProviderRateLimiter", return_value=Mock()), patch(
        "core.llm.provider_health.build_llm_provider",
        return_value=_Provider(),
    ), patch("core.llm.provider_health.Redis.from_url", return_value=redis):
        provider_health.run_llm_provider_canaries()
        status = provider_health.configured_llm_provider_status()

    assert redis.ttl == 900
    assert status["providers"][0]["last_canary_status"] == "succeeded"
    assert status["providers"][0]["last_canary_checked_at"]


def test_run_provider_canaries_classifies_failures():
    config = SimpleNamespace(
        matching=SimpleNamespace(
            llm_judge=SimpleNamespace(runtime=SimpleNamespace(providers=[_entry()])),
        ),
    )
    circuit = _Circuit()
    error = RuntimeError("server error")
    error.status_code = 500

    with patch("core.llm.provider_health.load_config", return_value=config), patch(
        "core.llm.provider_health.ProviderCircuitBreaker",
        return_value=circuit,
    ), patch("core.llm.provider_health.ProviderRateLimiter", return_value=Mock()), patch(
        "core.llm.provider_health.build_llm_provider",
        return_value=_Provider(error=error),
    ):
        result = provider_health.run_llm_provider_canaries()

    assert result["results"][0]["status"] == "failed"
    assert result["results"][0]["error_category"] == "server_error"
    assert circuit.failures == [("nvidia", "nvidia-model")]
