from __future__ import annotations

import pytest

from core.llm.provider_rate_limiter import (
    ProviderCircuitBreaker,
    ProviderCircuitOpen,
    ProviderRateLimitExceeded,
    ProviderRateLimiter,
)


class _FakeRedis:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def eval(self, *args):
        self.calls.append(args)
        if not self.responses:
            raise AssertionError("No fake Redis response queued")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class _FakeCircuitRedis:
    def __init__(self):
        self.values: dict[str, int | str] = {}
        self.ttls: dict[str, int] = {}

    def exists(self, key):
        return key in self.values

    def ttl(self, key):
        return self.ttls.get(key, -1)

    def incr(self, key):
        value = int(self.values.get(key, 0)) + 1
        self.values[key] = value
        return value

    def expire(self, key, seconds):
        self.ttls[key] = seconds

    def setex(self, key, seconds, value):
        self.values[key] = value
        self.ttls[key] = seconds

    def get(self, key):
        return self.values.get(key)

    def delete(self, *keys):
        deleted = 0
        for key in keys:
            if key in self.values:
                deleted += 1
                self.values.pop(key, None)
                self.ttls.pop(key, None)
        return deleted


def test_provider_rate_limiter_allows_available_slot() -> None:
    redis = _FakeRedis([[1, 0, 39]])
    limiter = ProviderRateLimiter(
        client_factory=lambda: redis,
        time_func=lambda: 10.0,
        sleep_func=lambda _seconds: None,
    )

    decision = limiter.acquire(provider_name="nvidia", requests_per_minute=40)

    assert decision.allowed is True
    assert decision.retry_after_seconds == 0
    assert decision.remaining == 39
    assert redis.calls[0][2].startswith("llm-provider-rate:nvidia")


def test_provider_rate_limiter_waits_and_retries_until_slot_available() -> None:
    redis = _FakeRedis([[0, 1000, 0], [1, 0, 38]])
    now = [10.0]
    slept = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    limiter = ProviderRateLimiter(
        client_factory=lambda: redis,
        time_func=lambda: now[0],
        sleep_func=sleep,
    )

    decision = limiter.wait_for_slot(
        provider_name="nvidia",
        requests_per_minute=40,
        max_wait_seconds=5,
    )

    assert decision.allowed is True
    assert slept == [1.0]
    assert len(redis.calls) == 2


def test_provider_rate_limiter_raises_when_slot_exceeds_max_wait() -> None:
    redis = _FakeRedis([[0, 2000, 0]])
    limiter = ProviderRateLimiter(
        client_factory=lambda: redis,
        time_func=lambda: 10.0,
        sleep_func=lambda _seconds: None,
    )

    with pytest.raises(ProviderRateLimitExceeded) as exc_info:
        limiter.wait_for_slot(
            provider_name="nvidia",
            requests_per_minute=40,
            max_wait_seconds=1,
        )

    assert exc_info.value.provider_name == "nvidia"
    assert exc_info.value.retry_after_seconds == 2.0


def test_provider_rate_limiter_fails_closed_when_redis_unavailable() -> None:
    redis = _FakeRedis([RuntimeError("redis down")])
    limiter = ProviderRateLimiter(
        client_factory=lambda: redis,
        time_func=lambda: 10.0,
        sleep_func=lambda _seconds: None,
        window_seconds=60,
    )

    with pytest.raises(ProviderRateLimitExceeded) as exc_info:
        limiter.acquire(provider_name="nvidia", requests_per_minute=40)

    assert exc_info.value.retry_after_seconds == 60


def test_provider_circuit_breaker_opens_after_threshold() -> None:
    redis = _FakeCircuitRedis()
    circuit = ProviderCircuitBreaker(
        client_factory=lambda: redis,
        failure_threshold=2,
        cooldown_seconds=120,
    )

    circuit.record_failure("nvidia")
    circuit.assert_available("nvidia")
    circuit.record_failure("nvidia")

    with pytest.raises(ProviderCircuitOpen) as exc_info:
        circuit.assert_available("nvidia")

    assert exc_info.value.provider_name == "nvidia"
    assert exc_info.value.retry_after_seconds == 120


def test_provider_circuit_breaker_success_clears_state() -> None:
    redis = _FakeCircuitRedis()
    circuit = ProviderCircuitBreaker(
        client_factory=lambda: redis,
        failure_threshold=1,
        cooldown_seconds=120,
    )

    circuit.record_failure("nvidia")
    with pytest.raises(ProviderCircuitOpen):
        circuit.assert_available("nvidia")

    circuit.record_success("nvidia")
    circuit.assert_available("nvidia")


def test_provider_circuit_breaker_scopes_by_model_and_reports_status() -> None:
    redis = _FakeCircuitRedis()
    circuit = ProviderCircuitBreaker(
        client_factory=lambda: redis,
        failure_threshold=1,
        cooldown_seconds=120,
    )

    circuit.record_failure("nvidia", model="model-a")

    with pytest.raises(ProviderCircuitOpen):
        circuit.assert_available("nvidia", model="model-a")
    circuit.assert_available("nvidia", model="model-b")
    status = circuit.status("nvidia", model="model-a")
    assert status["circuit_open"] is True
    assert status["circuit_failure_count"] == 1

    reset = circuit.reset("nvidia", model="model-a")
    assert reset["deleted_keys"] == 2
    circuit.assert_available("nvidia", model="model-a")
