from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from core.llm.global_budget import (
    BudgetedLLMProvider,
    GlobalLlmBudgetExceeded,
    GlobalLlmBudgetUnavailable,
    _RECONCILE_SCRIPT,
    _RESERVE_SCRIPT,
    consume_global_llm_request,
    ensure_global_llm_budget_available,
    global_llm_budget_lane,
    reconcile_current_global_llm_request_usage,
    reconcile_global_llm_budget,
    reserve_global_llm_budget,
)
from core.llm.openai_service import OpenAIService


def test_reservation_is_reconciled_to_reported_provider_usage(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    monkeypatch.setattr("core.llm.global_budget._next_utc_day_timestamp", lambda: 1_800)
    client = Mock()
    client.eval.side_effect = ([1, "ok", 1, 5000], 1_250)
    set_budget_usage = Mock()
    monkeypatch.setattr(
        "core.llm.global_budget.set_global_llm_budget_usage",
        set_budget_usage,
    )

    reservation = reserve_global_llm_budget(5_000, client=client)
    provider = SimpleNamespace(last_usage={"total_tokens": 1_250})
    reconcile_global_llm_budget(reservation, provider)

    assert client.eval.call_count == 2
    reconcile_args = client.eval.call_args.args
    assert reconcile_args[1:] == (
        1,
        reservation.tokens_key,
        5_000,
        1_250,
    )
    assert set_budget_usage.call_args_list == [
        call("requests", 1, 100, reset_at=1_800),
        call("tokens", 5_000, 2_000_000, reset_at=1_800),
        call("tokens", 1_250, 2_000_000, reset_at=1_800),
    ]


def test_budgeted_provider_reconciles_after_success(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    client = Mock()
    client.eval.side_effect = ([1, "ok", 1, 5_000], 800)
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)

    class Provider:
        def estimate_budget_tokens(self, operation, text):
            assert (operation, text) == ("extract_resume_data", "resume text")
            return 5_000

        def extract_resume_data(self, text):
            consume_global_llm_request()
            reconcile_current_global_llm_request_usage(800)
            return {"profile": {}}

    result = BudgetedLLMProvider(Provider()).extract_resume_data("resume text")

    assert result == {"profile": {}}
    assert client.eval.call_count == 2
    assert client.eval.call_args_list[0].args[6] == 5_000
    assert client.eval.call_args_list[1].args[-2:] == (5_000, 800)


def test_budgeted_provider_counts_every_actual_request_attempt(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    monkeypatch.setattr("core.llm.global_budget._next_utc_day_timestamp", lambda: 1_800)
    client = Mock()
    client.eval.side_effect = (
        [1, "ok", 1, 100],
        [1, "ok", 2, 200],
        [1, "ok", 3, 300],
    )
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)
    set_budget_usage = Mock()
    monkeypatch.setattr(
        "core.llm.global_budget.set_global_llm_budget_usage",
        set_budget_usage,
    )

    class ThreeRequestProvider:
        last_usage = None

        def estimate_budget_tokens(self, operation, texts):
            assert operation == "generate_embeddings_batch"
            return 100

        def generate_embeddings_batch(self, texts):
            for _ in range(3):
                consume_global_llm_request(client=client)
            return [[1.0] for _ in texts]

    result = BudgetedLLMProvider(ThreeRequestProvider()).generate_embeddings_batch(["a"])

    assert result == [[1.0]]
    assert client.eval.call_count == 3
    assert [entry.args[0] for entry in client.eval.call_args_list] == [
        client.eval.call_args_list[0].args[0]
    ] * 3
    assert set_budget_usage.call_args_list == [
        call("requests", 1, 100, reset_at=1_800),
        call("tokens", 100, 2_000_000, reset_at=1_800),
        call("requests", 2, 100, reset_at=1_800),
        call("tokens", 200, 2_000_000, reset_at=1_800),
        call("requests", 3, 100, reset_at=1_800),
        call("tokens", 300, 2_000_000, reset_at=1_800),
    ]


def test_known_usage_is_reconciled_even_when_parse_fails(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    client = Mock()
    client.eval.side_effect = ([1, "ok", 1, 32_800], 700)
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)

    class ParseFailureProvider:
        last_usage = {"total_tokens": 12}

        def estimate_budget_tokens(self, operation, text):
            return 32_800

        def extract_resume_data(self, text):
            consume_global_llm_request()
            reconcile_current_global_llm_request_usage(700)
            raise ValueError("invalid JSON")

    with pytest.raises(ValueError, match="invalid JSON"):
        BudgetedLLMProvider(ParseFailureProvider()).extract_resume_data("resume text")

    assert client.eval.call_count == 2
    assert client.eval.call_args_list[1].args[-2:] == (32_800, 700)


def test_unknown_attempt_retains_reservation_and_retry_reserves_again(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    client = Mock()
    client.eval.side_effect = (
        [1, "ok", 1, 32_800],
        [1, "ok", 2, 65_600],
        33_500,
    )
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)

    class RetryingProvider:
        last_usage = {"total_tokens": 10}

        def estimate_budget_tokens(self, operation, text):
            return 32_800

        def extract_resume_data(self, text):
            consume_global_llm_request()
            # The first attempt times out after dispatch and returns no usage.
            consume_global_llm_request()
            reconcile_current_global_llm_request_usage(700)
            return {"profile": {}}

    result = BudgetedLLMProvider(RetryingProvider()).extract_resume_data("resume text")

    assert result == {"profile": {}}
    assert client.eval.call_count == 3
    assert client.eval.call_args_list[2].args[-2:] == (32_800, 700)


def test_retry_fails_closed_when_second_reservation_exceeds_budget(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "50000")
    client = Mock()
    client.eval.side_effect = (
        [1, "ok", 1, 32_800],
        [0, "tokens", 1, 32_800],
    )
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)

    class RetryingProvider:
        last_usage = {"total_tokens": 10}

        def estimate_budget_tokens(self, operation, text):
            return 32_800

        def extract_resume_data(self, text):
            consume_global_llm_request()
            consume_global_llm_request()
            pytest.fail("A retry was sent without token capacity")

    with pytest.raises(GlobalLlmBudgetExceeded, match="tokens budget exhausted"):
        BudgetedLLMProvider(RetryingProvider()).extract_resume_data("resume text")

    assert client.eval.call_count == 2


def test_missing_provider_estimate_fails_before_reservation(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    reserve = Mock()
    monkeypatch.setattr("core.llm.global_budget.reserve_global_llm_budget", reserve)

    with pytest.raises(GlobalLlmBudgetUnavailable, match="request token estimate"):
        BudgetedLLMProvider(Mock(spec=["extract_resume_data"])).extract_resume_data("resume")

    reserve.assert_not_called()


def test_real_service_batch_reserves_each_chunk_near_token_ceiling(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "200")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "33")
    state = {"requests": 0, "tokens": 0}
    reserved: list[int] = []

    def eval_budget(script, *args):
        if script == _RESERVE_SCRIPT:
            request_limit, token_limit, estimate = map(int, args[3:6])
            if state["requests"] + 1 > request_limit:
                return [0, "requests", state["requests"], state["tokens"]]
            if state["tokens"] + estimate > token_limit:
                return [0, "tokens", state["requests"], state["tokens"]]
            state["requests"] += 1
            state["tokens"] += estimate
            reserved.append(estimate)
            return [1, "ok", state["requests"], state["tokens"]]
        assert script == _RECONCILE_SCRIPT
        estimate, actual = map(int, args[2:4])
        state["tokens"] = state["tokens"] - estimate + actual
        return state["tokens"]

    client = Mock()
    client.eval.side_effect = eval_budget
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)
    service = OpenAIService(api_key="test", max_output_tokens=32_768)
    service.client = Mock()
    service.client.embeddings.create.side_effect = [
        SimpleNamespace(
            usage=SimpleNamespace(total_tokens=32),
            data=[SimpleNamespace(embedding=[1.0]) for _ in range(32)],
        ),
        SimpleNamespace(
            usage=SimpleNamespace(total_tokens=1),
            data=[SimpleNamespace(embedding=[1.0])],
        ),
    ]

    vectors = BudgetedLLMProvider(service).generate_embeddings_batch(["abcd"] * 33)

    assert len(vectors) == 33
    assert reserved == [32, 1]
    assert state == {"requests": 2, "tokens": 33}
    assert service.client.embeddings.create.call_count == 2


def test_real_service_parse_failure_reconciles_reported_usage(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "200")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    budget_client = Mock()
    budget_client.eval.side_effect = ([1, "ok", 1, 33_000], 950)
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: budget_client)
    service = OpenAIService(api_key="test", max_output_tokens=32_768)
    service.client = Mock()
    service.client.chat.completions.create.return_value = SimpleNamespace(
        choices=[
            SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(content="{"),
            )
        ],
        usage=SimpleNamespace(total_tokens=950),
    )
    schema = {"type": "object", "properties": {"value": {"type": "string"}}}
    estimate = service.estimate_budget_tokens("extract_structured_data", "text", schema)

    with pytest.raises(ValueError):
        BudgetedLLMProvider(service).extract_structured_data("text", schema)

    assert estimate > 32_768
    assert budget_client.eval.call_count == 2
    assert budget_client.eval.call_args_list[0].args[6] == estimate
    assert budget_client.eval.call_args_list[1].args[-2:] == (estimate, 950)
    assert service.client.chat.completions.create.call_count == 1


def test_budget_exhaustion_records_bounded_security_event(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    monkeypatch.setattr("core.llm.global_budget._next_utc_day_timestamp", lambda: 1_800)
    client = Mock()
    client.eval.return_value = [0, "tokens", 1, 2_000_000]
    record_event = Mock()
    set_budget_usage = Mock()
    monkeypatch.setattr(
        "core.llm.global_budget.record_public_security_event",
        record_event,
    )
    monkeypatch.setattr(
        "core.llm.global_budget.set_global_llm_budget_usage",
        set_budget_usage,
    )

    with pytest.raises(GlobalLlmBudgetExceeded, match="tokens budget exhausted"):
        reserve_global_llm_budget(1, client=client)

    record_event.assert_called_once_with("global_budget_exhausted")
    assert set_budget_usage.call_args_list == [
        call("requests", 1, 100, reset_at=1_800),
        call("tokens", 2_000_000, 2_000_000, reset_at=1_800),
    ]


def test_background_lane_preserves_interactive_request_reserve(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "10")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_INTERACTIVE_REQUESTS_RESERVE", "2")
    client = Mock()
    client.eval.side_effect = (
        [0, "requests", 8, 100],
        [1, "ok", 9, 200],
    )

    with global_llm_budget_lane("background"):
        with pytest.raises(GlobalLlmBudgetExceeded, match="Background daily"):
            reserve_global_llm_budget(100, client=client)

    reservation = reserve_global_llm_budget(100, client=client)

    assert reservation is not None
    assert client.eval.call_args_list[0].args[4] == 8
    assert client.eval.call_args_list[1].args[4] == 10


def test_background_retries_cannot_consume_interactive_reserve(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "10")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_INTERACTIVE_REQUESTS_RESERVE", "2")
    client = Mock()
    client.eval.side_effect = (
        [1, "ok", 8, 100],
        [0, "requests", 8, 100],
    )
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)

    class RetryingProvider:
        def estimate_budget_tokens(self, operation, text):
            return 100

        def extract_resume_data(self, text):
            consume_global_llm_request()
            consume_global_llm_request()
            pytest.fail("Background retry used interactive request reserve")

    with global_llm_budget_lane("background"):
        with pytest.raises(GlobalLlmBudgetExceeded, match="Background daily"):
            BudgetedLLMProvider(RetryingProvider()).extract_resume_data("resume")

    assert client.eval.call_count == 2
    assert client.eval.call_args_list[0].args[4] == 8
    assert client.eval.call_args_list[1].args[4] == 8


def test_direct_budgeted_request_without_wrapper_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")

    with pytest.raises(GlobalLlmBudgetUnavailable, match="BudgetedLLMProvider reservation"):
        consume_global_llm_request()


def test_zero_usage_keeps_attempt_reservation(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    client = Mock()
    client.eval.return_value = [1, "ok", 1, 100]
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)

    class Provider:
        def estimate_budget_tokens(self, operation, text):
            return 100

        def extract_resume_data(self, text):
            consume_global_llm_request()
            reconcile_current_global_llm_request_usage(0)
            return {"profile": {}}

    BudgetedLLMProvider(Provider()).extract_resume_data("resume")

    assert client.eval.call_count == 1


def test_capacity_check_rejects_without_consuming_budget(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "200")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    client = Mock()
    client.eval.return_value = [0, "requests", 199, 1_000]

    with pytest.raises(GlobalLlmBudgetExceeded, match="requests budget exhausted"):
        ensure_global_llm_budget_available(
            estimated_requests=2,
            estimated_tokens=32_768,
            client=client,
        )

    assert "INCR" not in client.eval.call_args.args[0]
    assert client.eval.call_args.args[6:] == (2, 32_768)
