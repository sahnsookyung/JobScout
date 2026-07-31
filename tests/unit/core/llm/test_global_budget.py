from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from core.llm.global_budget import (
    BudgetedLLMProvider,
    GlobalLlmBudgetExceeded,
    consume_global_llm_request,
    reconcile_global_llm_budget,
    reserve_global_llm_budget,
)


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
    provider = Mock()
    provider.extract_resume_data.return_value = {"profile": {}}
    provider.last_usage = {"total_tokens": 800}
    reservation = object()

    monkeypatch.setattr(
        "core.llm.global_budget.reserve_global_llm_budget",
        lambda estimated_tokens: reservation,
    )
    reconcile = Mock()
    monkeypatch.setattr("core.llm.global_budget.reconcile_global_llm_budget", reconcile)

    result = BudgetedLLMProvider(provider).extract_resume_data("resume text")

    assert result == {"profile": {}}
    reconcile.assert_called_once_with(reservation, provider)


def test_budgeted_provider_counts_every_actual_request_attempt(monkeypatch) -> None:
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "100")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    monkeypatch.setattr("core.llm.global_budget._next_utc_day_timestamp", lambda: 1_800)
    client = Mock()
    client.eval.side_effect = ([1, "ok", 1, 100], [1, 2], [1, 3])
    monkeypatch.setattr("core.llm.global_budget.get_redis_client", lambda: client)
    set_budget_usage = Mock()
    monkeypatch.setattr(
        "core.llm.global_budget.set_global_llm_budget_usage",
        set_budget_usage,
    )

    class ThreeRequestProvider:
        last_usage = None

        def generate_embeddings_batch(self, texts):
            for _ in range(3):
                consume_global_llm_request(client=client)
            return [[1.0] for _ in texts]

    result = BudgetedLLMProvider(ThreeRequestProvider()).generate_embeddings_batch(["a"])

    assert result == [[1.0]]
    assert client.eval.call_count == 3
    assert client.eval.call_args_list[1].args[0] != client.eval.call_args_list[0].args[0]
    assert set_budget_usage.call_args_list == [
        call("requests", 1, 100, reset_at=1_800),
        call("tokens", 100, 2_000_000, reset_at=1_800),
        call("requests", 2, 100, reset_at=1_800),
        call("requests", 3, 100, reset_at=1_800),
    ]


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
