from __future__ import annotations

import asyncio
from collections import defaultdict
from unittest.mock import Mock
from uuid import uuid4

import pytest

from core.llm import global_budget as budget
from core.llm.budget_policy import admin_budget_exempt
from database.database import current_database_user_id, database_context


@pytest.fixture
def accounting(monkeypatch):
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_BUDGET_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_REQUESTS_PER_DAY", "200")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_TOKENS_PER_DAY", "2000000")
    monkeypatch.setenv("JOBSCOUT_CLOUD_GLOBAL_LLM_INTERACTIVE_REQUESTS_RESERVE", "20")
    monkeypatch.delenv("JOBSCOUT_CLOUD_ADMIN_LLM_BUDGET_EXEMPT", raising=False)
    monkeypatch.delenv("JOBSCOUT_CLOUD_CATALOG_LLM_BUDGET_EXEMPT", raising=False)
    state = defaultdict(int)

    def evaluate(script, key_count, *args):
        if script == budget._RECONCILE_SCRIPT:
            key, reserved, actual = args
            state[key] = max(state[key] - reserved + actual, 0)
            return state[key]
        requests, tokens, request_limit, token_limit, *rest = args
        if script == budget._RESERVE_SCRIPT:
            needed_requests, needed_tokens = 1, rest[0]
        else:
            needed_requests, needed_tokens = rest
        if request_limit >= 0 and state[requests] + needed_requests > request_limit:
            return [0, "requests", state[requests], state[tokens]]
        if token_limit >= 0 and state[tokens] + needed_tokens > token_limit:
            return [0, "tokens", state[requests], state[tokens]]
        if script == budget._RESERVE_SCRIPT:
            state[requests] += 1
            state[tokens] += needed_tokens
        return [1, "ok", state[requests], state[tokens]]

    client = Mock()
    client.eval.side_effect = evaluate
    monkeypatch.setattr(budget, "get_redis_client", lambda: client)
    return state, client


class RetryingProvider:
    def estimate_budget_tokens(self, operation, text):
        return 3_000_000

    def extract_resume_data(self, text):
        budget.consume_global_llm_request()
        budget.consume_global_llm_request()
        budget.reconcile_current_global_llm_request_usage(500)
        return {"profile": {}}


@pytest.mark.parametrize("lane", ["interactive", "background"])
def test_admin_is_uncapped_with_separate_retry_accounting(monkeypatch, accounting, lane):
    state, client = accounting
    monkeypatch.setattr(budget, "admin_budget_exempt", lambda: True)
    with budget.global_llm_budget_lane(lane):
        budget.ensure_global_llm_budget_available(estimated_tokens=3_000_000)
        assert budget.BudgetedLLMProvider(RetryingProvider()).extract_resume_data("resume") == {"profile": {}}
    assert len(state) == 2
    assert all(":llm-usage:admin:" in key for key in state)
    assert next(value for key, value in state.items() if key.endswith(":requests")) == 2
    assert next(value for key, value in state.items() if key.endswith(":tokens")) == 3_000_500
    assert client.eval.call_count == 3


def test_catalog_exemption_does_not_exempt_interactive_users(monkeypatch, accounting):
    state, _ = accounting
    monkeypatch.setenv("JOBSCOUT_CLOUD_CATALOG_LLM_BUDGET_EXEMPT", "true")
    with budget.global_llm_budget_lane("background"):
        budget.BudgetedLLMProvider(RetryingProvider()).extract_resume_data("catalog")
    assert all(":llm-usage:catalog:" in key for key in state)
    with pytest.raises(budget.GlobalLlmBudgetExceeded, match="tokens"):
        budget.BudgetedLLMProvider(RetryingProvider()).extract_resume_data("user")


def test_public_capacity_stays_exhausted_after_admin_work(monkeypatch, accounting):
    state, _ = accounting
    day = budget.datetime.now(budget.timezone.utc).strftime("%Y-%m-%d")
    requests_key = f"jobscout-cloud:llm-budget:{day}:requests"
    tokens_key = f"jobscout-cloud:llm-budget:{day}:tokens"
    state.update({requests_key: 200, tokens_key: 1_750_960})
    monkeypatch.setattr(budget, "admin_budget_exempt", lambda: True)
    budget.BudgetedLLMProvider(RetryingProvider()).extract_resume_data("admin")
    monkeypatch.setattr(budget, "admin_budget_exempt", lambda: False)
    with pytest.raises(budget.GlobalLlmBudgetExceeded, match="requests"):
        budget.ensure_global_llm_budget_available()
    assert (state[requests_key], state[tokens_key]) == (200, 1_750_960)


def test_identity_failure_fails_closed_and_never_calls_provider(monkeypatch, accounting):
    monkeypatch.setattr(budget, "admin_budget_exempt", Mock(side_effect=RuntimeError("db down")))
    with pytest.raises(budget.GlobalLlmBudgetUnavailable, match="identity verification"):
        budget.reserve_global_llm_budget(1)
    accounting[1].eval.assert_not_called()


def test_exempt_usage_backend_failure_still_fails_closed(monkeypatch, accounting):
    monkeypatch.setattr(budget, "admin_budget_exempt", lambda: True)
    accounting[1].eval.side_effect = RuntimeError("redis down")
    with pytest.raises(budget.GlobalLlmBudgetUnavailable, match="backend"):
        budget.reserve_global_llm_budget(1)


def test_only_server_installed_owner_context_propagates_and_is_restored():
    async def observe(owner):
        with database_context(user_id=owner, tenant_id="shared-tenant"):
            await asyncio.sleep(0)
            return await asyncio.to_thread(current_database_user_id)

    async def run():
        return await asyncio.gather(observe("admin"), observe("ordinary"))

    assert asyncio.run(run()) == ["admin", "ordinary"]
    assert current_database_user_id() is None


@pytest.mark.parametrize("owner", [None, "bad-uuid", "00000000-0000-0000-0000-000000000001"])
def test_missing_invalid_or_system_owner_never_inherits_admin(monkeypatch, owner):
    monkeypatch.setenv("JOBSCOUT_CLOUD_ADMIN_LLM_BUDGET_EXEMPT", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_PLATFORM_ADMIN_EMAIL", "admin@example.com")
    session = Mock(side_effect=AssertionError("should not query"))
    monkeypatch.setattr("database.database.SessionLocal", session)
    with database_context(user_id=owner, tenant_id=uuid4()):
        assert not admin_budget_exempt()
    session.assert_not_called()


def test_admin_exemption_is_opt_in(monkeypatch):
    monkeypatch.delenv("JOBSCOUT_CLOUD_ADMIN_LLM_BUDGET_EXEMPT", raising=False)
    with database_context(user_id=uuid4(), tenant_id=uuid4()):
        assert not admin_budget_exempt()
