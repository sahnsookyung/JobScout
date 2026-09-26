import asyncio
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from core.llm.global_budget import GlobalLlmBudgetExceeded
from services.base.extraction import ProviderQuotaExceeded, _on_extraction_error, _run_extraction_batch
from services.extraction.main import ExtractionBatchConsumer


def test_budget_error_carries_next_utc_reset():
    before = datetime.now(timezone.utc)
    error = GlobalLlmBudgetExceeded("budget exhausted")
    reset = datetime.fromtimestamp(error.reset_at, timezone.utc)
    assert reset > before
    assert (reset.hour, reset.minute, reset.second) == (0, 0, 0)


def test_reset_survives_error_translation():
    with patch("services.base.extraction._mark_job_retryable"):
        with pytest.raises(ProviderQuotaExceeded) as raised:
            _on_extraction_error(GlobalLlmBudgetExceeded("daily", reset_at=1800000000), 1, None, 0, [30], 30, threading.Event())
    assert raised.value.reset_at == 1800000000


def test_batch_defers_only_remaining_work():
    repo = MagicMock()
    repo.get_unextracted_jobs.return_value = [SimpleNamespace(id=i) for i in (1, 2, 3)]
    uow = MagicMock()
    uow.return_value.__enter__.return_value = repo
    with patch("services.base.extraction.job_uow", uow), patch(
        "services.base.extraction._extract_single_job",
        side_effect=[True, ProviderQuotaExceeded("daily", reset_at=1800000000)],
    ):
        with pytest.raises(ProviderQuotaExceeded) as raised:
            _run_extraction_batch(Mock(), threading.Event())
    repo.job_post.defer_extraction_until.assert_called_once_with([2, 3], datetime.fromtimestamp(1800000000, timezone.utc))
    assert raised.value.processed == 1


def test_deferred_batch_acknowledges_without_sleep_or_requeue():
    consumer = ExtractionBatchConsumer(Mock(), threading.Event())
    error = ProviderQuotaExceeded("daily", reset_at=1800000000)
    error.processed = 4
    with patch("services.extraction.main.run_job_extraction", side_effect=error), patch(
        "services.extraction.main.asyncio.sleep", new_callable=AsyncMock
    ) as sleep, patch("services.extraction.main.enqueue_job") as enqueue:
        ok, data = asyncio.run(consumer._do_process("1", {"task_id": "budget-test"}))
    assert ok and data["status"] == "deferred"
    assert data["retry_at"] == 1800000000 and data["processed"] == 4
    assert data["error_code"] == "global_budget_exhausted"
    sleep.assert_not_called()
    enqueue.assert_not_called()
