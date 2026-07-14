from unittest.mock import MagicMock

import pytest

from core import public_concurrency


@pytest.fixture(autouse=True)
def _enable_public_concurrency(monkeypatch):
    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    monkeypatch.setenv("JOBSCOUT_CLOUD_EPHEMERAL_RETENTION_SECONDS", "14400")
    monkeypatch.setenv("JOBSCOUT_CLOUD_RETENTION_SWEEP_SECONDS", "300")


def test_acquire_transfers_request_lease_to_task():
    client = MagicMock()
    client.get.return_value = None
    client.eval.return_value = 3

    state = public_concurrency.acquire_public_task_slot(
        client,
        task_id="task-1",
        owner_id="owner-1",
    )

    assert state == "transferred"
    args = client.eval.call_args.args
    assert args[1] == 3
    assert args[2].endswith(":owner-1")
    assert args[4].endswith(":task-1")
    assert args[5:7] == ("task-1", "owner-1")
    assert args[8] == 14700


@pytest.mark.parametrize(
    ("result", "error_type"),
    [
        (-1, public_concurrency.PublicTaskAlreadyRunning),
        (-2, public_concurrency.PublicTaskCapacityExceeded),
    ],
)
def test_acquire_fails_closed_for_busy_or_full_capacity(result, error_type):
    client = MagicMock()
    client.get.return_value = None
    client.eval.return_value = result

    with pytest.raises(error_type):
        public_concurrency.acquire_public_task_slot(
            client,
            task_id="task-1",
            owner_id="owner-1",
        )


def test_backend_failure_is_reported_as_unavailable():
    client = MagicMock()
    client.get.side_effect = RuntimeError("redis down")

    with pytest.raises(public_concurrency.PublicTaskConcurrencyUnavailable):
        public_concurrency.acquire_public_task_slot(
            client,
            task_id="task-1",
            owner_id="owner-1",
        )


def test_release_uses_task_owner_mapping():
    client = MagicMock()

    public_concurrency.release_public_task_slot(client, "task-1")

    args = client.eval.call_args.args
    assert args[1] == 2
    assert args[2] == public_concurrency.GLOBAL_CONCURRENCY_KEY
    assert args[3].endswith(":task-1")
    assert args[4] == "task-1"


def test_disabled_mode_never_touches_redis(monkeypatch):
    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "false")
    client = MagicMock()

    assert (
        public_concurrency.acquire_public_task_slot(
            client,
            task_id="task-1",
            owner_id="owner-1",
        )
        == "disabled"
    )
    public_concurrency.release_public_task_slot(client, "task-1")
    assert client.mock_calls == []
