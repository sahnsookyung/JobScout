"""RQ must configure timeouts before startup commands cache Redis sockets."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from redis import Redis
from rq import Worker

from core import llm_evaluation_worker
from notification import worker as notification_worker


@pytest.mark.parametrize("worker_kind", ["notification", "llm_evaluation"])
def test_startup_connections_allow_blocking_dequeue(monkeypatch, worker_kind):
    # Redis 8 defaults to a short socket timeout. No network is used here:
    # cache a real connection as the first startup command would do.
    redis = Redis.from_url("redis://localhost:6379/0", socket_timeout=1)
    pool = redis.connection_pool
    workers = []

    def cache_connection(*args, **kwargs):
        if not pool._available_connections:
            pool._available_connections.append(pool.make_connection())
        return True

    def make_worker(queues, connection):
        worker = Worker(queues, connection=connection, prepare_for_work=False)
        worker.work = Mock()
        workers.append(worker)
        return worker

    module = notification_worker if worker_kind == "notification" else llm_evaluation_worker
    monkeypatch.setattr(module.Redis, "from_url", Mock(return_value=redis))
    monkeypatch.setattr(module, "Worker", make_worker)
    monkeypatch.setattr(module, "start_metrics_server", Mock())

    if worker_kind == "notification":
        monkeypatch.setattr(
            module, "get_notification_runtime_config",
            lambda: SimpleNamespace(redis_url="redis://localhost"),
        )
        monkeypatch.setattr(redis, "ping", cache_connection)
        module.start_worker(burst=True)
    else:
        monkeypatch.setattr(
            module, "load_config",
            lambda: SimpleNamespace(orchestrator=SimpleNamespace(redis_url="redis://localhost")),
        )
        monkeypatch.setattr(module, "bind_llm_evaluation_queue_depths", Mock())
        monkeypatch.setattr(module, "enqueue_stale_or_retryable_evaluations", Mock())
        monkeypatch.setattr(module, "schedule_llm_recovery_sweep", cache_connection)
        monkeypatch.setattr(module, "record_worker_running", Mock())
        module.start_worker(burst=True)

    try:
        assert len(workers) == 1
        assert pool._available_connections
        for connection in pool._available_connections:
            assert connection.socket_timeout > workers[0].dequeue_timeout
    finally:
        redis.close()
        pool.disconnect()
