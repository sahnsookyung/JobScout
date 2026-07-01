import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

from core import llm_evaluation_worker


def test_metrics_port_defaults_and_parses_env(monkeypatch):
    monkeypatch.delenv("LLM_EVALUATION_METRICS_PORT", raising=False)
    assert llm_evaluation_worker._metrics_port() == 9474

    monkeypatch.setenv("LLM_EVALUATION_METRICS_PORT", "9555")
    assert llm_evaluation_worker._metrics_port() == 9555


def test_metrics_port_falls_back_for_invalid_env(monkeypatch, caplog):
    monkeypatch.setenv("LLM_EVALUATION_METRICS_PORT", "not-a-port")

    assert llm_evaluation_worker._metrics_port() == 9474
    assert "Invalid LLM_EVALUATION_METRICS_PORT" in caplog.text


def test_queue_depths_normalizes_missing_and_string_values():
    queue = Mock()

    with patch(
        "core.llm_evaluation_worker.get_llm_evaluation_queue_status",
        return_value={
            "queued": "3",
            "started": None,
            "deferred": 2,
            "scheduled": 0,
            "failed": "1",
            "ignored": 99,
        },
    ):
        assert llm_evaluation_worker._queue_depths(queue) == {
            "queued": 3,
            "started": 0,
            "deferred": 2,
            "scheduled": 0,
            "failed": 1,
        }


def test_check_readiness_logs_queue_status(caplog):
    with patch(
        "core.llm_evaluation_worker.check_llm_evaluation_queue_readiness",
        return_value={"ready": True, "queued": 0},
    ) as check:
        llm_evaluation_worker.check_readiness()

    check.assert_called_once_with()
    assert "LLM evaluation worker readiness" in caplog.text


def test_main_checks_readiness_and_exits(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["llm-evaluation-worker", "--check-readiness"],
    )

    with patch("core.llm_evaluation_worker.check_readiness") as check, patch(
        "core.llm_evaluation_worker.start_worker",
    ) as start:
        llm_evaluation_worker.main()

    check.assert_called_once_with()
    start.assert_not_called()


def test_main_passes_burst_and_queue_arguments(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "llm-evaluation-worker",
            "--burst",
            "--queue",
            "llm_evaluations",
            "--queue",
            "llm_evaluation_retry",
        ],
    )

    with patch("core.llm_evaluation_worker.start_worker") as start:
        llm_evaluation_worker.main()

    start.assert_called_once_with(
        burst=True,
        queue_names=["llm_evaluations", "llm_evaluation_retry"],
    )


def test_start_worker_wires_queue_metrics_and_worker_lifecycle(monkeypatch):
    config = SimpleNamespace(
        orchestrator=SimpleNamespace(redis_url="redis://localhost:6379/2"),
    )
    redis_conn = Mock(name="redis")
    queue_instances = []

    def queue_factory(name, connection):
        queue = Mock(name=f"queue-{name}")
        queue.name = name
        queue.connection = connection
        queue_instances.append(queue)
        return queue

    worker = Mock()
    worker_cls = Mock(return_value=worker)

    with patch("core.llm_evaluation_worker.load_config", return_value=config), patch(
        "core.llm_evaluation_worker.Redis.from_url",
        return_value=redis_conn,
    ) as from_url, patch(
        "core.llm_evaluation_worker.Queue",
        side_effect=queue_factory,
    ) as queue_cls, patch(
        "core.llm_evaluation_worker._metrics_port",
        return_value=9555,
    ), patch(
        "core.llm_evaluation_worker.start_metrics_server",
    ) as metrics_server, patch(
        "core.llm_evaluation_worker.bind_llm_evaluation_queue_depths",
    ) as bind_depths, patch(
        "core.llm_evaluation_worker.get_llm_evaluation_queue_status",
        return_value={
            "queued": 1,
            "started": 0,
            "deferred": 0,
            "scheduled": 0,
            "failed": 0,
        },
    ), patch(
        "core.llm_evaluation_worker.enqueue_stale_or_retryable_evaluations",
        return_value=2,
    ) as startup_sweep, patch(
        "core.llm_evaluation_worker.Worker",
        worker_cls,
    ), patch(
        "core.llm_evaluation_worker.record_worker_running",
    ) as record_running:
        llm_evaluation_worker.start_worker(
            burst=True,
            queue_names=["primary", "retry"],
        )

    from_url.assert_called_once_with("redis://localhost:6379/2")
    assert [queue.name for queue in queue_instances] == ["primary", "retry"]
    assert queue_cls.call_count == 2
    metrics_server.assert_called_once_with(9555)
    bind_depths.assert_called_once()
    assert callable(bind_depths.call_args.args[0])
    startup_sweep.assert_called_once_with()
    worker_cls.assert_called_once_with(queue_instances, connection=redis_conn)
    worker.work.assert_called_once_with(burst=True, with_scheduler=True)
    assert record_running.call_args_list[0].args == (
        "llm_evaluation",
        "worker",
        True,
    )
    assert record_running.call_args_list[-1].args == (
        "llm_evaluation",
        "worker",
        False,
    )


def test_start_worker_keeps_running_when_startup_sweep_fails(caplog):
    config = SimpleNamespace(orchestrator=SimpleNamespace(redis_url="redis://redis/0"))
    worker = Mock()

    with patch("core.llm_evaluation_worker.load_config", return_value=config), patch(
        "core.llm_evaluation_worker.Redis.from_url",
        return_value=Mock(),
    ), patch("core.llm_evaluation_worker.Queue") as queue_cls, patch(
        "core.llm_evaluation_worker._metrics_port",
        return_value=9474,
    ), patch(
        "core.llm_evaluation_worker.start_metrics_server",
    ), patch(
        "core.llm_evaluation_worker.bind_llm_evaluation_queue_depths",
    ), patch(
        "core.llm_evaluation_worker.enqueue_stale_or_retryable_evaluations",
        side_effect=RuntimeError("db down"),
    ), patch(
        "core.llm_evaluation_worker.Worker",
        return_value=worker,
    ), patch(
        "core.llm_evaluation_worker.record_worker_running",
    ) as record_running:
        queue_cls.return_value.name = "llm_evaluations"

        llm_evaluation_worker.start_worker()

    worker.work.assert_called_once_with(burst=False, with_scheduler=True)
    assert "Failed to run LLM evaluation startup sweep" in caplog.text
    assert record_running.call_args_list[-1].args == (
        "llm_evaluation",
        "worker",
        False,
    )


def test_start_worker_records_stopped_when_work_raises():
    config = SimpleNamespace(orchestrator=SimpleNamespace(redis_url="redis://redis/0"))
    worker = Mock()
    worker.work.side_effect = KeyboardInterrupt

    with patch("core.llm_evaluation_worker.load_config", return_value=config), patch(
        "core.llm_evaluation_worker.Redis.from_url",
        return_value=Mock(),
    ), patch("core.llm_evaluation_worker.Queue"), patch(
        "core.llm_evaluation_worker.start_metrics_server",
    ), patch(
        "core.llm_evaluation_worker.bind_llm_evaluation_queue_depths",
    ), patch(
        "core.llm_evaluation_worker.enqueue_stale_or_retryable_evaluations",
    ), patch(
        "core.llm_evaluation_worker.Worker",
        return_value=worker,
    ), patch(
        "core.llm_evaluation_worker.record_worker_running",
    ) as record_running:
        try:
            llm_evaluation_worker.start_worker()
        except KeyboardInterrupt:
            pass

    assert record_running.call_args_list[-1].args == (
        "llm_evaluation",
        "worker",
        False,
    )
