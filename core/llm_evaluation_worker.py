from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from prometheus_client import start_http_server as start_metrics_server
from redis import Redis
from rq import Queue, Worker

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core import metrics as _metrics_declarations  # noqa: F401
from core.config_loader import load_config
from core.llm_evaluation_queue import (
    LLM_EVALUATION_QUEUE,
    check_llm_evaluation_queue_readiness,
    enqueue_stale_or_retryable_evaluations,
    get_llm_evaluation_queue_status,
    schedule_llm_recovery_sweep,
)
from core.metrics import bind_llm_evaluation_queue_depths, record_worker_running

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def _metrics_port() -> int:
    raw = os.getenv("LLM_EVALUATION_METRICS_PORT", "9474")
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid LLM_EVALUATION_METRICS_PORT=%r; defaulting to 9474", raw)
        return 9474

def _queue_depths(queue: Queue) -> dict[str, int]:
    status = get_llm_evaluation_queue_status(queue)
    return {
        key: int(status.get(key, 0) or 0)
        for key in ("queued", "started", "deferred", "scheduled", "failed")
    }


def start_worker(*, burst: bool = False, queue_names: list[str] | None = None) -> None:
    redis_conn = Redis.from_url(load_config().orchestrator.redis_url)
    queues = [Queue(name, connection=redis_conn) for name in (queue_names or [LLM_EVALUATION_QUEUE])]

    metrics_port = _metrics_port()
    start_metrics_server(metrics_port)
    logger.info("LLM evaluation worker metrics listening on :%s", metrics_port)
    if queues:
        bind_llm_evaluation_queue_depths(lambda: _queue_depths(queues[0]))

    try:
        enqueue_stale_or_retryable_evaluations()
    except Exception:
        logger.warning("Failed to run LLM evaluation startup sweep", exc_info=True)
    try:
        schedule_llm_recovery_sweep(delay_seconds=0, queue=queues[0])
    except Exception:
        logger.warning("Failed to schedule LLM evaluation recovery sweep", exc_info=True)

    worker = Worker(queues, connection=redis_conn)
    record_worker_running("llm_evaluation", "worker", True)
    try:
        worker.work(burst=burst, with_scheduler=True)
    finally:
        record_worker_running("llm_evaluation", "worker", False)


def check_readiness() -> None:
    status = check_llm_evaluation_queue_readiness()
    logger.info("LLM evaluation worker readiness: %s", status)


def main() -> None:
    parser = argparse.ArgumentParser(description="JobScout LLM evaluation RQ worker")
    parser.add_argument("--burst", action="store_true", help="Process queued jobs and exit")
    parser.add_argument("--queue", action="append", dest="queues", help="Queue name to consume")
    parser.add_argument(
        "--check-readiness",
        action="store_true",
        help="Ping Redis and inspect the LLM evaluation queue, then exit",
    )
    args = parser.parse_args()
    if args.check_readiness:
        check_readiness()
        return
    start_worker(burst=args.burst, queue_names=args.queues)


if __name__ == "__main__":
    main()
