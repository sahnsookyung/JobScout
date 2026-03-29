#!/usr/bin/env python3
"""
RQ Worker for JobScout Notification Service

Processes notifications from the Redis Queue using the new channel architecture.

Usage:
    uv run python -m notification.worker
    uv run python -m notification.worker --burst
    uv run python -m notification.worker --verbose
"""

import os
import sys
import argparse
import logging
import threading
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from redis import Redis
from rq import Worker, Queue
from rq.registry import FailedJobRegistry

from notification import process_notification_task

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# How often (seconds) to poll the DLQ. Override via env for testing.
_DLQ_POLL_INTERVAL = int(os.environ.get('NOTIFICATION_DLQ_POLL_INTERVAL', '60'))


def _monitor_dlq(redis_conn: Redis, queue_name: str, stop: threading.Event) -> None:
    """
    Background thread: periodically log DLQ depth so log-based alerting
    catches a growing failed registry without needing a separate dashboard.

    Log levels:
      ERROR   — count increased since last poll (new terminal failures arriving)
      WARNING — count is non-zero but stable (existing failures, needs attention)
      INFO    — count just dropped to zero (operator cleared the DLQ)
    """
    queue = Queue(queue_name, connection=redis_conn)
    registry = FailedJobRegistry(queue=queue)
    last_count = 0

    while not stop.is_set():
        try:
            count = len(registry)
            if count > last_count:
                logger.error(
                    "Notification DLQ growing: %d failed job(s) (+%d since last check) "
                    "— terminal channel failure, check RQ failed registry",
                    count, count - last_count,
                )
            elif count > 0:
                logger.warning(
                    "Notification DLQ non-empty: %d failed job(s) awaiting operator action",
                    count,
                )
            elif last_count > 0:
                logger.info("Notification DLQ cleared (0 failed jobs)")
            last_count = count
        except Exception as exc:
            logger.warning("DLQ monitor check failed: %s", exc)

        stop.wait(timeout=_DLQ_POLL_INTERVAL)


def start_worker(burst: bool = False, queues: list = None):
    """Start the RQ worker with a background DLQ monitor thread."""
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

    if queues is None:
        queues = ['notifications']

    logger.info("Starting RQ Worker")
    logger.info("Redis URL: %s", redis_url)
    logger.info("Queues: %s", ", ".join(queues))
    logger.info("Burst mode: %s", burst)

    stop_event = threading.Event()

    try:
        redis_conn = Redis.from_url(redis_url)
        redis_conn.ping()
        logger.info("Connected to Redis")

        worker = Worker(queues, connection=redis_conn)

        if not burst and queues:
            # Burst mode exits immediately after draining — no point polling DLQ.
            if len(queues) > 1:
                logger.warning(
                    "DLQ monitor only watches the first queue (%s); "
                    "failures on %s are not monitored",
                    queues[0], queues[1:],
                )
            monitor = threading.Thread(
                target=_monitor_dlq,
                args=(redis_conn, queues[0], stop_event),
                name="dlq-monitor",
                daemon=True,
            )
            monitor.start()
            logger.info("DLQ monitor started (interval: %ds)", _DLQ_POLL_INTERVAL)

        if burst:
            logger.info("Running in burst mode...")
            worker.work(burst=True)
        else:
            logger.info("Worker started. Press Ctrl+C to stop.")
            worker.work(with_scheduler=True)  # processes enqueue_in delayed jobs

    except KeyboardInterrupt:
        logger.info("\nWorker stopped")
    except Exception as e:
        logger.error("Error: %s", e)
        sys.exit(1)
    finally:
        stop_event.set()


def main():
    parser = argparse.ArgumentParser(description='JobScout Notification Worker')
    parser.add_argument('--burst', action='store_true', help='Process all and exit')
    parser.add_argument('--queues', nargs='+', default=['notifications'])
    parser.add_argument('--verbose', action='store_true')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    start_worker(burst=args.burst, queues=args.queues)


if __name__ == '__main__':
    main()
