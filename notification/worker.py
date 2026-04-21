"""
RQ Worker for JobScout Notification Service

Processes notifications from the Redis Queue using the new channel architecture.

Usage:
    uv run python -m notification.worker
    uv run python -m notification.worker --burst
    uv run python -m notification.worker --verbose
"""

import argparse
import logging
import os
import sys
import threading
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from prometheus_client import start_http_server as start_metrics_server
# Import for side effect: registers all JobScout Counter/Histogram singletons
# so start_http_server(9464) below exposes them (at zero) immediately.
from core import metrics as _metrics_declarations  # noqa: F401
from redis import Redis
from rq import Worker, Queue
from rq.registry import FailedJobRegistry

from notification.runtime_config import get_notification_runtime_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# How often (seconds) to poll the DLQ. Override via env for testing.
try:
    _DLQ_POLL_INTERVAL = int(os.environ.get('NOTIFICATION_DLQ_POLL_INTERVAL', '60'))
except ValueError:
    logger.warning(
        "Invalid NOTIFICATION_DLQ_POLL_INTERVAL value %r — defaulting to 60s",
        os.environ.get('NOTIFICATION_DLQ_POLL_INTERVAL'),
    )
    _DLQ_POLL_INTERVAL = 60

# Prometheus scrape port for this worker. RQ's work() loop is blocking and
# synchronous, so we spin up prometheus_client's built-in daemon-thread HTTP
# server on a dedicated port rather than embed in an async app.
try:
    _METRICS_PORT = int(os.environ.get('NOTIFICATION_METRICS_PORT', '9464'))
except ValueError:
    logger.warning(
        "Invalid NOTIFICATION_METRICS_PORT value %r — defaulting to 9464",
        os.environ.get('NOTIFICATION_METRICS_PORT'),
    )
    _METRICS_PORT = 9464


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
    redis_url = get_notification_runtime_config().redis_url

    if queues is None:
        queues = ['notifications']

    logger.info("Starting RQ Worker")
    try:
        from urllib.parse import urlparse
        parsed = urlparse(redis_url)
        safe_url = parsed._replace(password="***").geturl() if parsed.password else redis_url  # NOSONAR
    except Exception:
        safe_url = "<unparseable>"
    logger.info("Redis URL: %s", safe_url)
    logger.info("Queues: %s", ", ".join(queues))
    logger.info("Burst mode: %s", burst)

    stop_event = threading.Event()

    if not burst:
        # Burst runs exit after draining; no value in standing up a scrape
        # endpoint the caller won't poll.
        try:
            start_metrics_server(_METRICS_PORT)
            logger.info("Prometheus metrics server listening on :%d", _METRICS_PORT)
        except OSError as exc:
            logger.warning(
                "Could not bind Prometheus metrics server on :%d (%s) — continuing without /metrics",
                _METRICS_PORT, exc,
            )

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
            # NOTE: burst mode drains the main queue only. Jobs rescheduled
            # via enqueue_in() (transient/rate-limit retries) sit in
            # ScheduledJobRegistry and are NOT processed in burst mode.
            logger.info("Running in burst mode...")
            worker.work(burst=True)
        else:
            logger.info("Worker started. Press Ctrl+C to stop.")
            worker.work(with_scheduler=True)  # embedded scheduler fires enqueue_in delayed jobs

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
