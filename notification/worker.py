"""
RQ Worker for JobScout Notification Service - SOLID Implementation

Processes notifications from the Redis Queue using the new channel architecture.

Usage:
    uv run python -m notification.worker
    uv run python -m notification.worker --burst
    uv run python -m notification.worker --verbose
"""

import argparse
import logging
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from redis import Redis
from rq import Worker, Queue

from notification import process_notification_task
from notification.runtime_config import get_notification_runtime_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def start_worker(burst: bool = False, queues: list = None):
    """Start the RQ worker."""
    redis_url = get_notification_runtime_config().redis_url

    if queues is None:
        queues = ['notifications']

    logger.info("Starting RQ Worker")
    logger.info("Redis URL: %s", redis_url)
    logger.info("Queues: %s", ", ".join(queues))
    logger.info("Burst mode: %s", burst)

    try:
        redis_conn = Redis.from_url(redis_url)
        redis_conn.ping()
        logger.info("Connected to Redis")

        worker = Worker(queues, connection=redis_conn)

        if burst:
            logger.info("Running in burst mode...")
            worker.work(burst=True)
        else:
            logger.info("Worker started. Press Ctrl+C to stop.")
            worker.work()

    except KeyboardInterrupt:
        logger.info("\nWorker stopped")
    except Exception as e:
        logger.error("Error: %s", e)
        sys.exit(1)


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
