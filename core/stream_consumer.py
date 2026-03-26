"""
Redis Stream Consumer - Common consumer logic for microservices.

This module provides reusable consumer patterns for services that consume
from Redis Streams. It handles:
- Reading messages from streams with proper error handling
- Message validation
- Consumer loop with backoff on errors
- Graceful shutdown via stop events

Usage:
    from core.stream_consumer import StreamConsumer, validate_message

    consumer = StreamConsumer(
        stream="extraction:jobs",
        group="extraction-service",
        consumer_name="extraction-1",
        logger=logger,
    )

    async def process_message(msg_id: str, msg: dict) -> bool:
        # Process the message
        return True

    await consumer.consume_loop(
        process_fn=process_message,
        stop_event=stop_event,
    )
"""

import asyncio
import logging
import threading
from typing import Any, Callable, Optional

from core.redis_streams import read_stream, ack_message, publish_completion

logger = logging.getLogger(__name__)


def validate_message(msg: dict, required_fields: list[str]) -> tuple[bool, str]:
    """Validate that a message contains all required fields.

    Args:
        msg: Message data dict
        required_fields: List of field names that must be present

    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if all required fields are present
        - error_message: Empty string if valid, otherwise describes the missing field
    """
    for field in required_fields:
        if field not in msg:
            return False, f"Missing required field: {field}"
    return True, ""


class StreamConsumer:
    """Generic Redis Stream consumer with common error handling and loop logic.

    This class provides a reusable consumer loop that:
    - Reads messages from a Redis Stream consumer group
    - Handles StopIteration and exceptions gracefully
    - Implements backoff on errors (1 second sleep)
    - Supports graceful shutdown via stop_event
    - Tracks message and error counts

    Subclasses should override _process_message() to implement service-specific logic.
    """

    def __init__(
        self,
        stream: str,
        group: str,
        consumer_name: str,
        logger: logging.Logger,
        block_ms: int = 5000,
        error_backoff_seconds: float = 1.0,
    ):
        """Initialize the stream consumer.

        Args:
            stream: Redis Stream name (e.g., "extraction:jobs")
            group: Consumer group name (e.g., "extraction-service")
            consumer_name: Unique consumer identifier (e.g., "extraction-1")
            logger: Logger instance for this consumer
            block_ms: Milliseconds to block waiting for messages (default 5000)
            error_backoff_seconds: Seconds to wait after an error before retrying
        """
        self.stream = stream
        self.group = group
        self.consumer_name = consumer_name
        self.logger = logger
        self.block_ms = block_ms
        self.error_backoff_seconds = error_backoff_seconds

        # Metrics
        self.message_count = 0
        self.error_count = 0

    def get_one_message(self) -> Optional[tuple[str, dict]]:
        """Pull a single message from the stream, blocking up to block_ms.

        Returns:
            Tuple of (message_id, message_data) if a message was received,
            None if the stream is exhausted or an error occurred.

        Note:
            Errors are logged and None is returned. This allows the consumer
            loop to continue running even if transient Redis errors occur.
            Persistent errors will be logged repeatedly for visibility.
        """
        gen = read_stream(
            self.stream,
            self.group,
            self.consumer_name,
            count=1,
            block=self.block_ms,
        )
        try:
            return next(gen)
        except StopIteration:
            # Generator exhausted - no messages available (normal timeout)
            return None
        except Exception as e:
            # Log the actual error from read_stream
            self.logger.error(
                "❌ Error reading from %s: %s: %s",
                self.stream,
                type(e).__name__,
                e,
            )
            return None

    async def _process_message(self, msg_id: str, msg: dict) -> bool:
        """Process a single message. Override in subclass for service-specific logic.

        Args:
            msg_id: Redis Stream message ID
            msg: Message data dict

        Returns:
            True if processing succeeded, False if it failed (message will still be acked)
        """
        raise NotImplementedError("Subclasses must implement _process_message()")

    async def consume_loop(
        self,
        stop_event: threading.Event,
    ) -> None:
        """Main consumer loop that runs until stop_event is set.

        This method:
        - Continuously reads messages from the stream
        - Calls _process_message() for each message
        - Implements backoff on errors
        - Handles graceful shutdown via stop_event
        - Logs summary statistics on exit

        Args:
            stop_event: Threading event to signal shutdown

        Raises:
            asyncio.CancelledError: If the task is cancelled (re-raised for proper cleanup)
        """
        self.logger.info(
            "Starting consumer for %s (group: %s, consumer: %s)",
            self.stream,
            self.group,
            self.consumer_name,
        )

        while not stop_event.is_set():
            try:
                self.logger.debug("Waiting for message from %s...", self.stream)
                result = await asyncio.to_thread(self.get_one_message)

                if result is None:
                    self.logger.debug("No messages received (timeout), continuing...")
                    continue

                msg_id, msg = result
                self.message_count += 1

                success = await self._process_message(msg_id, msg)
                if not success:
                    self.error_count += 1

            except asyncio.CancelledError:
                self.logger.info(
                    "🛑 Consumer cancelled (processed: %d, errors: %d)",
                    self.message_count,
                    self.error_count,
                )
                raise

            except Exception as e:
                self.error_count += 1
                self.logger.error(
                    "❌ Error in consumer loop: %s: %s",
                    type(e).__name__,
                    e,
                    exc_info=True,
                )
                await asyncio.sleep(self.error_backoff_seconds)

        self.logger.info(
            "Consumer stopped (processed: %d, errors: %d)",
            self.message_count,
            self.error_count,
        )


class StreamConsumerWithCompletion(StreamConsumer):
    """Stream consumer that publishes completion events and acks messages.

    This extends StreamConsumer with common completion handling:
    - Publishes completion events to Redis PubSub
    - Acknowledges messages after processing
    - Handles failures by publishing failed status

    Subclasses should override _do_process() to implement the actual processing logic.
    """

    def __init__(
        self,
        stream: str,
        group: str,
        consumer_name: str,
        completion_channel: str,
        logger: logging.Logger,
        block_ms: int = 5000,
        error_backoff_seconds: float = 1.0,
    ):
        """Initialize the stream consumer with completion handling.

        Args:
            stream: Redis Stream name
            group: Consumer group name
            consumer_name: Unique consumer identifier
            completion_channel: Redis PubSub channel for completion events
            logger: Logger instance
            block_ms: Milliseconds to block waiting for messages
            error_backoff_seconds: Seconds to wait after an error before retrying
        """
        super().__init__(
            stream, group, consumer_name, logger, block_ms, error_backoff_seconds
        )
        self.completion_channel = completion_channel

    async def _do_process(self, msg_id: str, msg: dict) -> tuple[bool, dict]:
        """Process a message and return result data. Override in subclass.

        Args:
            msg_id: Redis Stream message ID
            msg: Message data dict

        Returns:
            Tuple of (success, result_data)
            - success: True if processing succeeded
            - result_data: Dict with status and any additional data to publish
        """
        raise NotImplementedError("Subclasses must implement _do_process()")

    async def _process_message(self, msg_id: str, msg: dict) -> bool:
        """Process a message, publish completion, and ack.

        This is the final implementation that:
        - Calls _do_process() to do the actual work
        - Publishes completion event (success or failure)
        - Acknowledges the message

        Args:
            msg_id: Redis Stream message ID
            msg: Message data dict

        Returns:
            True if processing succeeded, False otherwise
        """
        task_id = msg.get("task_id", "unknown")

        try:
            success, result_data = await self._do_process(msg_id, msg)

            # Publish completion
            completion_payload = {
                "task_id": task_id,
                "status": result_data.get("status", "completed" if success else "failed"),
            }
            completion_payload.update({
                k: v for k, v in result_data.items()
                if k not in ("status",)
            })

            await asyncio.to_thread(
                publish_completion,
                self.completion_channel,
                completion_payload,
            )

            # Always ack the message (even on failure to avoid redelivery loop)
            await asyncio.to_thread(
                ack_message,
                self.stream,
                self.group,
                msg_id,
            )

            if success:
                self.logger.info(
                    "✅ Job done: task_id=%s, status=%s",
                    task_id,
                    completion_payload["status"],
                )
            else:
                self.logger.info(
                    "✅ Acknowledged failed job: task_id=%s, msg_id=%s",
                    task_id,
                    msg_id,
                )

            return success

        except Exception as e:
            # Unexpected error - still publish failure and ack
            self.logger.error(
                "❌ Processing failed: task_id=%s, error=%s: %s",
                task_id,
                type(e).__name__,
                e,
                exc_info=True,
            )

            await asyncio.to_thread(
                publish_completion,
                self.completion_channel,
                {
                    "task_id": task_id,
                    "status": "failed",
                    "error": str(e),
                },
            )

            await asyncio.to_thread(
                ack_message,
                self.stream,
                self.group,
                msg_id,
            )

            self.logger.info("✅ Acknowledged failed job: msg_id=%s", msg_id)
            return False
