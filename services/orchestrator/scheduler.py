"""Scheduler use cases for the orchestrator service."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, Optional

import redis.asyncio as redis_async

from services.orchestrator.pipeline_runs import PipelineRunService

logger = logging.getLogger(__name__)

ScrapeLoop = Callable[[Any, Any, asyncio.Event], Awaitable[None]]
RedisFactory = Callable[..., Any]
RepairFn = Callable[..., Any]


async def _close_async_client(client: Any) -> None:
    close_fn = getattr(client, "aclose", None) or getattr(client, "close", None)
    if close_fn is None:
        return
    result = close_fn()
    if hasattr(result, "__await__"):
        await result


class ScrapeScheduler:
    """Own the recurring scrape scheduler's infrastructure lifecycle."""

    def __init__(
        self,
        *,
        ctx: Any,
        redis_url: str,
        loop_fn: ScrapeLoop,
        disabled: bool = False,
        redis_factory: RedisFactory = redis_async.from_url,
    ) -> None:
        self._ctx = ctx
        self._redis_url = redis_url
        self._loop_fn = loop_fn
        self._disabled = disabled
        self._redis_factory = redis_factory
        self._redis_client: Optional[Any] = None
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    @property
    def task(self) -> Optional[asyncio.Task]:
        return self._task

    async def start(self) -> None:
        if self._disabled:
            logger.info("Scraper scheduler disabled via DISABLE_SCRAPER")
            return
        if self._task is not None:
            return

        self._stop_event.clear()
        self._redis_client = self._redis_factory(self._redis_url)
        self._task = asyncio.create_task(
            self._loop_fn(self._ctx, self._redis_client, self._stop_event)
        )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self._redis_client is not None:
            await _close_async_client(self._redis_client)
            self._redis_client = None


class RepairScheduler:
    """Own the periodic stuck-job repair schedule."""

    def __init__(
        self,
        *,
        pipeline_runs: PipelineRunService,
        interval_seconds: int,
        extraction_limit: int,
        embedding_limit: int,
        repair_fn: RepairFn,
    ) -> None:
        self._pipeline_runs = pipeline_runs
        self._interval_seconds = interval_seconds
        self._extraction_limit = extraction_limit
        self._embedding_limit = embedding_limit
        self._repair_fn = repair_fn
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    @property
    def task(self) -> Optional[asyncio.Task]:
        return self._task

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._interval_seconds,
                )
            except asyncio.TimeoutError:
                await self.run_once()
            except asyncio.CancelledError:
                raise

    async def run_once(self) -> None:
        task_id = f"repair-{uuid.uuid4().hex[:8]}"
        try:
            await asyncio.to_thread(
                self._repair_fn,
                task_id=task_id,
                pipeline_runs=self._pipeline_runs,
                extraction_limit=self._extraction_limit,
                embedding_limit=self._embedding_limit,
            )
        except Exception:
            logger.warning("Scheduled stuck-job repair failed", exc_info=True)
