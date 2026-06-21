"""Registry lifecycle helpers for in-memory orchestrator task state."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any


class OrchestratorStateRegistryService:
    """Create, retrieve, and clean stale orchestration state entries."""

    def __init__(
        self,
        *,
        logger: logging.Logger,
        time_fn: Callable[[], float],
        sleep_fn: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.logger = logger
        self.time_fn = time_fn
        self.sleep_fn = sleep_fn

    async def get_or_create(
        self,
        registry: Any,
        task_id: str,
        *,
        state_cls: type[Any],
    ) -> Any:
        """Get or create orchestration state, loading from Redis if not in memory."""
        async with registry.lock:
            if task_id in registry.orchestrations:
                registry.timestamps[task_id] = self.time_fn()
                return registry.orchestrations[task_id]

        state = await state_cls.create(task_id, load_from_redis=True)

        async with registry.lock:
            if task_id not in registry.orchestrations:
                registry.orchestrations[task_id] = state
                registry.timestamps[task_id] = self.time_fn()
            return registry.orchestrations[task_id]

    async def cleanup_stale(
        self,
        registry: Any,
        *,
        ttl_seconds: float,
        sleep_seconds: float = 300,
    ) -> None:
        """Periodically remove orchestrations that have exceeded the configured TTL."""
        while True:
            await self.sleep_fn(sleep_seconds)
            stale_states: list[Any] = []
            async with registry.lock:
                now = self.time_fn()
                stale = [
                    task_id
                    for task_id, timestamp in registry.timestamps.items()
                    if now - timestamp > ttl_seconds
                ]
                for task_id in stale:
                    state = registry.orchestrations.pop(task_id, None)
                    if state:
                        stale_states.append(state)
                    registry.timestamps.pop(task_id, None)
                    registry.tasks.pop(task_id, None)
                    registry.active_task_ids.discard(task_id)
                if stale:
                    self.logger.info("Cleaned up %d stale orchestrations", len(stale))

            for state in stale_states:
                await state.close(registry)
