"""Task state snapshots and background task registration for orchestrator routes."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Optional

StateGetter = Callable[[Any, str], Awaitable[Any]]
TaskStateReader = Callable[[str], dict[str, Any] | None]
StateFactory = Callable[..., Awaitable[Any]]
CompletionHandler = Callable[[str, asyncio.Task, Any, Any | None], Awaitable[None]]


class OrchestratorTaskStateService:
    """Build task snapshots and register managed background tasks."""

    def __init__(
        self,
        *,
        state_getter: StateGetter,
        task_state_reader: TaskStateReader,
        logger: logging.Logger,
    ) -> None:
        self.state_getter = state_getter
        self.task_state_reader = task_state_reader
        self.logger = logger

    def snapshot(self, state: Any) -> dict[str, Any]:
        """Build a JSON-safe task snapshot."""
        result = dict(state.result)
        if state.matches_count and "matches_count" not in result:
            result["matches_count"] = state.matches_count
        return {
            "success": True,
            "task_id": state.task_id,
            "status": state.status,
            "task_type": state.task_type,
            "current_stage": state.current_stage,
            "result": result,
            "error": state.error,
        }

    def status_response(self, snapshot: dict[str, Any], response_model: type[Any]) -> Any:
        """Convert task snapshot dict to the configured response model."""
        return response_model(
            success=bool(snapshot.get("success", True)),
            task_id=str(snapshot.get("task_id", "")),
            status=str(snapshot.get("status", "unknown")),
            task_type=snapshot.get("task_type"),
            current_stage=snapshot.get("current_stage"),
            result=snapshot.get("result", {}) or {},
            error=snapshot.get("error"),
        )

    async def get_existing_snapshot(
        self,
        registry: Any,
        task_id: str,
        *,
        pipeline_runs: Any | None = None,
        state_factory: StateFactory,
    ) -> Optional[dict[str, Any]]:
        """Return a snapshot for an existing task without creating a new one."""
        async with registry.lock:
            state = registry.orchestrations.get(task_id)
            if state is not None:
                return self.snapshot(state)

        if pipeline_runs is not None:
            try:
                durable_snapshot = await asyncio.to_thread(pipeline_runs.get_snapshot, task_id)
                if durable_snapshot is not None:
                    return durable_snapshot
            except Exception:
                self.logger.debug(
                    "Durable task snapshot unavailable for %s; falling back to Redis",
                    task_id,
                    exc_info=True,
                )

        persisted = await asyncio.to_thread(self.task_state_reader, task_id)
        if not persisted:
            return None

        state = await state_factory(task_id, load_from_redis=True)
        return self.snapshot(state)

    async def spawn_background_task(
        self,
        registry: Any,
        task_id: str,
        task_type: str,
        coroutine: "asyncio.Future[None]",
        message: str,
        *,
        response_model: type[Any],
        completion_handler: CompletionHandler,
        current_stage: Optional[str] = None,
        initial_result: Optional[dict[str, Any]] = None,
        pipeline_runs: Any | None = None,
    ) -> Any:
        """Register and start a background task."""
        state = await self.state_getter(registry, task_id)
        state.status = "queued"
        state.task_type = task_type
        state.current_stage = current_stage
        state.result = initial_result or {}
        state.error = None
        await state._save_to_redis()
        if pipeline_runs is not None:
            snapshot = await asyncio.to_thread(
                pipeline_runs.start_run,
                task_id=task_id,
                run_type=task_type,
                current_stage=current_stage,
                metadata=state.result,
            )
            if snapshot.get("pipeline_run_id"):
                state.result["pipeline_run_id"] = snapshot["pipeline_run_id"]
                await state._save_to_redis()

        task = asyncio.create_task(coroutine)

        def safe_done_callback(t: asyncio.Task) -> None:
            try:
                cb_task = asyncio.create_task(
                    completion_handler(task_id, t, registry, pipeline_runs)
                )
                cb_task.add_done_callback(lambda _: None)
            except RuntimeError:
                self.logger.warning(
                    "Could not handle task completion for %s: no running loop",
                    task_id,
                )

        task.add_done_callback(safe_done_callback)

        async with registry.lock:
            registry.tasks[task_id] = task

        return response_model(success=True, task_id=task_id, message=message)
