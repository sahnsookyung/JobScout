"""Operator controls for active orchestrator tasks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

StateGetter = Callable[[Any, str], Awaitable[Any]]


class OrchestratorControlService:
    """Stop active orchestration tasks without depending on FastAPI."""

    def __init__(self, *, state_getter: StateGetter) -> None:
        self.state_getter = state_getter

    async def active(self, registry: Any) -> dict[str, Any]:
        async with registry.lock:
            task_ids = list(registry.active_task_ids)

        if not task_ids:
            return {"success": False, "message": "No active tasks"}

        states: list[dict[str, Any]] = []
        for active_task_id in task_ids:
            state = await self.state_getter(registry, active_task_id)
            states.append(
                {
                    "task_id": active_task_id,
                    "status": state.status,
                    "task_type": state.task_type,
                    "current_stage": state.current_stage,
                    "resume_fingerprint": state.resume_fingerprint,
                    "matches_count": state.matches_count,
                    "result": state.result,
                    "error": state.error,
                }
            )

        return {"success": True, "tasks": states}

    async def stop(
        self,
        registry: Any,
        *,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        async with registry.lock:
            if task_id:
                task_ids_to_stop = [task_id] if task_id in registry.active_task_ids else []
            else:
                task_ids_to_stop = list(registry.active_task_ids)

        if not task_ids_to_stop:
            return {"success": False, "message": "No active tasks to stop"}

        stopped: list[str] = []
        for active_task_id in task_ids_to_stop:
            async with registry.lock:
                task = registry.tasks.get(active_task_id)
                if task and not task.done():
                    task.cancel()
                    stopped.append(active_task_id)
                    continue

            state = await self.state_getter(registry, active_task_id)
            if state.status not in ("completed", "failed", "cancelled"):
                state.status = "cancelled"
                state.error = "Cancelled by user"
                await state._save_to_redis()
                await state.notify(
                    {
                        "task_id": active_task_id,
                        "status": "cancelled",
                        "error": "Cancelled by user",
                    }
                )
                stopped.append(active_task_id)

        return {
            "success": True,
            "stopped": stopped,
            "message": f"Cancelled {len(stopped)} task(s)",
        }
