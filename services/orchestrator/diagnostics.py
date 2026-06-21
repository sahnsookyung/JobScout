"""Diagnostics helpers for orchestrator Redis streams and task state."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import Any

GetStreamInfo = Callable[[str], dict[str, Any]]
StreamExists = Callable[[str], bool]
GetTaskState = Callable[[str], dict[str, Any] | None]


class OrchestratorDiagnosticsService:
    """Build diagnostics snapshots without binding logic to FastAPI routes."""

    def __init__(
        self,
        *,
        get_stream_info: GetStreamInfo,
        stream_exists: StreamExists,
        get_task_state: GetTaskState,
        recent_task_limit: int,
        recent_task_scan_limit: int,
        logger: logging.Logger,
    ) -> None:
        self.get_stream_info = get_stream_info
        self.stream_exists = stream_exists
        self.get_task_state = get_task_state
        self.recent_task_limit = recent_task_limit
        self.recent_task_scan_limit = recent_task_scan_limit
        self.logger = logger

    def get_stream_diagnostic(self, stream_name: str) -> dict[str, Any]:
        """Return status dict for a single Redis stream."""
        try:
            if not self.stream_exists(stream_name):
                return {"exists": False, "length": 0}

            info = self.get_stream_info(stream_name)
            result: dict[str, Any] = {
                "exists": True,
                "length": info.get("length", 0),
                "first_entry": info.get("first-entry"),
            }

            try:
                groups = info.get("groups", []) or []
                result["consumer_groups"] = [
                    {
                        "name": group.get("name"),
                        "consumers": group.get("consumers", 0),
                        "pending": group.get("pending", 0),
                        "last_delivered_id": group.get("last-delivered-id"),
                    }
                    for group in groups
                ]
            except Exception:
                self.logger.exception(
                    "Consumer groups error for stream %s",
                    stream_name,
                )
                result["consumer_groups_error"] = "Failed to retrieve consumer groups"

            return result
        except Exception:
            self.logger.exception("Stream diagnostic error for %s", stream_name)
            return {"error": "Failed to retrieve stream info"}

    async def get_active_orchestration_states(self, registry: Any) -> list[dict[str, Any]]:
        """Return status snapshots of all currently active orchestrations."""
        async with registry.lock:
            return [
                {
                    "task_id": task_id,
                    "status": registry.orchestrations[task_id].status,
                    "error": registry.orchestrations[task_id].error,
                }
                for task_id in registry.active_task_ids
                if task_id in registry.orchestrations
            ]

    def _recent_task_keys(self, redis_client: Any) -> Iterable[Any]:
        scan_iter = getattr(redis_client, "scan_iter", None)
        if callable(scan_iter):
            return scan_iter(match="task:*:state", count=self.recent_task_scan_limit)
        return redis_client.keys("task:*:state")

    def get_recent_tasks(self, redis_client: Any) -> list[dict[str, Any]] | dict[str, str]:
        """Return status snapshots of the most recent task states from Redis."""
        try:
            recent: list[dict[str, Any]] = []
            for scanned_count, key in enumerate(self._recent_task_keys(redis_client)):
                if scanned_count >= self.recent_task_scan_limit:
                    break
                if isinstance(key, bytes):
                    key = key.decode("utf-8", errors="replace")

                task_id = str(key).removeprefix("task:").removesuffix(":state")
                task_data = self.get_task_state(task_id)
                if task_data:
                    recent.append(
                        {
                            "task_id": task_id,
                            "status": task_data.get("status"),
                            "error": task_data.get("error"),
                        }
                    )
                if len(recent) >= self.recent_task_limit:
                    break
            return recent
        except Exception:
            self.logger.exception("Failed to retrieve recent tasks from Redis")
            return {"error": "Failed to retrieve recent tasks"}
