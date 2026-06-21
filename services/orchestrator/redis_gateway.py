from __future__ import annotations

from typing import Any

from core.redis_streams import (
    delete_task_state,
    get_stream_info,
    get_task_state,
    set_task_state,
    stream_exists,
)


class RedisTaskStateGateway:
    """Compatibility projection gateway for Redis task state and diagnostics."""

    def __init__(self, *, ttl: int = 3600) -> None:
        self.ttl = ttl

    def get_task_state(self, task_id: str) -> dict[str, Any] | None:
        return get_task_state(task_id)

    def set_task_state(self, task_id: str, state: dict[str, Any], *, ttl: int | None = None) -> None:
        set_task_state(task_id, state, ttl or self.ttl)

    def delete_task_state(self, task_id: str) -> None:
        delete_task_state(task_id)

    def get_stream_info(self, stream: str) -> dict[str, Any]:
        return get_stream_info(stream)

    def stream_exists(self, stream: str) -> bool:
        return stream_exists(stream)
