"""Redis-backed concurrency slots for public ephemeral workflows."""

from __future__ import annotations

import os
import time
from typing import Any

USER_CONCURRENCY_PREFIX = "jobscout-cloud:concurrency:user"
TASK_OWNER_PREFIX = "jobscout-cloud:concurrency:task-owner"
GLOBAL_CONCURRENCY_KEY = "jobscout-cloud:concurrency:global"
QUOTA_EXEMPT_PREFIX = "jobscout-cloud:quota-exempt"
MAX_CONCURRENCY_TTL_SECONDS = 25 * 60 * 60

_ACQUIRE_TASK_SCRIPT = """
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[3])
local current = redis.call('GET', KEYS[1])
local ttl = tonumber(ARGV[4])
local expires_at = tonumber(ARGV[3]) + ttl

if current == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], ttl)
  redis.call('ZADD', KEYS[2], expires_at, ARGV[1])
  redis.call('EXPIRE', KEYS[2], ttl)
  redis.call('SET', KEYS[3], ARGV[2], 'EX', ttl)
  return 2
end

if current and string.sub(current, 1, 8) == 'request:' then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
  redis.call('ZREM', KEYS[2], current)
  redis.call('ZADD', KEYS[2], expires_at, ARGV[1])
  redis.call('EXPIRE', KEYS[2], ttl)
  redis.call('SET', KEYS[3], ARGV[2], 'EX', ttl)
  return 3
end

if current then
  return -1
end
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[5]) then
  return -2
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl)
redis.call('ZADD', KEYS[2], expires_at, ARGV[1])
redis.call('EXPIRE', KEYS[2], ttl)
redis.call('SET', KEYS[3], ARGV[2], 'EX', ttl)
return 1
"""

_RELEASE_TASK_SCRIPT = """
local owner_id = redis.call('GET', KEYS[2])
if owner_id then
  local user_key = ARGV[2] .. owner_id
  if redis.call('GET', user_key) == ARGV[1] then
    redis.call('DEL', user_key)
  end
end
redis.call('DEL', KEYS[2])
return redis.call('ZREM', KEYS[1], ARGV[1])
"""


class PublicTaskConcurrencyError(RuntimeError):
    """Base class for fail-closed public workflow concurrency errors."""


class PublicTaskAlreadyRunning(PublicTaskConcurrencyError):
    """The account already owns a different expensive workflow slot."""


class PublicTaskCapacityExceeded(PublicTaskConcurrencyError):
    """All global public workflow slots are occupied."""


class PublicTaskConcurrencyUnavailable(PublicTaskConcurrencyError):
    """The Redis concurrency backend could not make a safe decision."""


def public_task_concurrency_enabled() -> bool:
    return os.getenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _ttl_seconds() -> int:
    try:
        retention = int(os.getenv("JOBSCOUT_CLOUD_EPHEMERAL_RETENTION_SECONDS", "14400"))
        sweep = int(os.getenv("JOBSCOUT_CLOUD_RETENTION_SWEEP_SECONDS", "300"))
    except ValueError as exc:
        raise PublicTaskConcurrencyUnavailable(
            "Invalid public-testing retention configuration."
        ) from exc
    return min(max(retention + sweep, 60), MAX_CONCURRENCY_TTL_SECONDS)


def acquire_public_task_slot(
    client: Any,
    *,
    task_id: Any,
    owner_id: Any | None,
    global_limit: int = 2,
) -> str:
    """Acquire, refresh, or transfer a request lease to a durable task."""
    if not public_task_concurrency_enabled() or owner_id is None:
        return "disabled"
    task_key = str(task_id)
    owner_key = str(owner_id)
    try:
        if client.get(f"{QUOTA_EXEMPT_PREFIX}:{owner_key}"):
            return "exempt"
        result = int(
            client.eval(
                _ACQUIRE_TASK_SCRIPT,
                3,
                f"{USER_CONCURRENCY_PREFIX}:{owner_key}",
                GLOBAL_CONCURRENCY_KEY,
                f"{TASK_OWNER_PREFIX}:{task_key}",
                task_key,
                owner_key,
                int(time.time()),
                _ttl_seconds(),
                max(int(global_limit), 1),
            )
        )
    except PublicTaskConcurrencyError:
        raise
    except Exception as exc:
        raise PublicTaskConcurrencyUnavailable(
            "Public-testing concurrency controls are unavailable."
        ) from exc
    if result == -1:
        raise PublicTaskAlreadyRunning(
            "Another expensive operation is already running for this account."
        )
    if result == -2:
        raise PublicTaskCapacityExceeded(
            "Public testing is at capacity. Please try again shortly."
        )
    return {1: "acquired", 2: "refreshed", 3: "transferred"}.get(result, "acquired")


def release_public_task_slot(client: Any, task_id: Any) -> None:
    """Release a task slot without allowing one task to release another's slot."""
    if not public_task_concurrency_enabled():
        return
    task_key = str(task_id)
    try:
        client.eval(
            _RELEASE_TASK_SCRIPT,
            2,
            GLOBAL_CONCURRENCY_KEY,
            f"{TASK_OWNER_PREFIX}:{task_key}",
            task_key,
            f"{USER_CONCURRENCY_PREFIX}:",
        )
    except Exception as exc:
        raise PublicTaskConcurrencyUnavailable(
            "Public-testing concurrency controls are unavailable."
        ) from exc
