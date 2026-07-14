"""Redis-backed account quotas for public ephemeral testing."""

from __future__ import annotations

import os
from typing import Any

from core.metrics import record_public_security_event
from core.redis_streams import get_redis_client

USER_QUOTA_INDEX_PREFIX = "jobscout-cloud:user-quota-keys"
QUOTA_EXEMPT_PREFIX = "jobscout-cloud:quota-exempt"

_CONSUME_SCRIPT = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local limit = tonumber(ARGV[1])
if current >= limit then
  return {0, current}
end
current = redis.call('INCR', KEYS[1])
redis.call('SADD', KEYS[2], KEYS[1])
return {1, current}
"""


class EphemeralQuotaExceeded(RuntimeError):
    """Raised when an account has consumed its public-testing allocation."""


class EphemeralQuotaUnavailable(RuntimeError):
    """Raised when public quotas cannot fail closed."""


def public_testing_quotas_enabled() -> bool:
    return os.getenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def quota_limit(operation: str, default: int) -> int:
    env_name = f"JOBSCOUT_PUBLIC_{operation.upper().replace('-', '_')}_LIMIT"
    try:
        return max(int(os.getenv(env_name, str(default))), 0)
    except ValueError as exc:
        raise EphemeralQuotaUnavailable(f"Invalid quota configuration: {env_name}.") from exc


def consume_ephemeral_quota(
    owner_id: Any,
    operation: str,
    *,
    default_limit: int,
    client: Any | None = None,
) -> int | None:
    """Consume one operation only when public-testing quotas are enabled."""
    if not public_testing_quotas_enabled():
        return None

    owner_key = str(owner_id)
    limit = quota_limit(operation, default_limit)
    if limit <= 0:
        record_public_security_event("quota_exhausted")
        raise EphemeralQuotaExceeded(f"{operation} is disabled for public testing accounts.")

    quota_key = f"jobscout-cloud:account-quota:{owner_key}:{operation}"
    index_key = f"{USER_QUOTA_INDEX_PREFIX}:{owner_key}"
    redis_client = client or get_redis_client()
    try:
        if redis_client.get(f"{QUOTA_EXEMPT_PREFIX}:{owner_key}"):
            return None
        raw = redis_client.eval(
            _CONSUME_SCRIPT,
            2,
            quota_key,
            index_key,
            limit,
        )
    except Exception as exc:
        raise EphemeralQuotaUnavailable("Public-testing quota backend is unavailable.") from exc

    if int(raw[0]) != 1:
        record_public_security_event("quota_exhausted")
        raise EphemeralQuotaExceeded(f"{operation} quota exceeded for this temporary account.")
    return max(limit - int(raw[1]), 0)
