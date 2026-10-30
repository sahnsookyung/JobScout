"""Exercise matching continuation with the real Redis concurrency guard."""

import uuid
from types import SimpleNamespace

import pytest
from redis import Redis

from core import public_concurrency, redis_streams
from services.scorer_matcher import main as matcher


@pytest.fixture
def page_queue(redis_container, monkeypatch):
    client = Redis.from_url(redis_container["url"], decode_responses=True)
    namespace = f"test:matching-continuation:{uuid.uuid4()}"
    owner, parent, stream = f"{namespace}:owner", f"{namespace}:parent", f"{namespace}:stream"
    monkeypatch.setenv("JOBSCOUT_PUBLIC_TESTING_QUOTAS_ENABLED", "true")
    monkeypatch.setattr(public_concurrency, "GLOBAL_CONCURRENCY_KEY", f"{namespace}:global")
    monkeypatch.setattr(redis_streams, "get_redis_client", lambda: client)
    monkeypatch.setattr(matcher, "get_redis_client", lambda: client)
    monkeypatch.setattr(matcher, "STREAM_MATCHING", stream)
    try:
        yield client, stream, parent, owner
    finally:
        keys = list(client.scan_iter(match=f"*{namespace}*"))
        if keys:
            client.delete(*keys)
        client.close()


@pytest.mark.redis
def test_matching_pages_share_parent_slot_and_release_only_at_completion(page_queue):
    client, stream, parent, owner = page_queue
    redis_streams.enqueue_job(stream, {"task_id": parent, "owner_id": owner})
    for current_page in (1, 2):
        warnings = matcher._maybe_enqueue_next_matching_page(
            parent_task_id=parent, current_page=current_page,
            resume_fingerprint="resume", owner_id=owner, tenant_id="tenant",
            result=SimpleNamespace(success=True, cancelled=False, backlog_processed_count=5),
            stats={"jobs_pending_matching": 10},
        )
        assert warnings[0]["code"] == "matching_backlog_page_enqueued"
        assert client.get(f"{public_concurrency.USER_CONCURRENCY_PREFIX}:{owner}") == parent
        assert client.zcard(public_concurrency.GLOBAL_CONCURRENCY_KEY) == 1
    assert client.xlen(stream) == 3
    with pytest.raises(public_concurrency.PublicTaskAlreadyRunning):
        redis_streams.enqueue_job(stream, {"task_id": f"{parent}:unrelated", "owner_id": owner})
    redis_streams.set_task_state(f"{parent}-match-page-2", {"status": "completed"})
    assert client.get(f"{public_concurrency.USER_CONCURRENCY_PREFIX}:{owner}") == parent
    redis_streams.set_task_state(parent, {"status": "completed"})
    assert client.get(f"{public_concurrency.USER_CONCURRENCY_PREFIX}:{owner}") is None
    assert client.zcard(public_concurrency.GLOBAL_CONCURRENCY_KEY) == 0


@pytest.mark.redis
@pytest.mark.parametrize("lease", ["missing", "other_owner", "other_task"])
def test_continuation_requires_an_existing_parent_slot_for_its_owner(page_queue, lease):
    client, stream, parent, owner = page_queue
    if lease != "missing":
        redis_streams.enqueue_job(stream, {
            "task_id": f"{parent}:other" if lease == "other_task" else parent,
            "owner_id": f"{owner}:other" if lease == "other_owner" else owner,
        })
    before = client.xlen(stream)
    with pytest.raises(public_concurrency.PublicTaskAlreadyRunning):
        redis_streams.enqueue_job(stream, {
            "task_id": f"{parent}-match-page-2", "parent_task_id": parent, "owner_id": owner,
        }, concurrency_task_id=parent)
    assert client.xlen(stream) == before
    assert client.zcard(public_concurrency.GLOBAL_CONCURRENCY_KEY) == before


@pytest.mark.redis
def test_failed_continuation_enqueue_does_not_release_parent_slot(page_queue, monkeypatch):
    client, stream, parent, owner = page_queue
    redis_streams.enqueue_job(stream, {"task_id": parent, "owner_id": owner})

    def unavailable(*args, **kwargs):
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(client, "xadd", unavailable)
    with pytest.raises(RuntimeError, match="queue unavailable"):
        redis_streams.enqueue_job(stream, {
            "task_id": f"{parent}-match-page-2", "parent_task_id": parent, "owner_id": owner,
        }, concurrency_task_id=parent)
    assert client.get(f"{public_concurrency.USER_CONCURRENCY_PREFIX}:{owner}") == parent
    assert client.zcard(public_concurrency.GLOBAL_CONCURRENCY_KEY) == 1


@pytest.mark.redis
def test_continuation_cannot_alias_an_unrelated_parent(page_queue):
    client, stream, parent, owner = page_queue
    with pytest.raises(ValueError, match="parent_task_id"):
        redis_streams.enqueue_job(stream, {
            "task_id": f"{parent}-match-page-2", "parent_task_id": "different", "owner_id": owner,
        }, concurrency_task_id=parent)
    assert client.xlen(stream) == 0
