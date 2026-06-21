from unittest.mock import AsyncMock, Mock, patch

import asyncio

import pytest

from services.orchestrator.batch_stage_queue import BatchStageQueueService


def _service() -> BatchStageQueueService:
    return BatchStageQueueService(
        redis_url="redis://example",
        batch_stage_timeout_seconds=1,
        logger=Mock(),
    )


@pytest.mark.asyncio
async def test_run_batch_stage_via_queue_enqueues_and_waits_for_completion():
    service = _service()
    pubsub = AsyncMock()
    redis_client = Mock()
    redis_client.pubsub.return_value = pubsub
    wait_for_task = AsyncMock(return_value={"status": "completed", "processed": "5"})
    cleanup = AsyncMock()

    with patch("services.orchestrator.batch_stage_queue.enqueue_job") as enqueue_job:
        processed, error = await service.run_batch_stage_via_queue(
            task_id="task-1",
            stage="extract",
            stream="extraction:batch",
            completion_channel="extraction:batch:completed",
            limit=10,
            correlation={"pipeline_run_id": "run-1"},
            wait_for_task_message=wait_for_task,
            cleanup_pubsub_and_client=cleanup,
            redis_factory=lambda *_args, **_kwargs: redis_client,
        )

    assert processed == 5
    assert error is None
    pubsub.subscribe.assert_awaited_once_with("extraction:batch:completed")
    enqueue_job.assert_called_once_with(
        "extraction:batch",
        {"task_id": "task-1", "limit": 10, "pipeline_run_id": "run-1"},
    )
    cleanup.assert_awaited_once_with(redis_client, pubsub)


@pytest.mark.asyncio
async def test_run_batch_stage_via_queue_returns_error_for_failed_completion():
    service = _service()
    pubsub = AsyncMock()
    redis_client = Mock()
    redis_client.pubsub.return_value = pubsub
    wait_for_task = AsyncMock(return_value={"status": "failed", "processed": 2, "error": "boom"})

    with patch("services.orchestrator.batch_stage_queue.enqueue_job"):
        processed, error = await service.run_batch_stage_via_queue(
            task_id="task-2",
            stage="embed",
            stream="embeddings:batch",
            completion_channel="embeddings:batch:completed",
            limit=10,
            wait_for_task_message=wait_for_task,
            cleanup_pubsub_and_client=AsyncMock(),
            redis_factory=lambda *_args, **_kwargs: redis_client,
        )

    assert processed == 2
    assert error == "boom"


@pytest.mark.asyncio
async def test_run_batch_stage_via_queue_returns_error_for_timeout():
    service = _service()
    service.batch_stage_timeout_seconds = 0.01
    pubsub = AsyncMock()
    redis_client = Mock()
    redis_client.pubsub.return_value = pubsub

    async def wait_forever(_pubsub, _task_id):
        await asyncio.sleep(1)

    with patch("services.orchestrator.batch_stage_queue.enqueue_job"):
        processed, error = await service.run_batch_stage_via_queue(
            task_id="task-timeout",
            stage="embed",
            stream="embeddings:batch",
            completion_channel="embeddings:batch:completed",
            limit=10,
            wait_for_task_message=wait_forever,
            cleanup_pubsub_and_client=AsyncMock(),
            redis_factory=lambda *_args, **_kwargs: redis_client,
        )

    assert processed == 0
    assert error == "embed stage timed out waiting for completion after 0.01s"


@pytest.mark.asyncio
async def test_run_batch_stage_selects_stream_and_channel():
    service = _service()

    with patch.object(
        service,
        "run_batch_stage_via_queue",
        new_callable=AsyncMock,
        return_value=(3, None),
    ) as run_via_queue:
        processed, error = await service.run_batch_stage(
            task_id="task-3",
            stage="embed",
            limit=20,
        )

    assert processed == 3
    assert error is None
    assert run_via_queue.call_args.kwargs["stream"] == "embeddings:batch"
    assert run_via_queue.call_args.kwargs["completion_channel"] == "embeddings:batch:completed"


@pytest.mark.asyncio
async def test_run_batch_stage_rejects_unsupported_stage():
    service = _service()

    with pytest.raises(ValueError, match="Unsupported batch stage: scrape"):
        await service.run_batch_stage(
            task_id="task-4",
            stage="scrape",
            limit=20,
        )
