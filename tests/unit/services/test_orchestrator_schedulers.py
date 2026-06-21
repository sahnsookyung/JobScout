import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from services.orchestrator.resume_etl import ResumeEtlOrchestrator
from services.orchestrator.scheduler import RepairScheduler, ScrapeScheduler
from services.orchestrator.scrape_pipeline import ScrapePipelineService


@pytest.mark.asyncio
async def test_scrape_scheduler_starts_loop_and_closes_redis_client():
    started = asyncio.Event()
    ctx = object()
    redis_client = AsyncMock()

    async def fake_loop(loop_ctx, loop_redis, stop_event):
        assert loop_ctx is ctx
        assert loop_redis is redis_client
        started.set()
        await stop_event.wait()

    scheduler = ScrapeScheduler(
        ctx=ctx,
        redis_url="redis://example",
        loop_fn=fake_loop,
        redis_factory=lambda _url: redis_client,
    )

    await scheduler.start()
    await asyncio.wait_for(started.wait(), timeout=1)
    await scheduler.stop()

    redis_client.aclose.assert_awaited_once()
    assert scheduler.task is None


@pytest.mark.asyncio
async def test_scrape_scheduler_disabled_does_not_start_loop():
    async def fake_loop(_ctx, _redis, _stop_event):
        raise AssertionError("disabled scheduler should not start")

    scheduler = ScrapeScheduler(
        ctx=object(),
        redis_url="redis://example",
        loop_fn=fake_loop,
        disabled=True,
        redis_factory=Mock(),
    )

    await scheduler.start()

    assert scheduler.task is None


@pytest.mark.asyncio
async def test_scrape_pipeline_scheduler_records_real_exception_metadata():
    stop_event = asyncio.Event()
    pipeline_runs = Mock()
    pipeline_runs.start_stage.return_value = {
        "pipeline_run_id": "run-1",
        "result": {"stages": []},
    }

    async def run_all_scrapers(_ctx, _redis_client):
        raise RuntimeError("scraper backend unavailable")

    async def sleep_and_stop(_duration):
        stop_event.set()

    service = ScrapePipelineService(
        redis_url="redis://example",
        lock_ttl_seconds=60,
        retry_intervals=[1],
        extraction_limit=10,
        embedding_limit=10,
        embedding_max_batches=1,
        batch_stage_timeout_seconds=1,
        scraper_interval_hours=1,
        release_lock_lua="return 1",
        logger=Mock(),
    )

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "services.orchestrator.scrape_pipeline.asyncio.sleep",
            sleep_and_stop,
        )
        await service.run_scheduler_loop(
            object(),
            AsyncMock(),
            stop_event,
            pipeline_runs=pipeline_runs,
            run_all_scrapers_fn=run_all_scrapers,
        )

    pipeline_runs.fail_run.assert_called_once()
    call_kwargs = pipeline_runs.fail_run.call_args.kwargs
    assert call_kwargs["error"] == "scraper backend unavailable"
    assert call_kwargs["retry_eligible"] is True
    assert call_kwargs["metadata"] == {
        "error": "scraper backend unavailable",
        "error_type": "RuntimeError",
    }


@pytest.mark.asyncio
async def test_repair_scheduler_run_once_uses_limits_and_pipeline_runs():
    calls = []
    pipeline_runs = Mock()

    def repair_fn(**kwargs):
        calls.append(kwargs)

    scheduler = RepairScheduler(
        pipeline_runs=pipeline_runs,
        interval_seconds=60,
        extraction_limit=25,
        embedding_limit=30,
        repair_fn=repair_fn,
    )

    await scheduler.run_once()

    assert calls
    assert calls[0]["pipeline_runs"] is pipeline_runs
    assert calls[0]["extraction_limit"] == 25
    assert calls[0]["embedding_limit"] == 30
    assert calls[0]["task_id"].startswith("repair-")


@pytest.mark.asyncio
async def test_resume_etl_orchestrator_sets_state_and_tracks_task():
    async def run_fn(*_args, **_kwargs):
        return None

    task = Mock()
    task.cancelled.return_value = False
    task.exception.return_value = None
    task.add_done_callback = Mock()

    def create_task(coro):
        coro.close()
        return task

    task_registry = set()
    state_writer = Mock()
    pipeline_runs = Mock()
    logger = Mock()
    orchestrator = ResumeEtlOrchestrator(
        run_fn=run_fn,
        task_registry=task_registry,
        state_writer=state_writer,
        now_fn=lambda: "2026-06-20T00:00:00+00:00",
        logger=logger,
        create_task=create_task,
    )

    await orchestrator.start(
        task_id="resume-task",
        file_path="/tmp/resume.pdf",
        upload_id="upload-1",
        owner_id="owner-1",
        resume_fingerprint="fp-1",
        mode="extract_and_embed",
        pipeline_runs=pipeline_runs,
    )

    state_writer.assert_called_once_with(
        "resume-task",
        {
            "status": "running",
            "step": "extracting",
            "upload_id": "upload-1",
            "resume_fingerprint": "fp-1",
        },
        ttl=3600,
    )
    pipeline_runs.start_run.assert_called_once()
    assert task in task_registry

    task.add_done_callback.call_args.args[0](task)

    assert task not in task_registry
    logger.error.assert_not_called()
