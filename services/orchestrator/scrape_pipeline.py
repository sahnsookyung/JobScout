"""Scrape/import pipeline use cases for the orchestrator service."""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, Dict, List, Optional

import redis.asyncio as redis_async

from core.app_context import AppContext
from core.metrics import (
    record_jobs_extraction_queued,
    record_jobs_imported,
)
from core.redis_streams import (
    STREAM_EXTRACTION_BATCH,
    enqueue_job,
)
from services.orchestrator.batch_stage_queue import BatchStageQueueService
from services.orchestrator.pipeline_runs import PipelineRunService

StageRunner = Callable[..., Awaitable[tuple[int, Optional[str]]]]
EmbeddingDrain = Callable[..., Awaitable[tuple[int, List[str], int]]]
ExtractionBackfill = Callable[..., Awaitable[Dict[str, str]]]
RunAllScrapers = Callable[[AppContext, redis_async.Redis], Awaitable[Dict[str, Any]]]
PostScrapePipeline = Callable[..., Awaitable[Dict[str, Any]]]
StateGetter = Callable[..., Awaitable[Any]]
WaitForTaskMessage = Callable[[Any, str], Awaitable[dict]]
CleanupPubsub = Callable[[Any, Any], Awaitable[None]]


class ScrapePipelineService:
    """Application service for scrape/import stage orchestration."""

    def __init__(
        self,
        *,
        redis_url: str,
        lock_ttl_seconds: int,
        retry_intervals: List[int],
        extraction_limit: int,
        embedding_limit: int,
        embedding_max_batches: int,
        batch_stage_timeout_seconds: float,
        scraper_interval_hours: float,
        release_lock_lua: str,
        logger: logging.Logger,
    ) -> None:
        self.redis_url = redis_url
        self.lock_ttl_seconds = lock_ttl_seconds
        self.retry_intervals = retry_intervals
        self.extraction_limit = extraction_limit
        self.embedding_limit = embedding_limit
        self.embedding_max_batches = embedding_max_batches
        self.batch_stage_timeout_seconds = batch_stage_timeout_seconds
        self.scraper_interval_hours = scraper_interval_hours
        self.release_lock_lua = release_lock_lua
        self.logger = logger
        self.batch_stage_queue = BatchStageQueueService(
            redis_url=redis_url,
            batch_stage_timeout_seconds=batch_stage_timeout_seconds,
            logger=logger,
        )

    async def acquire_scraper_lock(
        self,
        redis_client: redis_async.Redis,
        scraper_id: str,
    ) -> Optional[str]:
        lock_key = f"scraper:lock:{scraper_id}"
        owner_id = str(uuid.uuid4())
        acquired = await redis_client.set(
            lock_key,
            owner_id,
            nx=True,
            ex=self.lock_ttl_seconds,
        )
        if acquired:
            self.logger.info("Acquired scraper lock for %s", scraper_id)
        else:
            self.logger.info(
                "Scraper lock for %s held by another instance, skipping",
                scraper_id,
            )
        return owner_id if acquired else None

    async def release_scraper_lock(
        self,
        redis_client: redis_async.Redis,
        lock_key: str,
        owner_id: str,
    ) -> None:
        await redis_client.eval(self.release_lock_lua, 1, lock_key, owner_id)
        self.logger.info("Released scraper lock: %s", lock_key)

    async def update_scraper_status(
        self,
        redis_client: redis_async.Redis,
        scraper_id: str,
        state: str,
        error: str = "",
    ) -> None:
        status_key = f"scraper:status:{scraper_id}"
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        mapping: Dict[str, str] = {"state": state}
        if state == "running":
            mapping["started_at"] = timestamp
            mapping["finished_at"] = ""
        elif state in ("idle", "failed"):
            mapping["finished_at"] = timestamp
            mapping["last_error"] = error
        await redis_client.hset(status_key, mapping=mapping)

    async def wait_for_scrape_with_retry(
        self,
        jobspy_client: Any,
        task_id: str,
        scraper_cfg: Any,
        max_retries: int = 5,
    ) -> List[Dict[str, Any]]:
        for attempt in range(max_retries):
            try:
                request_timeout = getattr(scraper_cfg, "request_timeout", None)
                result = jobspy_client.wait_for_result(
                    task_id,
                    request_timeout_s=request_timeout,
                )
                if result is not None:
                    return result
                return []
            except Exception as exc:
                if attempt == max_retries - 1:
                    self.logger.exception(
                        "Scraper retry exhausted for task %s",
                        task_id,
                    )
                    raise
                interval_index = min(attempt, len(self.retry_intervals) - 1)
                wait_time = self.retry_intervals[interval_index] * random.uniform(0.5, 1.5)
                self.logger.warning(
                    "Scraper attempt %s/%s failed for %s: %s. Retrying in %.1fs...",
                    attempt + 1,
                    max_retries,
                    scraper_cfg.site_type,
                    exc,
                    wait_time,
                )
                await asyncio.sleep(wait_time)
        return []

    async def scrape_single_scraper(
        self,
        ctx: AppContext,
        redis_client: redis_async.Redis,
        scraper_cfg: Any,
    ) -> Dict[str, Any]:
        scraper_id = str(scraper_cfg.site_type[0])
        lock_key = f"scraper:lock:{scraper_id}"
        owner_id = await self.acquire_scraper_lock(redis_client, scraper_id)
        if not owner_id:
            return {
                "scraper_id": scraper_id,
                "jobs_scraped": 0,
                "jobs_imported": 0,
                "ingest_failed": 0,
                "ingest_errors": [],
                "error": "skipped: lock held",
            }

        try:
            await self.update_scraper_status(redis_client, scraper_id, "running")
            task_id = ctx.jobspy_client.submit_scrape(scraper_cfg)
            if not task_id:
                self.logger.warning("No task_id from scraper %s", scraper_id)
                return {
                    "scraper_id": scraper_id,
                    "jobs_scraped": 0,
                    "jobs_imported": 0,
                    "ingest_failed": 0,
                    "ingest_errors": [],
                    "error": "no task_id",
                }

            jobs = await self.wait_for_scrape_with_retry(
                ctx.jobspy_client,
                task_id,
                scraper_cfg,
            )
            jobs_imported = 0
            ingest_errors: List[str] = []
            if jobs:
                from database.uow import job_uow

                for index, job in enumerate(jobs, start=1):
                    try:
                        with job_uow() as repo:
                            ctx.job_etl_service.ingest_one(repo, job, scraper_id)
                        jobs_imported += 1
                    except Exception as exc:
                        ingest_errors.append(f"job {index}: {exc}")
                        self.logger.exception("Ingest failed for %s job %s", scraper_id, index)

            self.logger.info("Scraped %s jobs from %s", len(jobs), scraper_id)
            ingest_failed = len(ingest_errors)
            return {
                "scraper_id": scraper_id,
                "jobs_scraped": len(jobs),
                "jobs_imported": jobs_imported,
                "ingest_failed": ingest_failed,
                "ingest_errors": ingest_errors,
                "error": (
                    f"ingest failed for {ingest_failed} of {len(jobs)} jobs"
                    if ingest_failed
                    else None
                ),
            }
        except Exception as exc:
            self.logger.exception("Scraper %s failed", scraper_id)
            return {
                "scraper_id": scraper_id,
                "jobs_scraped": 0,
                "jobs_imported": 0,
                "ingest_failed": 0,
                "ingest_errors": [],
                "error": str(exc),
            }
        finally:
            await self.release_scraper_lock(redis_client, lock_key, owner_id)
            await self.update_scraper_status(redis_client, scraper_id, "idle", "")

    async def run_all_scrapers(
        self,
        ctx: AppContext,
        redis_client: redis_async.Redis,
    ) -> Dict[str, Any]:
        total_jobs = 0
        total_scraped = 0
        results_by_scraper: List[Dict[str, Any]] = []
        errors: List[str] = []

        for scraper_cfg in ctx.config.scrapers:
            result = await self.scrape_single_scraper(ctx, redis_client, scraper_cfg)
            results_by_scraper.append(result)
            total_scraped += int(result.get("jobs_scraped", 0) or 0)
            total_jobs += int(result.get("jobs_imported", result.get("jobs_scraped", 0)) or 0)
            if result["error"]:
                errors.append(f"{result['scraper_id']}: {result['error']}")

        record_jobs_imported(total_jobs)
        return {
            "total_jobs": total_jobs,
            "total_scraped": total_scraped,
            "results_by_scraper": results_by_scraper,
            "errors": errors,
        }

    def get_downstream_config_errors(self) -> Dict[str, str]:
        return {}

    async def wait_for_next_message(self, pubsub: Any) -> dict:
        """Return the next data message from a Redis pubsub stream."""
        return await self.batch_stage_queue.wait_for_next_message(pubsub)

    async def wait_for_task_message(self, pubsub: Any, task_id: str) -> dict:
        """Return the first completion message matching a task id."""
        return await self.batch_stage_queue.wait_for_task_message(pubsub, task_id)

    async def cleanup_pubsub_and_client(
        self,
        redis_client: Any,
        pubsub: Any,
    ) -> None:
        await self.batch_stage_queue.cleanup_pubsub_and_client(redis_client, pubsub)

    async def run_batch_stage_via_queue(
        self,
        *,
        task_id: str,
        stage: str,
        stream: str,
        completion_channel: str,
        limit: int,
        correlation: Optional[Dict[str, Any]] = None,
        wait_for_task_message: Optional[WaitForTaskMessage] = None,
        cleanup_pubsub_and_client: Optional[CleanupPubsub] = None,
        redis_factory: Callable[..., Any] = redis_async.from_url,
    ) -> tuple[int, Optional[str]]:
        return await self.batch_stage_queue.run_batch_stage_via_queue(
            task_id=task_id,
            stage=stage,
            stream=stream,
            completion_channel=completion_channel,
            limit=limit,
            correlation=correlation,
            wait_for_task_message=wait_for_task_message,
            cleanup_pubsub_and_client=cleanup_pubsub_and_client,
            redis_factory=redis_factory,
        )

    async def run_batch_stage(
        self,
        ctx: AppContext,
        *,
        task_id: str,
        stage: str,
        limit: int,
        correlation: Optional[Dict[str, Any]] = None,
        wait_for_task_message: Optional[WaitForTaskMessage] = None,
        cleanup_pubsub_and_client: Optional[CleanupPubsub] = None,
        redis_factory: Callable[..., Any] = redis_async.from_url,
    ) -> tuple[int, Optional[str]]:
        del ctx
        return await self.batch_stage_queue.run_batch_stage(
            task_id=task_id,
            stage=stage,
            limit=limit,
            correlation=correlation,
            wait_for_task_message=wait_for_task_message,
            cleanup_pubsub_and_client=cleanup_pubsub_and_client,
            redis_factory=redis_factory,
        )

    async def enqueue_best_effort_extraction_backfill(
        self,
        task_id: str,
        *,
        extraction_limit: int,
        embedding_limit: int,
        correlation: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        extraction_task_id = f"{task_id}-extract"
        followup_embedding_task_id = f"{task_id}-post-extract-embed"
        await asyncio.to_thread(
            enqueue_job,
            STREAM_EXTRACTION_BATCH,
            {
                "task_id": extraction_task_id,
                "limit": extraction_limit,
                **(correlation or {}),
                "enqueue_embeddings_batch": {
                    "task_id": followup_embedding_task_id,
                    "limit": embedding_limit,
                    **(correlation or {}),
                },
            },
        )
        record_jobs_extraction_queued(extraction_limit)
        return {
            "extraction_task_id": extraction_task_id,
            "followup_embedding_task_id": followup_embedding_task_id,
        }

    async def run_embedding_stage_until_drained(
        self,
        ctx: AppContext,
        *,
        task_id: str,
        limit: int,
        max_batches: Optional[int] = None,
        run_batch_stage_fn: Optional[StageRunner] = None,
        correlation: Optional[Dict[str, Any]] = None,
    ) -> tuple[int, List[str], int]:
        total_embedded = 0
        errors: List[str] = []
        batches_run = 0
        max_batch_count = max_batches or self.embedding_max_batches
        run_stage = run_batch_stage_fn or self.run_batch_stage

        for batch_index in range(max_batch_count):
            batch_task_id = task_id if batch_index == 0 else f"{task_id}-embed-{batch_index + 1}"
            embedded, embed_error = await run_stage(
                ctx,
                task_id=batch_task_id,
                stage="embed",
                limit=limit,
                correlation=correlation,
            )
            batches_run += 1
            total_embedded += embedded
            if embed_error:
                errors.append(embed_error)
                break
            if embedded <= 0:
                break
        else:
            errors.append(
                f"embedding max batch guard reached after {max_batch_count} batches"
            )

        return total_embedded, errors, batches_run

    async def run_post_scrape_job_pipeline(
        self,
        ctx: AppContext,
        task_id: Optional[str] = None,
        *,
        pipeline_runs: Optional[PipelineRunService] = None,
        run_type: str = "scrape",
        run_embedding_fn: Optional[EmbeddingDrain] = None,
        enqueue_extraction_fn: Optional[ExtractionBackfill] = None,
    ) -> Dict[str, Any]:
        stage_errors: Dict[str, List[str]] = {}
        pipeline_task_id = task_id or f"scrape-batch-{uuid.uuid4().hex[:8]}"
        run_embedding = run_embedding_fn or self.run_embedding_stage_until_drained
        enqueue_extraction = enqueue_extraction_fn or self.enqueue_best_effort_extraction_backfill

        embedding_correlation = await self.start_durable_stage(
            pipeline_runs=pipeline_runs,
            state=None,
            task_id=pipeline_task_id,
            stage="embedding",
            run_type=run_type,
            queued_count=self.embedding_limit,
            metadata={"stage": "embedding", "task_id": pipeline_task_id},
        )
        embedded, embed_errors, embedding_batches = await run_embedding(
            ctx,
            task_id=pipeline_task_id,
            limit=self.embedding_limit,
            correlation=embedding_correlation,
        )
        if embed_errors:
            stage_errors.setdefault("embed", []).extend(embed_errors)
            await self.fail_durable_stage(
                pipeline_runs=pipeline_runs,
                task_id=pipeline_task_id,
                stage="embedding",
                run_type=run_type,
                error="; ".join(embed_errors),
                metadata={"errors": embed_errors, "embedded_count": embedded},
            )
        else:
            await self.complete_durable_stage(
                pipeline_runs=pipeline_runs,
                task_id=pipeline_task_id,
                stage="embedding",
                run_type=run_type,
                processed_count=int(embedded or 0),
                metadata={
                    "embedded_count": embedded,
                    "embedding_batches": embedding_batches,
                },
            )

        extraction_queued = False
        extraction_task_id = None
        followup_embedding_task_id = None
        extraction_correlation = await self.start_durable_stage(
            pipeline_runs=pipeline_runs,
            state=None,
            task_id=pipeline_task_id,
            stage="extraction",
            run_type=run_type,
            queued_count=self.extraction_limit,
            metadata={"stage": "extraction", "task_id": pipeline_task_id},
        )
        try:
            extraction_backfill = await enqueue_extraction(
                pipeline_task_id,
                extraction_limit=self.extraction_limit,
                embedding_limit=self.embedding_limit,
                correlation=extraction_correlation,
            )
            extraction_queued = True
            extraction_task_id = extraction_backfill["extraction_task_id"]
            followup_embedding_task_id = extraction_backfill["followup_embedding_task_id"]
            await self.complete_durable_stage(
                pipeline_runs=pipeline_runs,
                task_id=pipeline_task_id,
                stage="extraction",
                run_type=run_type,
                processed_count=0,
                metadata=extraction_backfill,
            )
        except Exception as exc:
            stage_errors.setdefault("extract_enqueue", []).append(str(exc))
            await self.fail_durable_stage(
                pipeline_runs=pipeline_runs,
                task_id=pipeline_task_id,
                stage="extraction",
                run_type=run_type,
                error=str(exc),
                metadata={"error": str(exc)},
            )

        return {
            "extracted": 0,
            "embedded": embedded,
            "embedding_batches": embedding_batches,
            "extraction_queued": extraction_queued,
            "extraction_task_id": extraction_task_id,
            "followup_embedding_task_id": followup_embedding_task_id,
            "stage_errors": stage_errors,
        }

    async def run_scheduler_loop(
        self,
        ctx: AppContext,
        redis_client: redis_async.Redis,
        stop_event: asyncio.Event,
        *,
        pipeline_runs: Optional[PipelineRunService] = None,
        run_all_scrapers_fn: Optional[RunAllScrapers] = None,
        run_post_scrape_pipeline_fn: Optional[PostScrapePipeline] = None,
    ) -> None:
        self.logger.info(
            "Scraper scheduler started (interval: %sh)",
            self.scraper_interval_hours,
        )
        run_all = run_all_scrapers_fn or self.run_all_scrapers
        run_post_scrape = run_post_scrape_pipeline_fn or self.run_post_scrape_job_pipeline

        while not stop_event.is_set():
            interval_seconds = self.scraper_interval_hours * 3600
            cycle_task_id = f"scheduled-scrape-{uuid.uuid4().hex[:8]}"
            try:
                self.logger.info("Starting scheduled scrape cycle")
                await self.start_durable_stage(
                    pipeline_runs=pipeline_runs,
                    state=None,
                    task_id=cycle_task_id,
                    stage="scrape",
                    run_type="scrape",
                    queued_count=self._scraper_count(ctx),
                    metadata={"stage": "scrape", "task_id": cycle_task_id},
                )
                result = await run_all(ctx, redis_client)
                if result["errors"]:
                    await self.fail_durable_stage(
                        pipeline_runs=pipeline_runs,
                        task_id=cycle_task_id,
                        stage="scrape",
                        run_type="scrape",
                        error="; ".join(result["errors"]),
                        metadata=result,
                    )
                    self.logger.warning(
                        "Scheduled scrape completed with errors: %s",
                        result["errors"],
                    )
                else:
                    await self.complete_durable_stage(
                        pipeline_runs=pipeline_runs,
                        task_id=cycle_task_id,
                        stage="scrape",
                        run_type="scrape",
                        processed_count=int(result["total_jobs"] or 0),
                        metadata=result,
                    )
                    self.logger.info(
                        "Scheduled scrape completed: %s scraped, %s imported from %s scrapers",
                        result.get("total_scraped", result["total_jobs"]),
                        result["total_jobs"],
                        len(result["results_by_scraper"]),
                    )

                if run_post_scrape_pipeline_fn is None:
                    pipeline_result = await self.run_post_scrape_job_pipeline(
                        ctx,
                        task_id=cycle_task_id,
                        pipeline_runs=pipeline_runs,
                        run_type="scrape",
                    )
                else:
                    pipeline_result = await run_post_scrape(ctx)
                self.logger.info(
                    "Scheduled scrape post-processing complete: jobs_imported=%d jobs_processed=%d extracted=%d embedded=%d extraction_queued=%s",
                    result["total_jobs"],
                    pipeline_result["embedded"],
                    pipeline_result["extracted"],
                    pipeline_result["embedded"],
                    pipeline_result.get("extraction_queued", False),
                )
                if pipeline_result["stage_errors"]:
                    self.logger.warning(
                        "Scheduled post-processing stage errors: %s",
                        pipeline_result["stage_errors"],
                    )
                if pipeline_runs is not None:
                    if result["errors"] or pipeline_result["stage_errors"]:
                        await asyncio.to_thread(
                            pipeline_runs.fail_run,
                            task_id=cycle_task_id,
                            run_type="scrape",
                            error="; ".join(result["errors"])
                            or str(pipeline_result["stage_errors"]),
                            retry_eligible=True,
                            metadata={
                                "scrape": result,
                                "post_scrape": pipeline_result,
                            },
                        )
                    else:
                        await asyncio.to_thread(
                            pipeline_runs.complete_run,
                            task_id=cycle_task_id,
                            run_type="scrape",
                            metadata={
                                "scrape": result,
                                "post_scrape": pipeline_result,
                            },
                        )
            except Exception as exc:
                self.logger.exception("Scheduled scrape failed")
                if pipeline_runs is not None:
                    error_message = str(exc) or "Scheduled scrape failed"
                    await asyncio.to_thread(
                        pipeline_runs.fail_run,
                        task_id=cycle_task_id,
                        run_type="scrape",
                        error=error_message,
                        retry_eligible=True,
                        metadata={
                            "error": error_message,
                            "error_type": type(exc).__name__,
                        },
                    )

            await asyncio.sleep(interval_seconds)

        self.logger.info("Scraper scheduler stopped")

    @staticmethod
    def _pipeline_stage_id(snapshot: Dict[str, Any], stage: str) -> Optional[str]:
        for stage_snapshot in snapshot.get("result", {}).get("stages", []):
            if stage_snapshot.get("stage") == stage:
                return stage_snapshot.get("id")
        return None

    @staticmethod
    def _scraper_count(ctx: AppContext) -> int:
        scrapers = getattr(getattr(ctx, "config", None), "scrapers", None)
        try:
            return len(scrapers or [])
        except TypeError:
            return 0

    async def start_durable_stage(
        self,
        *,
        pipeline_runs: Optional[PipelineRunService],
        state: Optional[Any],
        task_id: str,
        stage: str,
        run_type: str,
        queued_count: int,
        metadata: Dict[str, Any],
    ) -> Dict[str, str]:
        if pipeline_runs is None:
            return {}
        snapshot = await asyncio.to_thread(
            pipeline_runs.start_stage,
            task_id=task_id,
            stage=stage,
            run_type=run_type,
            queued_count=queued_count,
            metadata=metadata,
        )
        correlation = {
            "pipeline_run_id": snapshot.get("pipeline_run_id"),
            "pipeline_stage_id": self._pipeline_stage_id(snapshot, stage),
        }
        if state is not None:
            state.result = dict(state.result or {})
            for key, value in correlation.items():
                if value:
                    state.result[key] = str(value)
            await state._save_to_redis()
        return {key: str(value) for key, value in correlation.items() if value}

    async def complete_durable_stage(
        self,
        *,
        pipeline_runs: Optional[PipelineRunService],
        task_id: str,
        stage: str,
        run_type: str,
        processed_count: int,
        failed_count: int = 0,
        skipped_count: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if pipeline_runs is None:
            return
        await asyncio.to_thread(
            pipeline_runs.complete_stage,
            task_id=task_id,
            stage=stage,
            run_type=run_type,
            processed_count=processed_count,
            failed_count=failed_count,
            skipped_count=skipped_count,
            metadata=metadata or {},
        )

    async def fail_durable_stage(
        self,
        *,
        pipeline_runs: Optional[PipelineRunService],
        task_id: str,
        stage: str,
        run_type: str,
        error: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if pipeline_runs is None:
            return
        await asyncio.to_thread(
            pipeline_runs.fail_stage,
            task_id=task_id,
            stage=stage,
            run_type=run_type,
            error=error,
            retry_eligible=True,
            metadata=metadata or {},
        )

    async def run_stage_task(
        self,
        *,
        task_id: str,
        registry: Any,
        ctx: AppContext,
        stage: str,
        limit: int,
        state_getter: StateGetter,
        pipeline_runs: Optional[PipelineRunService] = None,
        run_all_scrapers_fn: Optional[RunAllScrapers] = None,
        run_batch_stage_fn: Optional[StageRunner] = None,
        redis_factory: Callable[..., Any] = redis_async.from_url,
    ) -> None:
        async with registry.lock:
            registry.active_task_ids.add(task_id)

        state = await state_getter(registry, task_id)
        state.task_type = "stage"
        state.current_stage = stage
        state.status = "running"
        state.result = {"stage": stage, "limit": limit}
        await state._save_to_redis()
        if pipeline_runs is not None:
            snapshot = await asyncio.to_thread(
                pipeline_runs.start_stage,
                task_id=task_id,
                stage=stage,
                run_type="stage",
                queued_count=limit,
                metadata=state.result,
            )
            state.result["pipeline_run_id"] = snapshot.get("pipeline_run_id")
            for stage_snapshot in snapshot.get("result", {}).get("stages", []):
                if stage_snapshot.get("stage") == stage:
                    state.result["pipeline_stage_id"] = stage_snapshot.get("id")
                    break
            await state._save_to_redis()
        await state.notify(
            {
                "task_id": task_id,
                "status": "running",
                "current_stage": stage,
                "message": f"Starting {stage} stage",
            }
        )

        redis_client = None
        try:
            if stage == "scrape":
                redis_client = redis_factory(self.redis_url)
                run_all = run_all_scrapers_fn or self.run_all_scrapers
                scrape_result = await run_all(ctx, redis_client)
                state.result = {
                    "stage": stage,
                    "scraped_jobs": scrape_result["total_jobs"],
                    "jobs_imported": scrape_result["total_jobs"],
                    "jobs_processed": 0,
                    "scrapers": scrape_result["results_by_scraper"],
                    "errors": scrape_result["errors"],
                }
                if scrape_result["errors"]:
                    raise RuntimeError("; ".join(scrape_result["errors"]))
            elif stage in {"extract", "embed"}:
                run_stage = run_batch_stage_fn or self.run_batch_stage
                processed, error = await run_stage(
                    ctx,
                    task_id=task_id,
                    stage=stage,
                    limit=limit,
                    correlation={
                        key: value
                        for key, value in {
                            "pipeline_run_id": state.result.get("pipeline_run_id"),
                            "pipeline_stage_id": state.result.get("pipeline_stage_id"),
                        }.items()
                        if value
                    },
                )
                state.result = {"stage": stage, "processed": processed, "limit": limit}
                if error:
                    raise RuntimeError(error)
            else:
                raise RuntimeError(f"Unsupported stage: {stage}")

            state.status = "completed"
            await state._save_to_redis()
            if pipeline_runs is not None:
                processed_count = int(
                    state.result.get("processed") or state.result.get("jobs_imported") or 0
                )
                await asyncio.to_thread(
                    pipeline_runs.complete_stage,
                    task_id=task_id,
                    stage=stage,
                    run_type="stage",
                    processed_count=processed_count,
                    metadata=state.result,
                )
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "completed",
                    "current_stage": stage,
                    "result": state.result,
                }
            )
        except Exception as exc:
            state.status = "failed"
            state.error = str(exc)
            await state._save_to_redis()
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.fail_stage,
                    task_id=task_id,
                    stage=stage,
                    run_type="stage",
                    error=state.error,
                    retry_eligible=True,
                    metadata=state.result,
                )
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "failed",
                    "current_stage": stage,
                    "error": state.error,
                    "result": state.result,
                }
            )
        finally:
            if redis_client is not None:
                await redis_client.aclose()
            async with registry.lock:
                registry.active_task_ids.discard(task_id)
            await state.close(registry)

    async def run_scrape_extract_embed_pipeline_task(
        self,
        *,
        task_id: str,
        registry: Any,
        ctx: AppContext,
        state_getter: StateGetter,
        pipeline_runs: Optional[PipelineRunService] = None,
        run_all_scrapers_fn: Optional[RunAllScrapers] = None,
        run_embedding_fn: Optional[EmbeddingDrain] = None,
        enqueue_extraction_fn: Optional[ExtractionBackfill] = None,
        redis_factory: Callable[..., Any] = redis_async.from_url,
    ) -> None:
        async with registry.lock:
            registry.active_task_ids.add(task_id)

        state = await state_getter(registry, task_id)
        existing_result = dict(state.result or {})
        state.task_type = "pipeline"
        state.status = "running"
        state.result = {
            "scraped_jobs": 0,
            "jobs_imported": 0,
            "jobs_processed": 0,
            "scrapers": [],
            "errors": [],
            "extracted_count": 0,
            "embedded_count": 0,
            "embedding_batches": 0,
            "extraction_enqueued": False,
            "extraction_task_id": None,
            "followup_embedding_task_id": None,
            "stage_errors": {},
        }
        if existing_result.get("pipeline_run_id"):
            state.result["pipeline_run_id"] = existing_result["pipeline_run_id"]
        await state._save_to_redis()

        redis_client = redis_factory(self.redis_url)
        run_all = run_all_scrapers_fn or self.run_all_scrapers
        run_embedding = run_embedding_fn or self.run_embedding_stage_until_drained
        enqueue_extraction = enqueue_extraction_fn or self.enqueue_best_effort_extraction_backfill
        try:
            state.current_stage = "scrape"
            await state._save_to_redis()
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "running",
                    "current_stage": "scrape",
                    "message": "Starting scrape stage",
                }
            )
            await self.start_durable_stage(
                pipeline_runs=pipeline_runs,
                state=state,
                task_id=task_id,
                stage="scrape",
                run_type="pipeline",
                queued_count=self._scraper_count(ctx),
                metadata={"stage": "scrape", "task_id": task_id},
            )
            scrape_result = await run_all(ctx, redis_client)
            state.result["scraped_jobs"] = scrape_result.get(
                "total_scraped",
                scrape_result["total_jobs"],
            )
            state.result["jobs_imported"] = scrape_result["total_jobs"]
            state.result["scrapers"] = scrape_result["results_by_scraper"]
            if scrape_result["errors"]:
                state.result["errors"] = list(scrape_result["errors"])
                state.result["stage_errors"]["scrape"] = list(scrape_result["errors"])
                await self.fail_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="scrape",
                    run_type="pipeline",
                    error="; ".join(scrape_result["errors"]),
                    metadata=scrape_result,
                )
            else:
                await self.complete_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="scrape",
                    run_type="pipeline",
                    processed_count=int(scrape_result["total_jobs"] or 0),
                    metadata=scrape_result,
                )

            state.current_stage = "embed"
            await state._save_to_redis()
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "running",
                    "current_stage": "embed",
                    "message": "Embedding scraped jobs",
                }
            )
            embedding_correlation = await self.start_durable_stage(
                pipeline_runs=pipeline_runs,
                state=state,
                task_id=task_id,
                stage="embedding",
                run_type="pipeline",
                queued_count=self.embedding_limit,
                metadata={"stage": "embedding", "task_id": task_id},
            )
            embedded, embed_errors, embedding_batches = await run_embedding(
                ctx,
                task_id=task_id,
                limit=self.embedding_limit,
                correlation=embedding_correlation,
            )
            state.result["embedded_count"] = embedded
            state.result["jobs_processed"] = embedded
            state.result["embedding_batches"] = embedding_batches
            if embed_errors:
                state.result["stage_errors"].setdefault("embed", []).extend(embed_errors)
                await self.fail_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="embedding",
                    run_type="pipeline",
                    error="; ".join(embed_errors),
                    metadata={"errors": embed_errors, "embedded_count": embedded},
                )
            else:
                await self.complete_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="embedding",
                    run_type="pipeline",
                    processed_count=int(embedded or 0),
                    metadata={
                        "embedded_count": embedded,
                        "embedding_batches": embedding_batches,
                    },
                )

            state.current_stage = "extract"
            await state._save_to_redis()
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "running",
                    "current_stage": "extract",
                    "message": "Queueing extraction enrichment",
                }
            )
            extraction_correlation = await self.start_durable_stage(
                pipeline_runs=pipeline_runs,
                state=state,
                task_id=task_id,
                stage="extraction",
                run_type="pipeline",
                queued_count=self.extraction_limit,
                metadata={"stage": "extraction", "task_id": task_id},
            )
            try:
                extraction_backfill = await enqueue_extraction(
                    task_id,
                    extraction_limit=self.extraction_limit,
                    embedding_limit=self.embedding_limit,
                    correlation=extraction_correlation,
                )
                state.result["extraction_enqueued"] = True
                state.result["extraction_task_id"] = extraction_backfill["extraction_task_id"]
                state.result["followup_embedding_task_id"] = extraction_backfill[
                    "followup_embedding_task_id"
                ]
                await self.complete_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="extraction",
                    run_type="pipeline",
                    processed_count=0,
                    metadata=extraction_backfill,
                )
            except Exception as exc:
                state.result["stage_errors"].setdefault("extract_enqueue", []).append(str(exc))
                await self.fail_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="extraction",
                    run_type="pipeline",
                    error=str(exc),
                    metadata={"error": str(exc)},
                )

            flat_errors = []
            for errors in state.result["stage_errors"].values():
                flat_errors.extend(errors)
            state.result["errors"] = flat_errors

            critical_errors = []
            for stage_name in ("scrape", "embed"):
                critical_errors.extend(state.result["stage_errors"].get(stage_name, []))
            if critical_errors:
                raise RuntimeError("; ".join(critical_errors))

            state.status = "completed"
            await state._save_to_redis()
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.complete_run,
                    task_id=task_id,
                    run_type="pipeline",
                    metadata=state.result,
                )
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "completed",
                    "current_stage": state.current_stage,
                    "result": state.result,
                }
            )
        except Exception as exc:
            state.status = "failed"
            state.error = str(exc)
            await state._save_to_redis()
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.fail_run,
                    task_id=task_id,
                    run_type="pipeline",
                    error=state.error,
                    retry_eligible=True,
                    metadata=state.result,
                )
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "failed",
                    "current_stage": state.current_stage,
                    "error": state.error,
                    "result": state.result,
                }
            )
        finally:
            await redis_client.aclose()
            async with registry.lock:
                registry.active_task_ids.discard(task_id)
            await state.close(registry)

    async def run_process_imported_jobs_pipeline_task(
        self,
        *,
        task_id: str,
        registry: Any,
        ctx: AppContext,
        state_getter: StateGetter,
        pipeline_runs: Optional[PipelineRunService] = None,
        run_embedding_fn: Optional[EmbeddingDrain] = None,
        enqueue_extraction_fn: Optional[ExtractionBackfill] = None,
    ) -> None:
        async with registry.lock:
            registry.active_task_ids.add(task_id)

        state = await state_getter(registry, task_id)
        existing_result = dict(state.result or {})
        state.task_type = "pipeline"
        state.status = "running"
        state.result = {
            "extracted_count": 0,
            "embedded_count": 0,
            "jobs_processed": 0,
            "embedding_batches": 0,
            "extraction_enqueued": False,
            "extraction_task_id": None,
            "followup_embedding_task_id": None,
            "stage_errors": {},
            "errors": [],
        }
        if existing_result.get("pipeline_run_id"):
            state.result["pipeline_run_id"] = existing_result["pipeline_run_id"]
        await state._save_to_redis()

        run_embedding = run_embedding_fn or self.run_embedding_stage_until_drained
        enqueue_extraction = enqueue_extraction_fn or self.enqueue_best_effort_extraction_backfill
        try:
            state.current_stage = "embed"
            await state._save_to_redis()
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "running",
                    "current_stage": "embed",
                    "message": "Processing imported jobs: embeddings",
                }
            )
            embedding_correlation = await self.start_durable_stage(
                pipeline_runs=pipeline_runs,
                state=state,
                task_id=task_id,
                stage="embedding",
                run_type="pipeline",
                queued_count=self.embedding_limit,
                metadata={"stage": "embedding", "task_id": task_id},
            )
            embedded, embed_errors, embedding_batches = await run_embedding(
                ctx,
                task_id=task_id,
                limit=self.embedding_limit,
                correlation=embedding_correlation,
            )
            state.result["embedded_count"] = embedded
            state.result["jobs_processed"] = embedded
            state.result["embedding_batches"] = embedding_batches
            if embed_errors:
                state.result["stage_errors"].setdefault("embed", []).extend(embed_errors)
                await self.fail_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="embedding",
                    run_type="pipeline",
                    error="; ".join(embed_errors),
                    metadata={"errors": embed_errors, "embedded_count": embedded},
                )
            else:
                await self.complete_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="embedding",
                    run_type="pipeline",
                    processed_count=int(embedded or 0),
                    metadata={
                        "embedded_count": embedded,
                        "embedding_batches": embedding_batches,
                    },
                )

            state.current_stage = "extract"
            await state._save_to_redis()
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "running",
                    "current_stage": "extract",
                    "message": "Queueing imported job extraction enrichment",
                }
            )
            extraction_correlation = await self.start_durable_stage(
                pipeline_runs=pipeline_runs,
                state=state,
                task_id=task_id,
                stage="extraction",
                run_type="pipeline",
                queued_count=self.extraction_limit,
                metadata={"stage": "extraction", "task_id": task_id},
            )
            try:
                extraction_backfill = await enqueue_extraction(
                    task_id,
                    extraction_limit=self.extraction_limit,
                    embedding_limit=self.embedding_limit,
                    correlation=extraction_correlation,
                )
                state.result["extraction_enqueued"] = True
                state.result["extraction_task_id"] = extraction_backfill["extraction_task_id"]
                state.result["followup_embedding_task_id"] = extraction_backfill[
                    "followup_embedding_task_id"
                ]
                await self.complete_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="extraction",
                    run_type="pipeline",
                    processed_count=0,
                    metadata=extraction_backfill,
                )
            except Exception as exc:
                state.result["stage_errors"].setdefault("extract_enqueue", []).append(str(exc))
                await self.fail_durable_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="extraction",
                    run_type="pipeline",
                    error=str(exc),
                    metadata={"error": str(exc)},
                )

            flat_errors = []
            for errors in state.result["stage_errors"].values():
                flat_errors.extend(errors)
            state.result["errors"] = flat_errors
            if embed_errors:
                raise RuntimeError("; ".join(embed_errors))

            state.status = "completed"
            await state._save_to_redis()
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.complete_run,
                    task_id=task_id,
                    run_type="pipeline",
                    metadata=state.result,
                )
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "completed",
                    "current_stage": state.current_stage,
                    "result": state.result,
                }
            )
        except Exception as exc:
            state.status = "failed"
            state.error = str(exc)
            await state._save_to_redis()
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.fail_run,
                    task_id=task_id,
                    run_type="pipeline",
                    error=state.error,
                    retry_eligible=True,
                    metadata=state.result,
                )
            await state.notify(
                {
                    "task_id": task_id,
                    "status": "failed",
                    "current_stage": state.current_stage,
                    "error": state.error,
                    "result": state.result,
                }
            )
        finally:
            async with registry.lock:
                registry.active_task_ids.discard(task_id)
            await state.close(registry)

    async def run_manual_scrape(
        self,
        *,
        ctx: AppContext,
        redis_client: redis_async.Redis,
        pipeline_runs: Optional[PipelineRunService] = None,
        run_all_scrapers_fn: Optional[RunAllScrapers] = None,
        run_post_scrape_pipeline_fn: Optional[PostScrapePipeline] = None,
    ) -> Dict[str, Any]:
        run_all = run_all_scrapers_fn or self.run_all_scrapers
        run_post_scrape = run_post_scrape_pipeline_fn or self.run_post_scrape_job_pipeline
        manual_task_id = f"manual-scrape-{uuid.uuid4().hex[:8]}"
        await self.start_durable_stage(
            pipeline_runs=pipeline_runs,
            state=None,
            task_id=manual_task_id,
            stage="scrape",
            run_type="scrape",
            queued_count=self._scraper_count(ctx),
            metadata={"stage": "scrape", "task_id": manual_task_id},
        )
        result = await run_all(ctx, redis_client)
        if result["errors"]:
            await self.fail_durable_stage(
                pipeline_runs=pipeline_runs,
                task_id=manual_task_id,
                stage="scrape",
                run_type="scrape",
                error="; ".join(result["errors"]),
                metadata=result,
            )
        else:
            await self.complete_durable_stage(
                pipeline_runs=pipeline_runs,
                task_id=manual_task_id,
                stage="scrape",
                run_type="scrape",
                processed_count=int(result["total_jobs"] or 0),
                metadata=result,
            )

        if run_post_scrape_pipeline_fn is None:
            pipeline_result = await self.run_post_scrape_job_pipeline(
                ctx,
                task_id=manual_task_id,
                pipeline_runs=pipeline_runs,
                run_type="scrape",
            )
        elif pipeline_runs is not None:
            pipeline_result = await run_post_scrape(
                ctx,
                manual_task_id,
                pipeline_runs=pipeline_runs,
            )
        else:
            pipeline_result = await run_post_scrape(ctx)

        stage_errors: Dict[str, List[str]] = {}
        if result["errors"]:
            stage_errors["scrape"] = list(result["errors"])
        for stage, errors in pipeline_result["stage_errors"].items():
            stage_errors.setdefault(stage, []).extend(errors)

        flat_errors = [err for errors in stage_errors.values() for err in errors]
        if pipeline_runs is not None:
            if flat_errors:
                await asyncio.to_thread(
                    pipeline_runs.fail_run,
                    task_id=manual_task_id,
                    run_type="scrape",
                    error="; ".join(flat_errors),
                    retry_eligible=True,
                    metadata={
                        "scrape": result,
                        "post_scrape": pipeline_result,
                    },
                )
            else:
                await asyncio.to_thread(
                    pipeline_runs.complete_run,
                    task_id=manual_task_id,
                    run_type="scrape",
                    metadata={
                        "scrape": result,
                        "post_scrape": pipeline_result,
                    },
                )
        return {
            "success": len(flat_errors) == 0,
            "task_id": manual_task_id,
            "total_jobs": result["total_jobs"],
            "total_scraped": result.get("total_scraped", result["total_jobs"]),
            "scrapers": result["results_by_scraper"],
            "errors": flat_errors,
            "scraped_jobs": result.get("total_scraped", result["total_jobs"]),
            "jobs_imported": result["total_jobs"],
            "jobs_processed": pipeline_result["embedded"],
            "extracted_count": pipeline_result["extracted"],
            "embedded_count": pipeline_result["embedded"],
            "stage_errors": stage_errors,
            "message": (
                f"Scraped {result.get('total_scraped', result['total_jobs'])} jobs "
                f"and imported {result['total_jobs']} from "
                f"{len([s for s in result['results_by_scraper'] if not s.get('error')])} scrapers; "
                f"extracted={pipeline_result['extracted']}, embedded={pipeline_result['embedded']}, "
                f"stage_errors={len(flat_errors)}"
            ),
        }
