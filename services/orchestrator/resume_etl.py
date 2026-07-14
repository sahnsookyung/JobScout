"""Resume ETL use case orchestration."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, MutableSet
from typing import Any, Optional

import redis.asyncio as redis_async

from core.redis_streams import (
    CHANNEL_EMBEDDINGS_DONE,
    CHANNEL_EXTRACTION_DONE,
    STREAM_EMBEDDINGS,
    STREAM_EXTRACTION,
    enqueue_job,
)
from services.orchestrator.pipeline_runs import PipelineRunService

ResumeEtlRunFn = Callable[..., Awaitable[None]]
StateWriter = Callable[..., None]
NowFn = Callable[[], str]
CreateTaskFn = Callable[[Awaitable[None]], asyncio.Task]
WaitForTaskMessage = Callable[[Any, str], Awaitable[dict]]
CleanupPubsub = Callable[[Any, Any], Awaitable[None]]
EnqueueJobFn = Callable[[str, dict[str, Any]], str]


class ResumeEtlOrchestrator:
    """Start and track resume ETL work without binding that logic to a route."""

    def __init__(
        self,
        *,
        run_fn: ResumeEtlRunFn,
        task_registry: MutableSet[asyncio.Task],
        state_writer: StateWriter,
        now_fn: NowFn,
        logger: logging.Logger,
        create_task: Optional[CreateTaskFn] = None,
    ) -> None:
        self._run_fn = run_fn
        self._task_registry = task_registry
        self._state_writer = state_writer
        self._now_fn = now_fn
        self._logger = logger
        self._create_task = create_task

    async def start(
        self,
        *,
        task_id: str,
        file_path: Optional[str],
        upload_id: Optional[str],
        owner_id: str,
        resume_fingerprint: Optional[str],
        mode: str,
        pipeline_runs: Optional[PipelineRunService],
        tenant_id: Optional[str] = None,
    ) -> asyncio.Task:
        initial_step = "embedding" if mode == "embed_only" else "extracting"
        initial_state: dict[str, Any] = {"status": "running", "step": initial_step}
        if upload_id:
            initial_state["upload_id"] = upload_id
        if resume_fingerprint:
            initial_state["resume_fingerprint"] = resume_fingerprint

        run_metadata = {
            **initial_state,
            "phase": f"{initial_step}_resume"
            if initial_step != "embedding"
            else "embedding_resume",
            "task_type": "resume_upload",
            "owner_id": owner_id,
            "updated_at": self._now_fn(),
        }
        if pipeline_runs is not None:
            await asyncio.to_thread(
                pipeline_runs.start_run,
                task_id=task_id,
                run_type="resume_upload",
                owner_id=owner_id,
                tenant_id=tenant_id,
                resume_fingerprint=resume_fingerprint,
                current_stage=initial_step,
                metadata=run_metadata,
            )

        self._state_writer(task_id, initial_state, ttl=3600)
        create_task = self._create_task or asyncio.create_task
        etl_task = create_task(
            self._run_fn(
                task_id,
                file_path,
                upload_id=upload_id,
                owner_id=owner_id,
                tenant_id=tenant_id,
                resume_fingerprint=resume_fingerprint,
                mode=mode,
                pipeline_runs=pipeline_runs,
            )
        )
        self._task_registry.add(etl_task)
        etl_task.add_done_callback(self._done_callback)
        return etl_task

    def _done_callback(self, task: asyncio.Task) -> None:
        self._task_registry.discard(task)
        if task.cancelled():
            return
        if task.exception() is not None:
            self._logger.error(
                "_run_resume_etl background task raised an unhandled exception"
            )


class ResumeEtlPipelineService:
    """Run resume extraction and embedding stream stages for one upload."""

    def __init__(
        self,
        *,
        redis_url: str,
        listener_timeout: float,
        wait_for_task_message: WaitForTaskMessage,
        cleanup_pubsub_and_client: CleanupPubsub,
        state_writer: StateWriter,
        now_fn: NowFn,
        logger: logging.Logger,
        redis_factory: Callable[..., Any] = redis_async.from_url,
        enqueue_job_fn: EnqueueJobFn = enqueue_job,
    ) -> None:
        self.redis_url = redis_url
        self.listener_timeout = listener_timeout
        self.wait_for_task_message = wait_for_task_message
        self.cleanup_pubsub_and_client = cleanup_pubsub_and_client
        self.state_writer = state_writer
        self.now_fn = now_fn
        self.logger = logger
        self.redis_factory = redis_factory
        self.enqueue_job = enqueue_job_fn

    async def _record_stage_start(
        self,
        *,
        pipeline_runs: Optional[PipelineRunService],
        task_id: str,
        stage: str,
        metadata: dict[str, Any],
    ) -> dict[str, str]:
        if pipeline_runs is None:
            return {}
        snapshot = await asyncio.to_thread(
            pipeline_runs.start_stage,
            task_id=task_id,
            stage=stage,
            run_type="resume_upload",
            queued_count=1,
            metadata=metadata,
        )
        correlation = {"pipeline_run_id": snapshot.get("pipeline_run_id")}
        for stage_snapshot in snapshot.get("result", {}).get("stages", []):
            if stage_snapshot.get("stage") == stage:
                correlation["pipeline_stage_id"] = stage_snapshot.get("id")
                break
        return {key: str(value) for key, value in correlation.items() if value}

    async def _record_stage_complete(
        self,
        *,
        pipeline_runs: Optional[PipelineRunService],
        task_id: str,
        stage: str,
        metadata: dict[str, Any],
    ) -> None:
        if pipeline_runs is None:
            return
        await asyncio.to_thread(
            pipeline_runs.complete_stage,
            task_id=task_id,
            stage=stage,
            run_type="resume_upload",
            processed_count=1,
            metadata=metadata,
        )

    async def _record_stage_failure(
        self,
        *,
        pipeline_runs: Optional[PipelineRunService],
        task_id: str,
        stage: str,
        error: str,
        metadata: dict[str, Any],
    ) -> None:
        if pipeline_runs is None:
            return
        await asyncio.to_thread(
            pipeline_runs.fail_stage,
            task_id=task_id,
            stage=stage,
            run_type="resume_upload",
            error=error,
            retry_eligible=True,
            metadata=metadata,
        )

    def _state(
        self,
        *,
        status: str,
        phase: str,
        upload_id: Optional[str],
        owner_id: str,
        tenant_id: Optional[str] = None,
        resume_fingerprint: Optional[str] = None,
        step: Optional[str] = None,
        error: Optional[str] = None,
    ) -> dict[str, Any]:
        state: dict[str, Any] = {
            "status": status,
            "phase": phase,
            "task_type": "resume_upload",
            "upload_id": upload_id,
            "owner_id": owner_id,
            "updated_at": self.now_fn(),
        }
        if step:
            state["step"] = step
        if resume_fingerprint:
            state["resume_fingerprint"] = resume_fingerprint
        if error:
            state["error"] = error
        return state

    def _write_state(self, task_id: str, state: dict[str, Any]) -> None:
        self.state_writer(task_id, state, ttl=3600)

    async def _fail_stage(
        self,
        *,
        pipeline_runs: Optional[PipelineRunService],
        task_id: str,
        stage: str,
        error: str,
        state: dict[str, Any],
    ) -> None:
        await self._record_stage_failure(
            pipeline_runs=pipeline_runs,
            task_id=task_id,
            stage=stage,
            error=error,
            metadata=state,
        )
        self._write_state(task_id, state)

    async def run(
        self,
        task_id: str,
        file_path: Optional[str],
        *,
        upload_id: Optional[str] = None,
        owner_id: str,
        tenant_id: Optional[str] = None,
        resume_fingerprint: Optional[str] = None,
        mode: str = "extract_and_embed",
        pipeline_runs: Optional[PipelineRunService] = None,
    ) -> None:
        redis_client = None
        pubsub = None

        try:
            redis_client = self.redis_factory(self.redis_url, decode_responses=True)
            pubsub = redis_client.pubsub()

            fingerprint = resume_fingerprint
            if mode != "embed_only":
                extraction_state = self._state(
                    status="running",
                    step="extracting",
                    phase="extracting_resume",
                    upload_id=upload_id,
                    owner_id=owner_id,
                    resume_fingerprint=resume_fingerprint,
                )
                extraction_correlation = await self._record_stage_start(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="resume_extraction",
                    metadata=extraction_state,
                )
                await pubsub.subscribe(CHANNEL_EXTRACTION_DONE)
                await asyncio.to_thread(
                    self.enqueue_job,
                    STREAM_EXTRACTION,
                    {
                        "task_id": task_id,
                        "resume_file": file_path,
                        "known_fingerprint": resume_fingerprint,
                        "resume_upload_id": upload_id,
                        "owner_id": owner_id,
                        "tenant_id": tenant_id,
                        **extraction_correlation,
                    },
                )
                self.logger.info("Enqueued extraction stage for resume ETL")

                async with asyncio.timeout(self.listener_timeout):
                    extraction_data = await self.wait_for_task_message(pubsub, task_id)

                await pubsub.unsubscribe(CHANNEL_EXTRACTION_DONE)

                extraction_status = (extraction_data or {}).get("status")
                if extraction_status not in {"completed", "skipped"}:
                    err = (extraction_data or {}).get("error", "Extraction failed")
                    self.logger.error(
                        "Resume extraction stage failed with status=%s",
                        extraction_status,
                    )
                    failure_state = self._state(
                        status="failed",
                        step="extracting",
                        phase="extracting_resume",
                        upload_id=upload_id,
                        owner_id=owner_id,
                        resume_fingerprint=resume_fingerprint,
                        error=err,
                    )
                    await self._fail_stage(
                        pipeline_runs=pipeline_runs,
                        task_id=task_id,
                        stage="resume_extraction",
                        error=err,
                        state=failure_state,
                    )
                    return

                fingerprint = extraction_data.get("resume_fingerprint")
                if not fingerprint:
                    self.logger.error("Extraction stage returned no resume fingerprint")
                    error = "No fingerprint in extraction response"
                    failure_state = self._state(
                        status="failed",
                        step="extracting",
                        phase="extracting_resume",
                        upload_id=upload_id,
                        owner_id=owner_id,
                        error=error,
                    )
                    await self._fail_stage(
                        pipeline_runs=pipeline_runs,
                        task_id=task_id,
                        stage="resume_extraction",
                        error=error,
                        state=failure_state,
                    )
                    return

                self.logger.info("Extraction stage completed")
                running_state = self._state(
                    status="running",
                    step="embedding",
                    phase="embedding_resume",
                    upload_id=upload_id,
                    owner_id=owner_id,
                    resume_fingerprint=fingerprint,
                )
                await self._record_stage_complete(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="resume_extraction",
                    metadata=running_state,
                )
                self._write_state(task_id, running_state)
            elif not fingerprint:
                error = "Missing resume fingerprint for embed-only retry"
                failure_state = self._state(
                    status="failed",
                    step="embedding",
                    phase="embedding_resume",
                    upload_id=upload_id,
                    owner_id=owner_id,
                    error=error,
                )
                await self._fail_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="resume_embedding",
                    error=error,
                    state=failure_state,
                )
                return

            embedding_state = self._state(
                status="running",
                step="embedding",
                phase="embedding_resume",
                upload_id=upload_id,
                owner_id=owner_id,
                resume_fingerprint=fingerprint,
            )
            embedding_correlation = await self._record_stage_start(
                pipeline_runs=pipeline_runs,
                task_id=task_id,
                stage="resume_embedding",
                metadata=embedding_state,
            )
            await pubsub.subscribe(CHANNEL_EMBEDDINGS_DONE)
            await asyncio.to_thread(
                self.enqueue_job,
                STREAM_EMBEDDINGS,
                {
                    "task_id": task_id,
                    "resume_fingerprint": fingerprint,
                    "resume_upload_id": upload_id,
                    "owner_id": owner_id,
                    "tenant_id": tenant_id,
                    **embedding_correlation,
                },
            )
            self.logger.info("Enqueued embedding stage for resume ETL")

            async with asyncio.timeout(self.listener_timeout):
                embeddings_data = await self.wait_for_task_message(pubsub, task_id)

            await pubsub.unsubscribe(CHANNEL_EMBEDDINGS_DONE)

            embeddings_status = (embeddings_data or {}).get("status")
            if embeddings_status != "completed":
                err = (embeddings_data or {}).get("error", "Embeddings failed")
                self.logger.error(
                    "Embedding stage failed with status=%s",
                    embeddings_status,
                )
                failure_state = self._state(
                    status="failed",
                    step="embedding",
                    phase="embedding_resume",
                    upload_id=upload_id,
                    owner_id=owner_id,
                    resume_fingerprint=fingerprint,
                    error=err,
                )
                await self._fail_stage(
                    pipeline_runs=pipeline_runs,
                    task_id=task_id,
                    stage="resume_embedding",
                    error=err,
                    state=failure_state,
                )
                return

            self.logger.info("Resume ETL completed successfully")
            completed_state = self._state(
                status="completed",
                phase="completed",
                upload_id=upload_id,
                owner_id=owner_id,
                resume_fingerprint=fingerprint,
            )
            await self._record_stage_complete(
                pipeline_runs=pipeline_runs,
                task_id=task_id,
                stage="resume_embedding",
                metadata=completed_state,
            )
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.complete_run,
                    task_id=task_id,
                    run_type="resume_upload",
                    metadata=completed_state,
                )
            self._write_state(task_id, completed_state)

        except asyncio.TimeoutError:
            self.logger.error("Timeout during resume ETL")
            failure_state = self._state(
                status="failed",
                step="embedding",
                phase="embedding_resume",
                upload_id=upload_id,
                owner_id=owner_id,
                resume_fingerprint=resume_fingerprint,
                error="Stage timeout",
            )
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.fail_run,
                    task_id=task_id,
                    run_type="resume_upload",
                    error="Stage timeout",
                    retry_eligible=True,
                    metadata=failure_state,
                )
            self._write_state(task_id, failure_state)
        except Exception as exc:
            self.logger.exception("Resume ETL failed due to an unhandled exception")
            failure_state = self._state(
                status="failed",
                step="embedding",
                phase="embedding_resume",
                upload_id=upload_id,
                owner_id=owner_id,
                resume_fingerprint=resume_fingerprint,
                error=str(exc),
            )
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.fail_run,
                    task_id=task_id,
                    run_type="resume_upload",
                    error=str(exc),
                    retry_eligible=True,
                    metadata=failure_state,
                )
            self._write_state(task_id, failure_state)
        finally:
            if redis_client:
                await self.cleanup_pubsub_and_client(redis_client, pubsub)
