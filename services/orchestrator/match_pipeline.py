"""Match pipeline orchestration independent of FastAPI route handlers."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Optional

StageRunner = Callable[..., Awaitable[tuple[bool, Optional[dict[str, Any]]]]]


class OrchestratorMatchPipelineService:
    """Coordinate extraction, embedding, and matching task stages."""

    def __init__(
        self,
        *,
        listener_timeout: float,
        logger: logging.Logger,
        record_jobs_matched: Callable[[int], None],
        record_jobs_embedded: Callable[[int], None],
        record_jobs_extracted: Callable[[int], None],
    ) -> None:
        self.listener_timeout = listener_timeout
        self.logger = logger
        self.record_jobs_matched = record_jobs_matched
        self.record_jobs_embedded = record_jobs_embedded
        self.record_jobs_extracted = record_jobs_extracted

    def durable_stage_name(self, stage_name: str) -> str:
        aliases = {
            "embed": "embedding",
            "embeddings": "embedding",
            "extract": "extraction",
            "match": "matching",
        }
        return aliases.get(stage_name, stage_name)

    def pipeline_stage_id(self, snapshot: dict[str, Any], stage: str) -> Optional[str]:
        for stage_snapshot in snapshot.get("result", {}).get("stages", []):
            if stage_snapshot.get("stage") == stage:
                return stage_snapshot.get("id")
        return None

    def stage_processed_count(self, stage_name: str, data: Optional[dict[str, Any]]) -> int:
        payload = data or {}
        if payload.get("status") == "skipped":
            return 0
        if stage_name == "matching":
            return int(payload.get("saved_count") or payload.get("matches_count") or 0)
        if stage_name in {"embeddings", "embedding"}:
            return int(payload.get("embedded_count") or payload.get("processed") or 1)
        if stage_name == "extraction":
            return int(payload.get("extracted_count") or payload.get("processed") or 1)
        return int(payload.get("processed") or 0)

    def record_stage_completion_metric(self, stage_name: str, count: int) -> None:
        if stage_name == "matching":
            self.record_jobs_matched(count)
        elif stage_name in {"embeddings", "embedding"}:
            self.record_jobs_embedded(count)
        elif stage_name == "extraction":
            self.record_jobs_extracted(count)

    async def start_pipeline_run_stage(
        self,
        *,
        pipeline_runs: Any | None,
        state: Any,
        task_id: str,
        stage_name: str,
        run_type: str,
        job_payload: dict[str, Any],
        queued_count: int = 1,
    ) -> dict[str, str]:
        if pipeline_runs is None:
            return {}

        durable_stage = self.durable_stage_name(stage_name)
        stage_metadata = {
            "stream_task_id": task_id,
            "stage": stage_name,
            "queued_payload": dict(job_payload),
        }
        snapshot = await asyncio.to_thread(
            pipeline_runs.start_stage,
            task_id=task_id,
            stage=durable_stage,
            run_type=run_type,
            queued_count=queued_count,
            metadata=stage_metadata,
        )
        correlation = {
            "pipeline_run_id": snapshot.get("pipeline_run_id"),
            "pipeline_stage_id": self.pipeline_stage_id(snapshot, durable_stage),
        }
        state.result = dict(state.result or {})
        for key, value in correlation.items():
            if value:
                state.result[key] = str(value)
                job_payload.setdefault(key, str(value))
        await state._save_to_redis()
        return {key: str(value) for key, value in correlation.items() if value}

    async def complete_pipeline_run_stage(
        self,
        *,
        pipeline_runs: Any | None,
        task_id: str,
        stage_name: str,
        run_type: str,
        data: Optional[dict[str, Any]],
    ) -> None:
        if pipeline_runs is None:
            return

        processed_count = self.stage_processed_count(stage_name, data)
        skipped_count = 1 if (data or {}).get("status") == "skipped" else 0
        await asyncio.to_thread(
            pipeline_runs.complete_stage,
            task_id=task_id,
            stage=self.durable_stage_name(stage_name),
            run_type=run_type,
            processed_count=processed_count,
            skipped_count=skipped_count,
            metadata=data or {},
        )
        self.record_stage_completion_metric(stage_name, processed_count)

    async def fail_pipeline_run_stage(
        self,
        *,
        pipeline_runs: Any | None,
        task_id: str,
        stage_name: str,
        run_type: str,
        error: str,
        data: Optional[dict[str, Any]] = None,
    ) -> None:
        if pipeline_runs is None:
            return

        await asyncio.to_thread(
            pipeline_runs.fail_stage,
            task_id=task_id,
            stage=self.durable_stage_name(stage_name),
            run_type=run_type,
            error=error,
            retry_eligible=True,
            metadata=data or {},
        )

    async def run_pipeline_stage(
        self,
        *,
        state: Any,
        pubsub: Any,
        stream: str,
        job_payload: dict[str, Any],
        stage_name: str,
        pipeline_runs: Any | None = None,
        run_type: str = "match",
        channel_map: dict[str, str],
        enqueue_job_fn: Callable[[str, dict[str, Any]], Any],
        wait_for_task_message_fn: Callable[[Any, str], Awaitable[dict[str, Any]]],
        start_stage_fn: Callable[..., Awaitable[dict[str, str]]],
        complete_stage_fn: Callable[..., Awaitable[None]],
        fail_stage_fn: Callable[..., Awaitable[None]],
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        completion_channel = channel_map.get(stage_name, "unknown")
        if isinstance(state.result, dict) and state.result.get("pipeline_run_id"):
            job_payload.setdefault("pipeline_run_id", state.result["pipeline_run_id"])
        await start_stage_fn(
            pipeline_runs=pipeline_runs,
            state=state,
            task_id=state.task_id,
            stage_name=stage_name,
            run_type=run_type,
            job_payload=job_payload,
        )

        self.logger.info(
            "📤 Enqueueing %s job to %s: task_id=%s",
            stage_name,
            stream,
            job_payload.get("task_id"),
        )
        self.logger.debug(" Payload: %s", json.dumps(job_payload))
        await asyncio.to_thread(enqueue_job_fn, stream, job_payload)
        self.logger.info(
            "✅ %s job enqueued: task_id=%s",
            stage_name.capitalize(),
            job_payload.get("task_id"),
        )

        self.logger.info("⏳ Waiting for %s completion on %s...", stage_name, completion_channel)
        data = await wait_for_task_message_fn(pubsub, state.task_id)
        if not data:
            self.logger.error(
                "❌ No completion message received for stage %s (task_id=%s)",
                stage_name,
                state.task_id,
            )
            state.status = "failed"
            state.error = f"No completion message from {stage_name}"
            await state._save_to_redis()
            await fail_stage_fn(
                pipeline_runs=pipeline_runs,
                task_id=state.task_id,
                stage_name=stage_name,
                run_type=run_type,
                error=state.error,
            )
            await state.notify(
                {"task_id": state.task_id, "status": "failed", "error": state.error}
            )
            return False, None

        self.logger.info(
            "📨 Received %s completion: task_id=%s, status=%s, channel=%s",
            stage_name,
            data.get("task_id"),
            data.get("status"),
            completion_channel,
        )

        status = data.get("status")
        if status == "failed":
            self.logger.error(
                "❌ %s failed for task %s: %s",
                stage_name,
                state.task_id,
                data.get("error"),
            )
            state.status = "failed"
            state.error = data.get("error", f"{stage_name.capitalize()} failed")
            await state._save_to_redis()
            await fail_stage_fn(
                pipeline_runs=pipeline_runs,
                task_id=state.task_id,
                stage_name=stage_name,
                run_type=run_type,
                error=state.error,
                data=data,
            )
            await state.notify(
                {"task_id": state.task_id, "status": "failed", "error": state.error}
            )
            return False, data

        if status not in ("skipped", "completed"):
            self.logger.warning("❌ Unexpected status in %s response: %s", stage_name, status)
            state.status = "failed"
            state.error = f"Unexpected status from {stage_name}: {status}"
            await state._save_to_redis()
            await fail_stage_fn(
                pipeline_runs=pipeline_runs,
                task_id=state.task_id,
                stage_name=stage_name,
                run_type=run_type,
                error=state.error,
                data=data,
            )
            await state.notify(
                {"task_id": state.task_id, "status": "failed", "error": state.error}
            )
            return False, data

        await complete_stage_fn(
            pipeline_runs=pipeline_runs,
            task_id=state.task_id,
            stage_name=stage_name,
            run_type=run_type,
            data=data,
        )
        return True, data

    async def handle_extraction_fingerprint(
        self,
        state: Any,
        task_id: str,
        extraction_data: dict[str, Any],
    ) -> bool:
        fp = extraction_data.get("resume_fingerprint")
        status = extraction_data.get("status")

        if not fp:
            if status != "skipped":
                self.logger.error("❌ No fingerprint in extraction response for task: %s", task_id)
                state.status = "failed"
                state.error = "No fingerprint in extraction response"
                await state._save_to_redis()
                await state.notify(
                    {"task_id": task_id, "status": "failed", "error": state.error}
                )
                return False
            self.logger.info(
                "ℹ️ Extraction skipped with no new fingerprint for task %s; using existing: %s",
                task_id,
                (state.resume_fingerprint or "")[:16],
            )
            return True

        state.resume_fingerprint = fp
        status_msg = (
            "Resume unchanged, using existing"
            if status == "skipped"
            else "Extraction complete"
        )
        self.logger.info("ℹ️ %s: %s...", status_msg, fp[:16])
        return True

    async def cleanup_pubsub_and_client(self, redis_client: Any, pubsub: Any) -> None:
        if pubsub:
            try:
                await pubsub.unsubscribe()
                await pubsub.close()
            except Exception as e:
                self.logger.warning("Failed to close pubsub: %s", e)
        if redis_client:
            try:
                await redis_client.aclose()
            except Exception as e:
                self.logger.warning("Failed to close Redis client: %s", e)

    async def run_extraction_stage(
        self,
        *,
        state: Any,
        task_id: str,
        pubsub: Any,
        pipeline_runs: Any | None,
        channel_extraction_done: str,
        stream_extraction: str,
        run_pipeline_stage_fn: StageRunner,
        handle_extraction_fingerprint_fn: Callable[[Any, str, dict[str, Any]], Awaitable[bool]],
    ) -> bool:
        state.status = "extracting"
        state.current_stage = "extract"
        await state._save_to_redis()
        await state.notify({
            "task_id": task_id,
            "status": "extracting",
            "message": "Starting extraction",
        })

        await pubsub.subscribe(channel_extraction_done)

        async with asyncio.timeout(self.listener_timeout):
            success, extraction_data = await run_pipeline_stage_fn(
                state=state,
                pubsub=pubsub,
                stream=stream_extraction,
                job_payload={"task_id": task_id},
                stage_name="extraction",
                pipeline_runs=pipeline_runs,
            )

        if not success:
            return False
        return await handle_extraction_fingerprint_fn(state, task_id, extraction_data or {})

    async def run_embeddings_stage(
        self,
        *,
        state: Any,
        task_id: str,
        pubsub: Any,
        pipeline_runs: Any | None,
        channel_extraction_done: str,
        channel_embeddings_done: str,
        stream_embeddings: str,
        run_pipeline_stage_fn: StageRunner,
    ) -> bool:
        state.status = "embedding"
        state.current_stage = "embed"
        await state._save_to_redis()
        await state.notify({
            "task_id": task_id,
            "status": "embedding",
            "message": "Starting embeddings",
        })

        await pubsub.unsubscribe(channel_extraction_done)
        await pubsub.subscribe(channel_embeddings_done)

        async with asyncio.timeout(self.listener_timeout):
            success, _ = await run_pipeline_stage_fn(
                state=state,
                pubsub=pubsub,
                stream=stream_embeddings,
                job_payload={
                    "task_id": task_id,
                    "resume_fingerprint": state.resume_fingerprint,
                },
                stage_name="embeddings",
                pipeline_runs=pipeline_runs,
            )

        return success

    async def run_matching_stage(
        self,
        *,
        state: Any,
        task_id: str,
        pubsub: Any,
        channel_done: str,
        pipeline_runs: Any | None,
        channel_matching_done: str,
        stream_matching: str,
        run_pipeline_stage_fn: StageRunner,
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        state.status = "matching"
        state.current_stage = "match"
        await state._save_to_redis()
        await state.notify({
            "task_id": task_id,
            "status": "matching",
            "message": "Starting matching",
        })

        await pubsub.unsubscribe(channel_done)
        await pubsub.subscribe(channel_matching_done)

        async with asyncio.timeout(self.listener_timeout):
            success, matching_data = await run_pipeline_stage_fn(
                state=state,
                pubsub=pubsub,
                stream=stream_matching,
                job_payload={
                    "task_id": task_id,
                    "resume_fingerprint": state.resume_fingerprint,
                },
                stage_name="matching",
                pipeline_runs=pipeline_runs,
            )

        return success, matching_data

    async def run_matching_fast_path(
        self,
        *,
        state: Any,
        task_id: str,
        pipeline_runs: Any | None,
        redis_url: str,
        redis_factory: Callable[..., Any],
        channel_matching_done: str,
        stream_matching: str,
        run_pipeline_stage_fn: StageRunner,
    ) -> tuple[Any, Any, bool, Optional[dict[str, Any]]]:
        redis_client = redis_factory(redis_url, decode_responses=True)
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(channel_matching_done)
        self.logger.info("📡 Subscribed to %s for matching", channel_matching_done)

        async with asyncio.timeout(self.listener_timeout):
            success, matching_data = await run_pipeline_stage_fn(
                state=state,
                pubsub=pubsub,
                stream=stream_matching,
                job_payload={
                    "task_id": task_id,
                    "resume_fingerprint": state.resume_fingerprint,
                },
                stage_name="matching",
                pipeline_runs=pipeline_runs,
            )

        return redis_client, pubsub, success, matching_data

    async def run_full_match_pipeline(
        self,
        *,
        state: Any,
        task_id: str,
        pipeline_runs: Any | None,
        redis_url: str,
        redis_factory: Callable[..., Any],
        channel_embeddings_done: str,
        run_extraction_stage_fn: Callable[..., Awaitable[bool]],
        run_embeddings_stage_fn: Callable[..., Awaitable[bool]],
        run_matching_stage_fn: Callable[..., Awaitable[tuple[bool, Optional[dict[str, Any]]]]],
    ) -> tuple[Any, Any, bool, Optional[dict[str, Any]]]:
        redis_client = redis_factory(redis_url, decode_responses=True)
        pubsub = redis_client.pubsub()

        if not await run_extraction_stage_fn(state, task_id, pubsub, pipeline_runs):
            return redis_client, pubsub, False, None

        if not await run_embeddings_stage_fn(state, task_id, pubsub, pipeline_runs):
            return redis_client, pubsub, False, None

        success, matching_data = await run_matching_stage_fn(
            state, task_id, pubsub, channel_embeddings_done, pipeline_runs
        )
        return redis_client, pubsub, success, matching_data

    async def complete_match_task(
        self,
        state: Any,
        task_id: str,
        matching_data: Optional[dict[str, Any]],
    ) -> None:
        state.status = "completed"
        state.current_stage = "match"
        state.matches_count = (matching_data or {}).get("matches_count", 0)
        state.result = {"matches_count": state.matches_count}
        await state._save_to_redis()
        self.logger.info(
            "🎉 Pipeline completed for task %s: %d matches",
            task_id,
            state.matches_count,
        )
        await state.notify(
            {
                "task_id": task_id,
                "status": "completed",
                "matches_count": state.matches_count,
                "message": f"Matching complete, {state.matches_count} matches",
            }
        )

    async def orchestrate_match(
        self,
        *,
        task_id: str,
        registry: Any,
        resume_fingerprint: Optional[str],
        pipeline_runs: Any | None,
        state_getter: Callable[[Any, str], Awaitable[Any]],
        run_matching_fast_path_fn: Callable[..., Awaitable[tuple[Any, Any, bool, Optional[dict[str, Any]]]]],
        run_full_match_pipeline_fn: Callable[..., Awaitable[tuple[Any, Any, bool, Optional[dict[str, Any]]]]],
        complete_match_task_fn: Callable[[Any, str, Optional[dict[str, Any]]], Awaitable[None]],
        fail_stage_fn: Callable[..., Awaitable[None]],
        cleanup_fn: Callable[[Any, Any], Awaitable[None]],
    ) -> None:
        async with registry.lock:
            registry.active_task_ids.add(task_id)

        state = await state_getter(registry, task_id)
        state.task_type = "match"

        if resume_fingerprint:
            state.resume_fingerprint = resume_fingerprint
            self.logger.info("🔄 Resume already processed, skipping extraction/embedding")
        else:
            state.status = "extracting"
            state.current_stage = "extract"

        await state._save_to_redis()

        redis_client = None
        pubsub = None
        try:
            self.logger.info("🚀 Starting pipeline for task: %s", task_id)

            if resume_fingerprint:
                await state.notify(
                    {
                        "task_id": task_id,
                        "status": "matching",
                        "message": "Resume already processed, starting matching",
                    }
                )
                self.logger.info("⏭️ Skipping extraction and embedding stages")
                redis_client, pubsub, success, matching_data = await run_matching_fast_path_fn(
                    state, task_id, pipeline_runs
                )
            else:
                redis_client, pubsub, success, matching_data = await run_full_match_pipeline_fn(
                    state, task_id, pipeline_runs
                )

            if not success:
                return

            await complete_match_task_fn(state, task_id, matching_data)

        except asyncio.TimeoutError:
            self.logger.exception(
                "❌ Orchestration timeout for task %s: %s",
                task_id,
                "stage timeout exceeded",
            )
            state.status = "failed"
            state.error = "Stage timeout"
            await state._save_to_redis()
            await fail_stage_fn(
                pipeline_runs=pipeline_runs,
                task_id=task_id,
                stage_name=state.current_stage or "matching",
                run_type="match",
                error=state.error,
            )
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.fail_run,
                    task_id=task_id,
                    run_type="match",
                    error=state.error,
                    retry_eligible=True,
                    metadata={"current_stage": state.current_stage},
                )
            await state.notify(
                {"task_id": task_id, "status": "failed", "error": state.error}
            )

        except Exception as e:
            self.logger.exception(
                "❌ Orchestration failed for task %s: %s",
                task_id,
                type(e).__name__,
            )
            state.status = "failed"
            state.error = str(e)
            await state._save_to_redis()
            await fail_stage_fn(
                pipeline_runs=pipeline_runs,
                task_id=task_id,
                stage_name=state.current_stage or "matching",
                run_type="match",
                error=state.error,
            )
            if pipeline_runs is not None:
                await asyncio.to_thread(
                    pipeline_runs.fail_run,
                    task_id=task_id,
                    run_type="match",
                    error=state.error,
                    retry_eligible=True,
                    metadata={"current_stage": state.current_stage},
                )
            await state.notify(
                {"task_id": task_id, "status": "failed", "error": str(e)}
            )

        finally:
            if redis_client:
                await cleanup_fn(redis_client, pubsub)
            async with registry.lock:
                registry.active_task_ids.discard(task_id)
            await state.close(registry)
