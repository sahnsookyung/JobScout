#!/usr/bin/env python3
"""
Pipeline service - manages background pipeline execution.
"""

import asyncio
import logging
import os
import sys
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from threading import Event, Lock
from typing import Any, Dict, Optional

from core.app_context import AppContext
from core.config_loader import load_config
from pipeline.runner import MatchingPipelineResult, run_matching_pipeline

sys.path.insert(
    0,
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )
        )
    ),
)
from main import run_resume_etl

logger = logging.getLogger(__name__)


@dataclass
class PipelineTask:
    """Represents a pipeline execution task."""

    task_id: str
    task_type: str
    status: str
    step: Optional[str] = None
    orchestrator_task_id: Optional[str] = None
    result: Optional[MatchingPipelineResult] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    stop_event: Event = field(default_factory=Event)
    message: Optional[str] = None
    phases: Optional[Dict[str, Any]] = None
    cancellation_requested: bool = False
    persistence_started: bool = False


class PipelineTaskManager:
    """Manages pipeline task execution and state."""

    ACTIVE_STATUSES = {"pending", "running", "cancellation_requested", "persisting"}
    TERMINAL_STATUSES = {"completed", "failed", "cancelled"}

    def __init__(self):
        self._tasks: Dict[str, PipelineTask] = {}
        self._lock = Lock()
        self._event_queues: Dict[str, asyncio.Queue] = {}
        self._subscriber_counts: Dict[str, int] = defaultdict(int)
        self._queue_lock = Lock()

    def subscribe(self, task_id: str) -> asyncio.Queue:
        """Subscribe to status updates for a task."""
        with self._queue_lock:
            if task_id not in self._event_queues:
                self._event_queues[task_id] = asyncio.Queue()
            self._subscriber_counts[task_id] += 1
            return self._event_queues[task_id]

    def unsubscribe(self, task_id: str):
        """Unsubscribe from status updates for a task."""
        with self._queue_lock:
            if task_id in self._subscriber_counts:
                self._subscriber_counts[task_id] -= 1
                if self._subscriber_counts[task_id] <= 0:
                    del self._subscriber_counts[task_id]
                    if task_id in self._event_queues:
                        del self._event_queues[task_id]

    def publish_update(self, task_id: str, data: Dict[str, Any]):
        """Publish a status update to all subscribers."""
        with self._queue_lock:
            queue = self._event_queues.get(task_id)
            if queue:
                try:
                    queue.put_nowait(data)
                except asyncio.QueueFull:
                    pass

    def create_matching_task(self) -> str:
        """Create or reuse the active matching task and start the worker."""
        with self._lock:
            for tid, task in self._tasks.items():
                if task.task_type == "matching" and task.status in self.ACTIVE_STATUSES:
                    return tid

        task_id = str(uuid.uuid4())
        with self._lock:
            self._tasks[task_id] = PipelineTask(
                task_id=task_id,
                task_type="matching",
                status="pending",
                step="initializing",
            )

        def run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                self._run_pipeline_background(task_id)
            finally:
                loop.close()

        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()
        return task_id

    def create_resume_task(self) -> str:
        """Create a standalone resume ETL task without starting matching work."""
        task_id = str(uuid.uuid4())
        with self._lock:
            self._tasks[task_id] = PipelineTask(
                task_id=task_id,
                task_type="resume_etl",
                status="pending",
                step="initializing",
            )
        return task_id

    def create_task(self) -> str:
        """Backward-compatible alias used by resume upload routes."""
        return self.create_resume_task()

    def get_task(self, task_id: str) -> Optional[PipelineTask]:
        """Get task by ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def get_active_task(self) -> Optional[PipelineTask]:
        """Get currently running matching task, if any."""
        with self._lock:
            for task in self._tasks.values():
                if task.task_type == "matching" and task.status in self.ACTIVE_STATUSES:
                    return task
        return None

    def request_stop(self, task_id: str) -> Optional[str]:
        """Request task cancellation and return the resulting status."""
        next_status = None
        step = None
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None
            if task.status in self.TERMINAL_STATUSES:
                return task.status
            if task.persistence_started or task.status == "persisting":
                task.status = "persisting"
                next_status = "persisting"
                step = task.step
            else:
                task.cancellation_requested = True
                task.stop_event.set()
                task.status = "cancellation_requested"
                next_status = "cancellation_requested"
                step = task.step

        if next_status:
            self.update_task_status(task_id, next_status, step=step)
            return next_status
        return None

    def stop_active_task(self) -> Optional[PipelineTask]:
        """Stop the currently running matching task."""
        next_status = None
        selected_task = None
        with self._lock:
            for task in self._tasks.values():
                if task.task_type == "matching" and task.status in self.ACTIVE_STATUSES:
                    if task.persistence_started or task.status == "persisting":
                        task.status = "persisting"
                        next_status = "persisting"
                    else:
                        task.cancellation_requested = True
                        task.stop_event.set()
                        task.status = "cancellation_requested"
                        next_status = "cancellation_requested"
                    selected_task = task
                    break

        if selected_task and next_status:
            self.update_task_status(
                selected_task.task_id,
                next_status,
                step=selected_task.step,
            )
        return selected_task

    def update_task_status(
        self,
        task_id: str,
        status: str,
        step: Optional[str] = None,
        result: Optional[MatchingPipelineResult] = None,
        error: Optional[str] = None,
    ):
        """Update task status and publish to subscribers."""
        if task_id in self._tasks:
            task = self._tasks[task_id]
            task.status = status
            if status == "persisting":
                task.persistence_started = True
            if status == "cancellation_requested":
                task.cancellation_requested = True
            if step is not None:
                task.step = step
            if result:
                task.result = result
            if error:
                task.error = error

        update_data = {"task_id": task_id, "status": status}
        if step is not None:
            update_data["step"] = step
        if result:
            update_data["matches_count"] = result.matches_count
            update_data["saved_count"] = result.saved_count
            update_data["notified_count"] = result.notified_count
            update_data["execution_time"] = result.execution_time
            update_data["success"] = result.success
            if result.error:
                update_data["error"] = result.error
        if error:
            update_data["error"] = error

        try:
            self.publish_update(task_id, update_data)
        except Exception as exc:
            logger.error(f"Failed to publish update: {exc}")

    def _cleanup_completed_tasks(self, keep_count: int = 5):
        """Remove old terminal tasks, keeping only the most recent ones."""
        with self._lock:
            completed_tasks = [
                (tid, task)
                for tid, task in self._tasks.items()
                if task.status in self.TERMINAL_STATUSES
            ]
            if len(completed_tasks) <= keep_count:
                return

            completed_tasks.sort(key=lambda item: item[1].created_at, reverse=True)
            for tid, _ in completed_tasks[keep_count:]:
                del self._tasks[tid]
                logger.debug(f"Cleaned up completed task {tid}")

    def _run_pipeline_background(self, task_id: str):
        """Run resume ETL then matching in a background thread."""
        try:
            self.update_task_status(task_id, "running")

            full_config = load_config()
            ctx = AppContext.build(full_config)

            def status_callback(step_name: str):
                task = self.get_task(task_id)
                if task and step_name == "saving_results":
                    task.persistence_started = True
                    self.update_task_status(task_id, "persisting", step=step_name)
                elif task and task.cancellation_requested:
                    self.update_task_status(task_id, "cancellation_requested", step=step_name)
                else:
                    self.update_task_status(task_id, "running", step=step_name)

            task = self.get_task(task_id)
            if not task:
                raise RuntimeError(f"Task {task_id} not found during execution")

            self.update_task_status(task_id, "running", step="resume_etl")
            logger.info("Running Resume ETL before matching...")
            try:
                try:
                    run_resume_etl(ctx, task.stop_event)
                except TypeError:
                    run_resume_etl(ctx)
                logger.info("Resume ETL completed")
            except Exception as exc:
                logger.error(f"Resume ETL failed: {exc}")

            if task.stop_event.is_set():
                self.update_task_status(task_id, "cancelled", step=task.step)
                return

            self.update_task_status(task_id, "running", step="matching")
            result = run_matching_pipeline(
                ctx,
                task.stop_event,
                status_callback=status_callback,
            )

            if result.cancelled:
                final_status = "cancelled"
            elif result.success:
                final_status = "completed"
            else:
                final_status = "failed"

            self.update_task_status(
                task_id,
                final_status,
                result=result,
                error=result.error if not result.success else None,
            )
        except Exception as exc:
            logger.exception(f"Error in background pipeline task {task_id}")
            self.update_task_status(task_id, "failed", error=str(exc))


_pipeline_manager = PipelineTaskManager()


def get_pipeline_manager() -> PipelineTaskManager:
    """Get the global pipeline task manager."""
    return _pipeline_manager
