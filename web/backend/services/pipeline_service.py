#!/usr/bin/env python3
"""
Pipeline service - manages background pipeline execution.
"""

import uuid
import logging
import threading
import asyncio
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Any, Callable, List
from threading import Lock, Event

from core.config_loader import load_config
from core.app_context import AppContext
from pipeline.runner import run_matching_pipeline, MatchingPipelineResult
from pipeline.control import PipelineController
from ..exceptions import PipelineLockedException

logger = logging.getLogger(__name__)


@dataclass
class PipelineTask:
    """Represents a pipeline execution task."""
    task_id: str
    status: str  # "pending", "running", "completed", "failed"
    step: Optional[str] = None
    result: Optional[MatchingPipelineResult] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    stop_event: Event = field(default_factory=Event)


class PipelineTaskManager:
    """Manages pipeline task execution and state."""
    
    def __init__(self):
        self._tasks: Dict[str, PipelineTask] = {}
        self._lock = Lock()
        self._controller = PipelineController()
        self._event_queues: Dict[str, asyncio.Queue] = {}
        self._subscriber_counts: Dict[str, int] = defaultdict(int)
        self._queue_lock = Lock()
    
    def subscribe(self, task_id: str) -> asyncio.Queue:
        """
        Subscribe to status updates for a task.
        
        Args:
            task_id: The task ID to subscribe to.
        
        Returns:
            asyncio.Queue for receiving status updates.
        """
        with self._queue_lock:
            if task_id not in self._event_queues:
                self._event_queues[task_id] = asyncio.Queue()
            self._subscriber_counts[task_id] += 1
            return self._event_queues[task_id]
    
    def unsubscribe(self, task_id: str):
        """
        Unsubscribe from status updates for a task.
        
        Args:
            task_id: The task ID to unsubscribe from.
        """
        with self._queue_lock:
            if task_id in self._subscriber_counts:
                self._subscriber_counts[task_id] -= 1
                if self._subscriber_counts[task_id] <= 0:
                    del self._subscriber_counts[task_id]
                    if task_id in self._event_queues:
                        del self._event_queues[task_id]
    
    def publish_update(self, task_id: str, data: Dict[str, Any]):
        """
        Publish a status update to all subscribers.
        
        Args:
            task_id: The task ID.
            data: The status data to publish.
        """
        with self._queue_lock:
            queue = self._event_queues.get(task_id)
            if queue:
                try:
                    queue.put_nowait(data)
                except asyncio.QueueFull:
                    pass
    
    def create_task(self) -> str:
        """
        Create a new pipeline task.
        
        Returns:
            Task ID.
        
        Raises:
            PipelineLockedException: If pipeline is already running.
        """
        # Check for existing running tasks
        with self._lock:
            for tid, task in self._tasks.items():
                if task.status in ["pending", "running"]:
                    return tid  # Return existing task ID
        
        # Try to acquire global lock
        if not self._controller.acquire_lock("frontend"):
            lock_info = self._controller.get_lock_info()
            owner = lock_info.get("source", "unknown") if lock_info else "unknown"
            raise PipelineLockedException(
                f"Pipeline is locked by another process ({owner}). "
                "Please try again later."
            )
        
        # Create new task
        task_id = str(uuid.uuid4())
        
        with self._lock:
            self._tasks[task_id] = PipelineTask(
                task_id=task_id,
                status="pending",
                step="initializing"
            )
        
        # Start background thread
        thread = threading.Thread(
            target=self._run_pipeline_background,
            args=(task_id,),
            daemon=True
        )
        thread.start()
        
        return task_id
    
    def get_task(self, task_id: str) -> Optional[PipelineTask]:
        """
        Get task by ID.
        
        Args:
            task_id: The task ID.
        
        Returns:
            PipelineTask or None if not found.
        """
        with self._lock:
            return self._tasks.get(task_id)
    
    def get_active_task(self) -> Optional[PipelineTask]:
        """
        Get currently running task, if any.
        
        Returns:
            PipelineTask or None if no active task.
        """
        with self._lock:
            for task in self._tasks.values():
                if task.status in ["pending", "running"]:
                    return task
        return None
    
    def request_stop(self, task_id: str) -> bool:
        """
        Request task cancellation.
        
        Args:
            task_id: The task ID to stop.
        
        Returns:
            True if stop was requested, False if task not found.
        """
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].stop_event.set()
                return True
        return False
    
    def stop_active_task(self) -> Optional[str]:
        """
        Stop the currently running task.
        
        Returns:
            Task ID if stopped, None if no active task.
        """
        with self._lock:
            for tid, task in self._tasks.items():
                if task.status in ["pending", "running"]:
                    task.stop_event.set()
                    return tid
        return None
    
    def update_task_status(
        self,
        task_id: str,
        status: str,
        **kwargs
    ):
        """
        Update task status and other fields.
        
        Args:
            task_id: The task ID.
            status: New status.
            **kwargs: Additional fields to update.
        """
        with self._lock:
            if task_id in self._tasks:
                task = self._tasks[task_id]
                task.status = status
                for key, value in kwargs.items():
                    setattr(task, key, value)
                
                if status in ["completed", "failed"]:
                    self._cleanup_completed_tasks()
        
        update_data = {"task_id": task_id, "status": status, **kwargs}
        self.publish_update(task_id, update_data)
    
    def _cleanup_completed_tasks(self, keep_count: int = 5):
        """
        Remove old completed/failed tasks, keeping only the most recent ones.
        
        Args:
            keep_count: Number of completed tasks to keep.
        """
        with self._lock:
            completed_tasks = [
                (tid, t) for tid, t in self._tasks.items()
                if t.status in ["completed", "failed"]
            ]
            
            if len(completed_tasks) <= keep_count:
                return
            
            completed_tasks.sort(key=lambda x: x[1].created_at, reverse=True)
            
            for tid, _ in completed_tasks[keep_count:]:
                del self._tasks[tid]
                logger.debug(f"Cleaned up completed task {tid}")
    
    # Private methods
    
    def _run_pipeline_background(self, task_id: str):
        """Run the matching pipeline in a background thread."""
        try:
            # Update status to running
            self.update_task_status(task_id, "running")
            
            # Load config and build app context
            full_config = load_config()
            ctx = AppContext.build(full_config)
            
            # Define status callback
            def status_callback(step_name: str):
                self.update_task_status(task_id, "running", step=step_name)
            
            # Get stop event
            task = self.get_task(task_id)
            if not task:
                logger.error(f"Task {task_id} not found during execution")
                raise RuntimeError(f"Task {task_id} not found during execution")
            
            # Run the pipeline
            result = run_matching_pipeline(
                ctx,
                task.stop_event,
                status_callback=status_callback
            )
            
            # Update task with result
            final_status = "completed" if result.success else "failed"
            self.update_task_status(
                task_id,
                final_status,
                result=result,
                error=result.error if not result.success else None
            )
            
        except Exception as e:
            logger.exception(f"Error in background pipeline task {task_id}")
            self.update_task_status(
                task_id,
                "failed",
                error=str(e)
            )
        finally:
            try:
                self._controller.release_lock()
            except Exception as e:
                logger.error(f"Error releasing pipeline lock: {e}")


# Global pipeline task manager
_pipeline_manager = PipelineTaskManager()


def get_pipeline_manager() -> PipelineTaskManager:
    """Get the global pipeline task manager."""
    return _pipeline_manager
