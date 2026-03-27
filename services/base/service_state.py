"""Shared base class for service state containers."""
import asyncio
import threading
from typing import Optional


class BaseServiceState:
    """Holds mutable service-level state shared across microservices.

    Subclasses are expected to be instantiated with a concrete consumer type;
    the generic annotations here use ``object`` to stay type-checker-agnostic
    without requiring a heavyweight generic parameter.
    """

    def __init__(
        self,
        ctx: object,
        consumer: object,
        batch_consumer: Optional[object] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> None:
        self.ctx = ctx
        self.consumer = consumer
        self.batch_consumer = batch_consumer
        self.stop_event = stop_event or threading.Event()
        self.consumer_task: Optional[asyncio.Task] = None
        self.batch_consumer_task: Optional[asyncio.Task] = None
