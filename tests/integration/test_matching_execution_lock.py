"""Real PostgreSQL session locks serialize matching pages and survive no leases."""

import asyncio
import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine

from core.matching_run_state import matching_execution_lock


@pytest.mark.db
@pytest.mark.asyncio
async def test_two_connections_serialize_parent_and_release_after_cancellation(test_database):
    engine = create_engine(test_database, pool_size=3)
    parent = str(uuid.uuid4())
    entered = asyncio.Event()

    async def contender():
        async with matching_execution_lock(parent):
            entered.set()

    try:
        with patch("core.matching_run_state.get_engine", return_value=engine):
            async with matching_execution_lock(parent):
                waiter = asyncio.create_task(contender())
                await asyncio.sleep(0.1)
                assert not entered.is_set()
                # Independent parent can proceed while this parent is locked.
                async with matching_execution_lock(str(uuid.uuid4()), timeout_seconds=0):
                    pass
                waiter.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiter
            # Cancellation did not leak the waiting connection or parent's lock.
            await asyncio.wait_for(contender(), timeout=2)
            assert entered.is_set()
    finally:
        engine.dispose()
