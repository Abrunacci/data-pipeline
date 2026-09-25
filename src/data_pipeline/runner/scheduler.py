"""Running every series on its interval, inside the API process.

Runs are aligned to the clock (every 10 minutes means :00, :10, :20…), so two processes alive at
once during a deploy aim at the same slots. ``Store.exclusive`` lets only one of them run a
series at a time, and a slot that already has an attempt is skipped, so a slot runs once.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta

import httpx

from data_pipeline.core.series import Series
from data_pipeline.core.sources import Source
from data_pipeline.runner.collect import Clock, collect
from data_pipeline.runner.store import Store

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def slot_start(moment: datetime, every: timedelta) -> datetime:
    """The start of the slot ``moment`` falls in."""
    return moment - (moment - _EPOCH) % every


async def run_slot(
    series: Series,
    sources: Mapping[str, Source],
    client: httpx.AsyncClient,
    store: Store,
    now: Clock,
) -> bool:
    """Run ``series`` for the current slot unless another process is on it or already did it.

    Returns whether it ran.
    """
    async with store.exclusive(series.id) as alone:
        if not alone:
            return False
        start = slot_start(now(), series.every)
        if not series.runs_at(start):
            return False
        if await store.attempted_in(series.id, start, start + series.every):
            return False
        await collect(series, sources, client, store, now)
        return True


async def run_forever(
    series: Series,
    sources: Mapping[str, Source],
    client: httpx.AsyncClient,
    store: Store,
    now: Clock = lambda: datetime.now(UTC),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Run ``series`` now if its current slot has not run, then at the start of every slot.

    An error in one run is logged and the next slot runs anyway. A run that starts at the very
    end of a slot and ends in the next one counts as that next slot's run, so that slot is
    skipped; it only happens at startup.
    """
    while True:
        try:
            await run_slot(series, sources, client, store, now)
        except Exception:
            logger.exception("run of %s failed", series.id)
        next_slot = slot_start(now(), series.every) + series.every
        await sleep((next_slot - now()).total_seconds())
