"""Running every series on its interval.

Runs are aligned to the clock (every 10 minutes means :00, :10, :20…). Each series runs in its
own loop, one run after the other, so a slow run never overlaps the next slot; the loop waits for
the start of the slot after the one the run ended in.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta

import httpx

from data_pipeline.core.readings import Accepted
from data_pipeline.core.series import Series
from data_pipeline.core.sources import Source
from data_pipeline.runner.collect import Clock, collect
from data_pipeline.runner.destination import Destination

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def slot_start(moment: datetime, every: timedelta) -> datetime:
    """The start of the slot ``moment`` falls in."""
    return moment - (moment - _EPOCH) % every


async def run_slot(
    series: Series,
    sources: Mapping[str, Source],
    client: httpx.AsyncClient,
    now: Clock,
    destination: Destination | None = None,
) -> bool:
    """Run ``series`` unless its current slot is outside its hours, and send its reading to
    ``destination`` if one was accepted. A destination that fails is logged, and the value
    dropped.

    Returns whether it ran.
    """
    if not series.runs_at(slot_start(now(), series.every)):
        return False
    observations = await collect(series, sources, client, now)
    if destination is not None:
        for observation in observations:
            if isinstance(observation.outcome, Accepted):
                try:
                    await destination.send(series, observation)
                except Exception:
                    logger.exception("sending %s failed; the value is dropped", series.id)
    return True


async def run_forever(
    series: Series,
    sources: Mapping[str, Source],
    client: httpx.AsyncClient,
    now: Clock = lambda: datetime.now(UTC),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    destination: Destination | None = None,
) -> None:
    """Run ``series`` now, then at the start of every slot.

    An error in one run is logged and the next slot runs anyway. A run that ends in the next slot
    counts as that slot's run, so that slot is skipped.
    """
    while True:
        try:
            await run_slot(series, sources, client, now, destination)
        except Exception:
            logger.exception("run of %s failed", series.id)
        next_slot = slot_start(now(), series.every) + series.every
        await sleep((next_slot - now()).total_seconds())
