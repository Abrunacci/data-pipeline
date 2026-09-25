"""Loading a series' past values once, from its history source."""

from __future__ import annotations

import logging
from datetime import datetime

import httpx

from data_pipeline.core.checks import canonical, value_problem
from data_pipeline.core.readings import Reading
from data_pipeline.core.series import Series
from data_pipeline.core.sources import HistorySource, MalformedResponseError
from data_pipeline.runner.collect import Clock
from data_pipeline.runner.fetch import FetchError, fetch
from data_pipeline.runner.store import Store

logger = logging.getLogger(__name__)


async def backfill(
    series: Series, source: HistorySource, client: httpx.AsyncClient, store: Store, now: Clock
) -> int | None:
    """Load the past values of ``series`` unless they were loaded before. Returns how many were
    stored, or None when it did not run: loaded already, another process on it, or it failed.

    Kept: the days before today (the source may still change today's), on the series' open
    days, with a value the calculator would accept. The plausible range is not applied: it is
    a check for today's prices, and the MEP was 37 in 2018. A failure is logged and tried again
    at the next start.
    """
    async with store.exclusive(series.id) as alone:
        if not alone or await store.has_history(series.id):
            return None
        try:
            body = await fetch(client, source.request())
            fetched_at = now()
            readings = source.parse(body, fetched_at)
        except (FetchError, MalformedResponseError) as error:
            logger.error(
                "loading past values of %s from %s failed: %s", series.id, source.name, error
            )
            return None
        kept = [
            Reading(canonical(reading.value), reading.as_of)
            for reading in readings
            if _keep(series, reading, fetched_at)
        ]
        await store.record_history(series.id, source.name, fetched_at, kept)
        logger.info("loaded %d past values of %s from %s", len(kept), series.id, source.name)
        return len(kept)


def _keep(series: Series, reading: Reading, fetched_at: datetime) -> bool:
    if value_problem(reading.value) is not None:
        return False
    if series.hours is None:
        return reading.as_of < fetched_at
    zone = series.hours.zone
    day = reading.as_of.astimezone(zone).date()
    before_today = day < fetched_at.astimezone(zone).date()
    return before_today and day.weekday() in series.hours.weekdays
