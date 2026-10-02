"""The runner's state, in memory: what the checks need to decide on the next reading, and nothing
more. Every attempt goes to the log instead of a table.

It holds, per series, the last accepted value, the suspects held back since it, and when the
last attempt was. A restart starts empty: the first reading of each series has no previous value
to check a jump against, and it is logged as accepted with no reference.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from data_pipeline.core.readings import (
    Accepted,
    Control,
    Held,
    Observation,
    Rejected,
    Suspect,
)
from data_pipeline.runner.store import SeriesState

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _Series:
    last_accepted: Decimal | None = None
    # Since the last accepted value, oldest first, with when each was fetched.
    suspects: list[tuple[datetime, Held]] = field(default_factory=list)
    last_attempt_at: datetime | None = None


class MemoryState:
    """A ``SeriesStore`` for one process. Its memory stays bounded: accepting a value drops the
    suspects before it, and ``state`` drops the ones that expired."""

    def __init__(self) -> None:
        self._series: dict[str, _Series] = {}
        self._busy: set[str] = set()

    async def record(self, observation: Observation) -> None:
        entry = self._series.setdefault(observation.series_id, _Series())
        entry.last_attempt_at = observation.fetched_at
        _log(observation, entry.last_accepted)
        match observation.outcome:
            case Accepted(reading=reading):
                entry.last_accepted, entry.suspects = reading.value, []
            case Suspect(reading=reading, why=why):
                entry.suspects.append((observation.fetched_at, Held(reading.value, why)))
            case Control() | Rejected():
                pass

    async def state(self, series_id: str, since: datetime) -> SeriesState:
        entry = self._series.get(series_id)
        if entry is None:
            return SeriesState(last_accepted=None, suspects=[])
        entry.suspects = [(at, held) for at, held in entry.suspects if at >= since]
        return SeriesState(
            last_accepted=entry.last_accepted, suspects=[held for _, held in entry.suspects]
        )

    async def attempted_in(self, series_id: str, start: datetime, end: datetime) -> bool:
        entry = self._series.get(series_id)
        last = None if entry is None else entry.last_attempt_at
        return last is not None and start <= last < end

    @asynccontextmanager
    async def exclusive(self, series_id: str) -> AsyncIterator[bool]:
        # One process: this only stops a slow run and the next slot from overlapping.
        if series_id in self._busy:
            yield False
            return
        self._busy.add(series_id)
        try:
            yield True
        finally:
            self._busy.discard(series_id)


def _log(observation: Observation, last_accepted: Decimal | None) -> None:
    """One line per attempt, the record a table used to keep."""
    head = f"{observation.series_id} {observation.source}"
    match observation.outcome:
        case Accepted(reading=reading, confirmed=confirmed, detail=detail):
            if last_accepted is None:
                how = "no reference"
            elif confirmed:
                how = f"confirmed: {detail}"
            else:
                how = "checked"
            logger.info("%s accepted %s (%s)", head, _value(reading.value), how)
        case Suspect(reading=reading, why=why, detail=detail):
            logger.info("%s held back %s, %s: %s", head, _value(reading.value), why, detail)
        case Control(reading=reading):
            logger.info("%s control %s", head, _value(reading.value))
        case Rejected(reason=reason, detail=detail):
            logger.warning("%s rejected, %s: %s", head, reason, detail)


def _value(value: Decimal) -> str:
    return format(value, "f")
