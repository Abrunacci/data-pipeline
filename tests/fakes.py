"""An in-memory ``Store`` with the same rules as the Postgres one, for tests of the runner."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from data_pipeline.core.readings import (
    Accepted,
    Control,
    Held,
    Observation,
    Reading,
    Rejected,
    Suspect,
)
from data_pipeline.runner.store import Day, Latest, Published, SeriesState


class MemoryStore:
    def __init__(self) -> None:
        self.observations: list[Observation] = []
        # Past values, kept apart: they are neither attempts nor decisions.
        self.history: list[tuple[str, str, Reading]] = []
        # The recording order of both, like the Postgres ids: it breaks as_of ties in daily().
        self._order: list[tuple[str, int]] = []
        self.busy: set[str] = set()

    async def record(self, observation: Observation) -> None:
        self._order.append(("observation", len(self.observations)))
        self.observations.append(observation)

    def _of(self, series_id: str) -> list[Observation]:
        # In the order they were recorded, like the Postgres store's ids.
        return [o for o in self.observations if o.series_id == series_id]

    async def state(self, series_id: str, since: datetime) -> SeriesState:
        last: Decimal | None = None
        suspects: list[Held] = []
        for o in self._of(series_id):
            match o.outcome:
                case Accepted(reading=reading):
                    last, suspects = reading.value, []
                case Suspect(reading=reading, why=why) if o.fetched_at >= since:
                    suspects.append(Held(reading.value, why))
                case Suspect() | Control() | Rejected():
                    pass
        return SeriesState(last_accepted=last, suspects=suspects)

    async def latest(self, series_id: str) -> Latest:
        history = self._of(series_id)
        published = None
        for o in history:
            if isinstance(o.outcome, Accepted):
                reading = o.outcome.reading
                published = Published(reading.value, reading.as_of, o.fetched_at, o.source)
        valid = [o for o in history if isinstance(o.outcome, Accepted | Suspect)]
        newest = valid[-1] if valid else None
        return Latest(
            published=published,
            suspect_at=newest.fetched_at
            if newest is not None and isinstance(newest.outcome, Suspect)
            else None,
            last_attempt_at=history[-1].fetched_at if history else None,
        )

    async def record_history(
        self, series_id: str, source: str, fetched_at: datetime, readings: Sequence[Reading]
    ) -> None:
        for reading in readings:
            self._order.append(("history", len(self.history)))
            self.history.append((series_id, source, reading))

    async def has_history(self, series_id: str) -> bool:
        return any(series == series_id for series, _, _ in self.history)

    async def daily(self, series_id: str, first: date, last: date, zone: ZoneInfo) -> list[Day]:
        # The value with the latest as_of each day; on a tie, the one recorded last.
        by_day: dict[date, Day] = {}
        for kind, index in self._order:
            if kind == "history":
                series, source, reading = self.history[index]
            else:
                o = self.observations[index]
                if not isinstance(o.outcome, Accepted):
                    continue
                series, source, reading = o.series_id, o.source, o.outcome.reading
            day = reading.as_of.astimezone(zone).date()
            if series != series_id or not first <= day <= last:
                continue
            if day not in by_day or reading.as_of >= by_day[day].as_of:
                by_day[day] = Day(day, reading.value, reading.as_of, source)
        return [by_day[day] for day in sorted(by_day)]

    async def attempted_in(self, series_id: str, start: datetime, end: datetime) -> bool:
        history = self._of(series_id)
        return bool(history) and start <= history[-1].fetched_at < end

    @asynccontextmanager
    async def exclusive(self, series_id: str) -> AsyncIterator[bool]:
        if series_id in self.busy:
            yield False
            return
        self.busy.add(series_id)
        try:
            yield True
        finally:
            self.busy.discard(series_id)
