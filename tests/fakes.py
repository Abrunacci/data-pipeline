"""An in-memory ``Store`` with the same rules as the Postgres one, for tests of the runner."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal

from data_pipeline.core.readings import Accepted, Observation, Rejected, Suspect
from data_pipeline.runner.store import Latest, Published, SeriesState


class MemoryStore:
    def __init__(self) -> None:
        self.observations: list[Observation] = []
        self.busy: set[str] = set()

    async def record(self, observation: Observation) -> None:
        self.observations.append(observation)

    def _of(self, series_id: str) -> list[Observation]:
        # In the order they were recorded, like the Postgres store's ids.
        return [o for o in self.observations if o.series_id == series_id]

    async def state(self, series_id: str) -> SeriesState:
        last: Decimal | None = None
        suspects: list[Decimal] = []
        for o in self._of(series_id):
            match o.outcome:
                case Accepted(reading=reading):
                    last, suspects = reading.value, []
                case Suspect(reading=reading):
                    suspects.append(reading.value)
                case Rejected():
                    pass
        return SeriesState(last_accepted=last, suspects=suspects)

    async def latest(self, series_id: str) -> Latest:
        history = self._of(series_id)
        accepted = [o for o in history if isinstance(o.outcome, Accepted)]
        valid = [o for o in history if not isinstance(o.outcome, Rejected)]
        published = None
        if accepted:
            last = accepted[-1]
            assert isinstance(last.outcome, Accepted)
            reading = last.outcome.reading
            published = Published(reading.value, reading.as_of, last.fetched_at, last.source)
        return Latest(
            published=published,
            pending=bool(valid) and isinstance(valid[-1].outcome, Suspect),
            last_attempt_at=history[-1].fetched_at if history else None,
        )

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
