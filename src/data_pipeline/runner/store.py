"""What the runner and the API need from storage. Postgres implements it in ``storage``."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol
from zoneinfo import ZoneInfo

from data_pipeline.core.readings import Held, Observation, Reading


@dataclass(frozen=True, slots=True)
class Published:
    """The value of a series as it is published: the last accepted reading."""

    value: Decimal
    as_of: datetime
    fetched_at: datetime
    source: str


@dataclass(frozen=True, slots=True)
class Day:
    """A series on one day: its last published or loaded value of that day."""

    date: date
    value: Decimal
    as_of: datetime
    source: str


@dataclass(frozen=True, slots=True)
class SeriesState:
    """What the runner needs to decide on a new reading.

    ``suspects`` are the values held back since the last accepted one and not expired, oldest
    first.
    """

    last_accepted: Decimal | None
    suspects: Sequence[Held]


@dataclass(frozen=True, slots=True)
class Latest:
    """What the API shows for a series. ``suspect_at`` is when the newest valid reading was
    fetched, if it is a suspect: the published value may be about to change."""

    published: Published | None
    suspect_at: datetime | None
    last_attempt_at: datetime | None


class Store(Protocol):
    async def record(self, observation: Observation) -> None: ...

    async def state(self, series_id: str, since: datetime) -> SeriesState:
        """The last accepted value, and the suspects after it fetched at or after ``since``."""
        ...

    async def latest(self, series_id: str) -> Latest: ...

    async def record_history(
        self, series_id: str, source: str, fetched_at: datetime, readings: Sequence[Reading]
    ) -> None:
        """Store past values loaded from a history source, all at once."""
        ...

    async def has_history(self, series_id: str) -> bool:
        """Whether past values were ever loaded for the series."""
        ...

    async def daily(self, series_id: str, first: date, last: date, zone: ZoneInfo) -> Sequence[Day]:
        """One value per day from ``first`` to ``last`` (days in ``zone``), for the days that
        have one: the published or loaded value with the latest ``as_of`` of that day."""
        ...

    async def attempted_in(self, series_id: str, start: datetime, end: datetime) -> bool:
        """Whether the last attempt recorded for the series was fetched in ``[start, end)``.

        A last attempt after ``end`` does not count: it was stamped by a clock that ran ahead
        and has since been corrected, and the current slot still has to run.
        """
        ...

    def exclusive(self, series_id: str) -> AbstractAsyncContextManager[bool]:
        """Try to become the only runner of the series; yields False if another one is."""
        ...
