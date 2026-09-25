"""What the runner and the API need from storage. Postgres implements it in ``storage``."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from data_pipeline.core.readings import Observation


@dataclass(frozen=True, slots=True)
class Published:
    """The value of a series as it is published: the last accepted reading."""

    value: Decimal
    as_of: datetime
    fetched_at: datetime
    source: str


@dataclass(frozen=True, slots=True)
class SeriesState:
    """What the runner needs to decide on a new reading.

    ``suspects`` are the values held back since the last accepted one, oldest first.
    """

    last_accepted: Decimal | None
    suspects: Sequence[Decimal]


@dataclass(frozen=True, slots=True)
class Latest:
    """What the API shows for a series. ``pending`` is True when the newest valid reading is a
    suspect, so the published value may be about to change."""

    published: Published | None
    pending: bool
    last_attempt_at: datetime | None


class Store(Protocol):
    async def record(self, observation: Observation) -> None: ...

    async def state(self, series_id: str) -> SeriesState: ...

    async def latest(self, series_id: str) -> Latest: ...

    async def attempted_in(self, series_id: str, start: datetime, end: datetime) -> bool:
        """Whether the last attempt recorded for the series was fetched in ``[start, end)``.

        A last attempt after ``end`` does not count: it was stamped by a clock that ran ahead
        and has since been corrected, and the current slot still has to run.
        """
        ...

    def exclusive(self, series_id: str) -> AbstractAsyncContextManager[bool]:
        """Try to become the only runner of the series; yields False if another one is."""
        ...
