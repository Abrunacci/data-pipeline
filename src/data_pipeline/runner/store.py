"""What running a series needs from where it keeps its state. ``runner.memory`` keeps it in
memory."""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from data_pipeline.core.readings import Held, Observation


@dataclass(frozen=True, slots=True)
class SeriesState:
    """What the runner needs to decide on a new reading.

    ``suspects`` are the values held back since the last accepted one and not expired, oldest
    first.
    """

    last_accepted: Decimal | None
    suspects: Sequence[Held]


class SeriesStore(Protocol):
    async def record(self, observation: Observation) -> None: ...

    async def state(self, series_id: str, since: datetime) -> SeriesState:
        """The last accepted value, and the suspects after it fetched at or after ``since``."""
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
