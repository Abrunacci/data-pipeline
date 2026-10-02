"""Where accepted values go: the app the series feeds. ``destinations`` implements it."""

from __future__ import annotations

from typing import Protocol

from data_pipeline.core.readings import Observation
from data_pipeline.core.series import Series


class Destination(Protocol):
    async def send(self, series: Series, observation: Observation) -> None:
        """Hand on one accepted reading of ``series``; its outcome is ``Accepted``.

        Raising is allowed: the scheduler logs the error and the value is dropped. There is no
        queue, because the next run sends a newer value.
        """
        ...
