"""A series: one value tracked over time, where it comes from and how it is checked."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from data_pipeline.core.checks import Rules
from data_pipeline.core.gap import Gap
from data_pipeline.core.schedule import OpeningHours

# A suspect older than this many intervals no longer counts towards confirming a new reading:
# after an outage, two readings must agree again before a jump is published.
SUSPECTS_EXPIRE_AFTER_INTERVALS = 3


@dataclass(frozen=True, slots=True)
class Series:
    """``id`` is the public name of the value: consumers look it up by it, so it never changes.

    - ``sources`` are tried in order until one gives a valid reading.
    - ``control`` is read every run to check that reading, never published. It is skipped when
      the reading came from the control source itself.
    - ``hours``: the series only runs, and its values only age, while they are open. None is
      always open.
    - ``official_source``: False when the source is not a documented, official one.
    - ``history``: the history source its past values are loaded from, once.
    - ``indicative``: the value is a reference price, not the one a trade gets. ``gap`` is how
      much less a trade got, from observed pairs, when there are any.
    """

    id: str
    description: str
    sources: tuple[str, ...]
    every: timedelta
    rules: Rules
    control: str | None = None
    hours: OpeningHours | None = None
    official_source: bool = True
    indicative: bool = False
    gap: Gap | None = None
    history: str | None = None

    def __post_init__(self) -> None:
        if not self.sources:
            raise ValueError(f"series {self.id} has no sources")
        if len(set(self.sources)) != len(self.sources):
            raise ValueError(f"series {self.id} lists a source twice")
        if self.every <= timedelta(0):
            raise ValueError(f"series {self.id}: every must be positive, got {self.every}")
        if self.gap is not None and not self.indicative:
            raise ValueError(f"series {self.id}: only an indicative series has a gap")
        if self.control is not None and self.control in self.sources[:1]:
            raise ValueError(f"series {self.id}: the primary source cannot be its own control")
        # Otherwise a healthy source would be rejected as stale between two runs.
        if self.rules.max_age < self.every:
            raise ValueError(
                f"series {self.id}: max_age ({self.rules.max_age}) is shorter than every"
                f" ({self.every})"
            )

    @property
    def suspects_expire_after(self) -> timedelta:
        return self.every * SUSPECTS_EXPIRE_AFTER_INTERVALS

    def runs_at(self, moment: datetime) -> bool:
        return self.hours is None or self.hours.is_open(moment)

    def age(self, as_of: datetime, moment: datetime) -> timedelta:
        """How old a value from ``as_of`` is at ``moment``, counting only open time."""
        if self.hours is None:
            return moment - as_of
        return self.hours.open_time(as_of, moment)
