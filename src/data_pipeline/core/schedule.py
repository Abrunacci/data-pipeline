"""Opening hours: when a series runs, and which time counts towards the age of its values.

A market's price does not change while it is closed, so a Friday closing price read on Monday
morning is not stale. Ages are counted in open time only. Holidays are not known: on a holiday
the value ages as on any other weekday.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

# The calculator's users' zone: days of the history, the MEP's hours, Argentine sources' times.
BUENOS_AIRES = ZoneInfo("America/Argentina/Buenos_Aires")


@dataclass(frozen=True, slots=True)
class OpeningHours:
    """``weekdays`` are 0 (Monday) to 6, and the hours are ``[opens, closes]`` in ``zone``,
    both ends included, so a run at the closing minute still happens."""

    weekdays: frozenset[int]
    opens: time
    closes: time
    zone: ZoneInfo

    def __post_init__(self) -> None:
        if not self.weekdays or not self.weekdays <= set(range(7)):
            raise ValueError(f"weekdays must be some of 0 to 6, got {sorted(self.weekdays)}")
        if self.opens.tzinfo is not None or self.closes.tzinfo is not None:
            raise ValueError("opens and closes are local times: the zone goes in zone")
        if not self.opens < self.closes:
            raise ValueError(f"opens ({self.opens}) must be before closes ({self.closes})")

    def is_open(self, moment: datetime) -> bool:
        local = moment.astimezone(self.zone)
        return local.weekday() in self.weekdays and self.opens <= local.time() <= self.closes

    def open_time(self, start: datetime, end: datetime) -> timedelta:
        """How much of ``[start, end]`` falls inside opening hours."""
        if end <= start:
            return timedelta(0)
        total = timedelta(0)
        day = start.astimezone(self.zone).date()
        last = end.astimezone(self.zone).date()
        while day <= last:
            if day.weekday() in self.weekdays:
                opens = datetime.combine(day, self.opens, self.zone)
                closes = datetime.combine(day, self.closes, self.zone)
                overlap = min(end, closes) - max(start, opens)
                total += max(overlap, timedelta(0))
            day += timedelta(days=1)
        return total
