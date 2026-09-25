"""What the API returns. Amounts travel as decimal strings, never as JSON numbers."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from data_pipeline.core.checks import Rules
from data_pipeline.runner.store import Latest


class Rate(BaseModel):
    """The published value of a series.

    - ``as_of``: when the source says the value is from. Show this one to people.
    - ``fetched_at``: when the pipeline read it.
    - ``stale``: ``as_of`` is older than the series allows; the sources are failing.
    - ``pending_confirmation``: the newest reading moved too far from this value and waits
      for the next readings to confirm it; this value may be about to change.
    """

    value: str
    as_of: datetime
    fetched_at: datetime
    source: str
    stale: bool
    pending_confirmation: bool
    last_attempt_at: datetime | None


class LatestRates(BaseModel):
    """Every configured series by id; null for one that has no accepted value yet."""

    rates: dict[str, Rate | None]


class Health(BaseModel):
    status: Literal["ok", "database_unavailable"]


def latest_rate(latest: Latest, rules: Rules, now: datetime) -> Rate | None:
    published = latest.published
    if published is None:
        return None
    return Rate(
        value=format(published.value, "f"),
        as_of=published.as_of,
        fetched_at=published.fetched_at,
        source=published.source,
        stale=now - published.as_of > rules.max_age,
        pending_confirmation=latest.pending,
        last_attempt_at=latest.last_attempt_at,
    )
