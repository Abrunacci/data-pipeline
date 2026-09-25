"""What the API returns. Amounts travel as decimal strings, never as JSON numbers."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel

from data_pipeline.core.gap import Gap
from data_pipeline.core.series import Series
from data_pipeline.runner.store import Latest


class FinalPriceGap(BaseModel):
    """How much less a trade got than the indicative price, from observed pairs: the median of
    ``samples`` observations between ``first`` and ``last``. ``percent`` 4.32 is 4.32 %."""

    percent: str
    samples: int
    first: datetime
    last: datetime


class Rate(BaseModel):
    """A series as it is published. ``value`` and the fields about it are null until a first
    value is accepted.

    - ``as_of``: when the source says the value is from. Show this one to people.
    - ``fetched_at``: when the pipeline read it; ``source``: which source gave it.
    - ``stale``: there is no value, or ``as_of`` is older than the series allows (counting only
      opening hours for a market): the sources are failing.
    - ``pending_confirmation``: the newest reading was held back and waits for the next ones
      or the control source to confirm it; the value may be about to change.
    - ``last_attempt_at``: the last time any source was asked, whatever the result.
    - ``official_source``: False when the value comes from an undocumented source.
    - ``indicative``: a reference price, not what a trade gets; ``final_price_gap`` says how
      much less a trade got, when there are observations.
    """

    value: str | None
    as_of: datetime | None
    fetched_at: datetime | None
    source: str | None
    stale: bool
    pending_confirmation: bool
    last_attempt_at: datetime | None
    official_source: bool
    indicative: bool
    final_price_gap: FinalPriceGap | None


class LatestRates(BaseModel):
    """Every configured series, by id."""

    rates: dict[str, Rate]


class Health(BaseModel):
    status: Literal["ok", "database_unavailable"]


def latest_rate(latest: Latest, series: Series, now: datetime) -> Rate:
    published = latest.published
    pending = latest.suspect_at is not None and (
        now - latest.suspect_at < series.suspects_expire_after
    )
    return Rate(
        value=None if published is None else format(published.value, "f"),
        as_of=None if published is None else published.as_of,
        fetched_at=None if published is None else published.fetched_at,
        source=None if published is None else published.source,
        stale=published is None or series.age(published.as_of, now) > series.rules.max_age,
        pending_confirmation=pending,
        last_attempt_at=latest.last_attempt_at,
        official_source=series.official_source,
        indicative=series.indicative,
        final_price_gap=None if series.gap is None else _gap(series.gap),
    )


def _gap(gap: Gap) -> FinalPriceGap:
    return FinalPriceGap(
        percent=format((gap.fraction * 100).quantize(Decimal("0.01")), "f"),
        samples=gap.samples,
        first=gap.first,
        last=gap.last,
    )
