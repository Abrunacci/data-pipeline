"""What a source reports, and what the pipeline decided about it."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


@dataclass(frozen=True, slots=True)
class Reading:
    """A value as a source reports it, before any check.

    ``as_of`` is when the source says the value is from, not when it was fetched.
    """

    value: Decimal
    as_of: datetime

    def __post_init__(self) -> None:
        if self.as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")


class Rejection(StrEnum):
    """Why a reading, or the attempt to get one, was not used."""

    FETCH_FAILED = "fetch_failed"
    MALFORMED = "malformed"
    # The source's own code failed: a bug to fix, not the source's answer.
    SOURCE_BUG = "source_bug"
    NOT_FINITE = "not_finite"
    NOT_POSITIVE = "not_positive"
    TOO_LARGE = "too_large"
    TOO_MANY_DECIMALS = "too_many_decimals"
    IMPLAUSIBLE = "implausible"
    NO_QUOTE = "no_quote"
    STALE = "stale"
    FROM_THE_FUTURE = "from_the_future"


@dataclass(frozen=True, slots=True)
class Accepted:
    """A reading that is published. ``confirmed`` is True when it was a suspect that confirmed
    itself, and ``detail`` says how."""

    reading: Reading
    confirmed: bool = False
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class Suspect:
    """A valid reading held back until it is confirmed; ``detail`` says why."""

    reading: Reading
    detail: str


@dataclass(frozen=True, slots=True)
class Control:
    """A valid reading from a series' control source. It is never published: it only tells
    whether the source's reading can be trusted."""

    reading: Reading


@dataclass(frozen=True, slots=True)
class Rejected:
    """A failed attempt. ``reading`` is kept for the record when the source gave one."""

    reason: Rejection
    detail: str
    reading: Reading | None = None


type Outcome = Accepted | Suspect | Control | Rejected


@dataclass(frozen=True, slots=True)
class Observation:
    """One attempt to read a series from one source, and its outcome."""

    series_id: str
    source: str
    fetched_at: datetime
    outcome: Outcome

    def __post_init__(self) -> None:
        if self.fetched_at.tzinfo is None:
            raise ValueError("fetched_at must be timezone-aware")
