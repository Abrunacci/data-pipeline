"""The checks a reading goes through before it is published.

They run in order: the value itself (``value_problem``), then whether it is plausible and
fresh (``reading_problem``). Each reading is checked on its own: nothing compares it with the
readings before it, which the runner does not keep. A malformed response never gets here:
sources refuse it when they parse it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from data_pipeline.core.readings import Reading, Rejection

# The calculator in cuanto-cuesta accepts prices greater than zero, up to 1,000,000, with at most
# 8 decimals (frontend/src/calculator/inputs.ts). A published value must be one it accepts.
MAX_VALUE = Decimal(1_000_000)
MAX_DECIMALS = 8
# The smallest step a published value can have: 0.00000001.
SMALLEST_STEP = Decimal(1).scaleb(-MAX_DECIMALS)

# Clocks differ a little between a source and this server; more than this is a wrong timestamp.
CLOCK_SKEW = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class Range:
    """The closed range a value can plausibly be in. It catches wrong fields and units."""

    min: Decimal
    max: Decimal

    def __post_init__(self) -> None:
        if not self.min.is_finite() or not self.max.is_finite() or not 0 < self.min < self.max:
            raise ValueError(f"need 0 < min < max, got {self.min} and {self.max}")

    def __contains__(self, value: Decimal) -> bool:
        return self.min <= value <= self.max


@dataclass(frozen=True, slots=True)
class Rules:
    """How a reading of a series is checked: its plausible range, and how old it may be."""

    plausible: Range
    max_age: timedelta

    def __post_init__(self) -> None:
        if self.max_age <= timedelta(0):
            raise ValueError(f"max_age must be positive, got {self.max_age}")


def canonical(value: Decimal) -> Decimal:
    """``value`` without trailing zeros, and with no positive exponent: 1615.300 → 1615.3,
    1E+3 → 1000. Write it with ``format(value, "f")``: ``str`` uses exponents for tiny values.

    It works on the digits, not with ``normalize()``, which rounds to the context's precision
    (28 digits) and would change a long value instead of just dropping its zeros.
    """
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):  # NaN and infinities have a str exponent
        raise ValueError(f"not a finite number: {value}")
    kept = list(digits)
    if not any(kept):
        return Decimal((sign, (0,), 0))
    while exponent < 0 and kept[-1] == 0:
        kept.pop()
        exponent += 1
    if exponent > 0:
        kept.extend([0] * exponent)
        exponent = 0
    return Decimal((sign, tuple(kept), exponent))


def decimals(value: Decimal) -> int:
    """How many decimals a finite ``value`` has once trailing zeros are dropped."""
    exponent = canonical(value).as_tuple().exponent
    assert isinstance(exponent, int)  # canonical refuses NaN and infinities
    return -exponent


def value_problem(value: Decimal) -> tuple[Rejection, str] | None:
    """Why ``value`` cannot be published as a price, or None if it can."""
    if not value.is_finite():
        return Rejection.NOT_FINITE, f"got {value}"
    if value <= 0:
        return Rejection.NOT_POSITIVE, f"got {value}"
    if value > MAX_VALUE:
        return Rejection.TOO_LARGE, f"must be at most {MAX_VALUE}, got {value}"
    if decimals(value) > MAX_DECIMALS:
        return Rejection.TOO_MANY_DECIMALS, f"must have at most {MAX_DECIMALS}, got {value}"
    return None


def reading_problem(
    reading: Reading, rules: Rules, fetched_at: datetime, age: timedelta
) -> tuple[Rejection, str] | None:
    """Why ``reading`` cannot be used, or None if it can.

    ``age`` is how old the reading is when it is fetched, as the series counts it: for a market
    with opening hours, only the time the market was open counts.
    """
    if (problem := value_problem(reading.value)) is not None:
        return problem
    if reading.value not in rules.plausible:
        return (
            Rejection.IMPLAUSIBLE,
            f"expected {rules.plausible.min} to {rules.plausible.max}, got {reading.value}",
        )
    if reading.as_of > fetched_at + CLOCK_SKEW:
        return Rejection.FROM_THE_FUTURE, f"as_of {reading.as_of}, fetched at {fetched_at}"
    if age > rules.max_age:
        return Rejection.STALE, f"as_of {reading.as_of} is {age} old, more than {rules.max_age}"
    return None
