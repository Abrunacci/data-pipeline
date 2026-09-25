"""How much less a trade gets than an indicative price promises, from observed pairs.

Binance lists a card price before the purchase and shows the final one only on the last screen.
Each observed pair (what the list promised, what the final screen gave, for the same amount)
gives one gap; the published gap is their median, with how many there are and their dates.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from statistics import median


@dataclass(frozen=True, slots=True)
class GapSample:
    """One observation: for ``amount`` paid, the list promised ``listed`` and the final screen
    gave ``final``, both in the bought currency, fees included."""

    observed_at: datetime
    amount: Decimal
    listed: Decimal
    final: Decimal

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        amounts = (self.amount, self.listed, self.final)
        if not all(amount.is_finite() and amount > 0 for amount in amounts):
            raise ValueError(f"amounts must be positive and finite: {self}")
        if self.final > self.listed:
            raise ValueError(f"final ({self.final}) is more than listed ({self.listed})")

    @property
    def gap(self) -> Decimal:
        """The fraction of the listed amount that did not arrive: 0.0432 is 4.32 %."""
        return 1 - self.final / self.listed


@dataclass(frozen=True, slots=True)
class Gap:
    fraction: Decimal
    samples: int
    first: datetime
    last: datetime


def summarize(samples: Sequence[GapSample]) -> Gap | None:
    """The median gap of ``samples``, or None when there are none."""
    if not samples:
        return None
    moments = [sample.observed_at for sample in samples]
    return Gap(
        fraction=Decimal(median(sample.gap for sample in samples)),
        samples=len(samples),
        first=min(moments),
        last=max(moments),
    )
