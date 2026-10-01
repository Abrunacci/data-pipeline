from __future__ import annotations

from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal
from fractions import Fraction

import pytest

from data_pipeline.core.gap import Gap, GapSample, estimate_final, summarize

WHEN = datetime(2026, 9, 25, 18, 18, tzinfo=UTC)


def sample(amount: str, fee: str, listed: str, final: str) -> GapSample:
    return GapSample(WHEN, Decimal(amount), Decimal(fee), Decimal(listed), Decimal(final))


def test_the_observed_purchase_gives_a_2_37_percent_price_gap() -> None:
    # 2026-09-25: 10 USD, 0.20 fee, 9.7763382 USDT listed, 9.35393217 received.
    # Final price 9.35393217 / 9.80 = 0.954482874...; listed 0.97763382; 1 - ratio = 0.023676...
    # With the fee, the total shortfall would be 4.32 %: the fee is not in the price gap.
    gap = sample("10", "0.20", "9.7763382", "9.35393217").gap
    assert gap.quantize(Decimal("0.0001"), ROUND_HALF_EVEN) == Decimal("0.0237")


def test_a_final_price_equal_to_the_list_is_no_gap() -> None:
    # 98 USD of price after a 2 USD fee, at the listed 1 USDT per USD.
    assert sample("100", "2", "100", "98").gap == 0


def test_the_median_is_published_with_the_dates() -> None:
    days = [datetime(2026, 9, day, 15, tzinfo=UTC) for day in (29, 25, 27)]
    samples = [
        GapSample(day, Decimal(100), Decimal(2), Decimal(100), Decimal(final))
        for day, final in zip(days, ("97", "96", "90"), strict=True)
    ]
    summary = summarize(samples)
    assert summary is not None
    assert summary.kept == Fraction(96, 98)
    assert summary.samples == 3
    assert (summary.first, summary.last) == (days[1], days[0])


@pytest.mark.parametrize(
    ("amount", "fee", "listed", "final", "message"),
    [
        ("10", "0.2", "9.77", "0", "positive"),
        ("10", "10", "9.77", "9.35", "less than the amount"),
        ("10", "-0.2", "9.77", "9.35", "at least 0"),
        ("100", "2", "100", "99", "better than"),
    ],
)
def test_impossible_observations_are_refused(
    amount: str, fee: str, listed: str, final: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        sample(amount, fee, listed, final)


def test_the_estimated_final_price_is_rounded_down() -> None:
    gap = summarize([sample("10", "0.20", "9.7763382", "9.35393217")])
    assert gap is not None
    # 0.97768597 * (1 - 0.023676...) = 0.954533789547...: rounded down, not up to ...79.
    assert estimate_final(Decimal("0.97768597"), gap) == Decimal("0.95453378")
    # On the observed purchase's own listed price it gives back its final price,
    # 9.35393217 / 9.80 = 0.95448287..., rounded down.
    assert estimate_final(Decimal("0.97763382"), gap) == Decimal("0.95448287")


def test_an_estimate_exactly_on_a_step_is_not_one_step_low() -> None:
    # 10 USD, no fee, 99.8704998 listed, 98.0728308 final: on the listed price per unit the
    # exact estimate is the final price per unit, 9.80728308, exactly on a step. A gap rounded
    # to 28 digits before multiplying gave 9.80728307.
    gap = summarize([sample("10", "0", "99.8704998", "98.0728308")])
    assert gap is not None
    assert estimate_final(Decimal("9.98704998"), gap) == Decimal("9.80728308")


def test_the_gap_must_keep_part_of_the_price() -> None:
    with pytest.raises(ValueError, match="above 0 and at most 1"):
        Gap(Fraction(0), 1, WHEN, WHEN)
    with pytest.raises(ValueError, match="above 0 and at most 1"):
        Gap(Fraction(11, 10), 1, WHEN, WHEN)
