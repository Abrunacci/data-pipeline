from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from data_pipeline.core.readings import (
    Accepted,
    Control,
    Held,
    HeldBack,
    Observation,
    Outcome,
    Reading,
    Rejected,
    Rejection,
    Suspect,
)
from data_pipeline.runner.memory import MemoryState

pytestmark = pytest.mark.anyio

NOW = datetime(2026, 10, 2, 15, 0, tzinfo=UTC)


def observed(outcome: Outcome, minutes: int = 0, series_id: str = "rate") -> Observation:
    at = NOW + timedelta(minutes=minutes)
    return Observation(series_id, "primary", at, outcome)


def reading(value: str) -> Reading:
    return Reading(Decimal(value), NOW)


async def test_a_series_never_seen_has_no_reference() -> None:
    state = await MemoryState().state("rate", since=NOW)
    assert state.last_accepted is None
    assert state.suspects == []


async def test_accepting_a_value_drops_the_suspects_before_it() -> None:
    memory = MemoryState()
    await memory.record(observed(Accepted(reading("1600"))))
    await memory.record(observed(Suspect(reading("1800"), HeldBack.JUMP, "jump"), 10))
    state = await memory.state("rate", since=NOW)
    assert state.last_accepted == Decimal(1600)
    assert state.suspects == [Held(Decimal(1800), HeldBack.JUMP)]

    await memory.record(observed(Accepted(reading("1810"), confirmed=True, detail="moved"), 20))
    state = await memory.state("rate", since=NOW)
    assert state.last_accepted == Decimal(1810)
    assert state.suspects == []


async def test_suspects_fetched_before_since_are_dropped() -> None:
    memory = MemoryState()
    await memory.record(observed(Suspect(reading("1800"), HeldBack.JUMP, "jump"), 0))
    await memory.record(observed(Suspect(reading("1801"), HeldBack.JUMP, "jump"), 10))
    since = NOW + timedelta(minutes=10)
    assert (await memory.state("rate", since)).suspects == [Held(Decimal(1801), HeldBack.JUMP)]
    # Gone for good, not only filtered: memory stays bounded.
    assert (await memory.state("rate", NOW)).suspects == [Held(Decimal(1801), HeldBack.JUMP)]


async def test_controls_and_rejections_change_no_decision_but_count_as_attempts() -> None:
    memory = MemoryState()
    await memory.record(observed(Accepted(reading("1600"))))
    await memory.record(observed(Control(reading("1700")), 10))
    await memory.record(observed(Rejected(Rejection.FETCH_FAILED, "timeout"), 11))
    assert (await memory.state("rate", NOW)).last_accepted == Decimal(1600)
    start = NOW + timedelta(minutes=10)
    assert await memory.attempted_in("rate", start, start + timedelta(minutes=10))
    assert not await memory.attempted_in("rate", NOW, start)
    assert not await memory.attempted_in("other", NOW, start)


async def test_series_are_kept_apart() -> None:
    memory = MemoryState()
    await memory.record(observed(Accepted(reading("1600")), series_id="a"))
    assert (await memory.state("b", NOW)).last_accepted is None


async def test_only_one_run_of_a_series_at_a_time() -> None:
    memory = MemoryState()
    async with memory.exclusive("rate") as first:
        async with memory.exclusive("rate") as second:
            assert first
            assert not second
        async with memory.exclusive("other") as other:
            assert other
    async with memory.exclusive("rate") as again:
        assert again


async def test_every_attempt_is_logged_and_a_first_value_says_it_had_no_reference(
    caplog: pytest.LogCaptureFixture,
) -> None:
    memory = MemoryState()
    with caplog.at_level("INFO", logger="data_pipeline.runner.memory"):
        await memory.record(observed(Rejected(Rejection.MALFORMED, "html")))
        await memory.record(observed(Accepted(reading("1600.5")), 1))
        await memory.record(observed(Accepted(reading("1601")), 10))
        await memory.record(observed(Suspect(reading("1900"), HeldBack.JUMP, "18.7 %"), 20))
        await memory.record(observed(Control(reading("1602")), 21))
    assert caplog.messages == [
        "rate primary rejected, malformed: html",
        "rate primary accepted 1600.5 (no reference)",
        "rate primary accepted 1601 (checked)",
        "rate primary held back 1900, jump: 18.7 %",
        "rate primary control 1602",
    ]
