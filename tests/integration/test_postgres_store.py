from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from data_pipeline.core.readings import (
    Accepted,
    Observation,
    Outcome,
    Reading,
    Rejected,
    Rejection,
    Suspect,
)
from data_pipeline.storage.postgres import PostgresStore
from data_pipeline.storage.tables import observations

pytestmark = pytest.mark.anyio

T0 = datetime(2026, 9, 25, 18, 0, tzinfo=UTC)


def at(minutes: int, outcome: Outcome, series_id: str = "rate") -> Observation:
    return Observation(series_id, "source", T0 + timedelta(minutes=minutes), outcome)


def reading(value: str, minutes: int = 0) -> Reading:
    return Reading(Decimal(value), T0 + timedelta(minutes=minutes))


async def test_an_empty_series_has_nothing_published(engine: AsyncEngine) -> None:
    store = PostgresStore(engine)
    latest = await store.latest("rate")
    assert latest.published is None
    assert not latest.pending
    assert latest.last_attempt_at is None
    state = await store.state("rate")
    assert state.last_accepted is None
    assert list(state.suspects) == []


async def test_the_last_accepted_value_is_published_exactly(engine: AsyncEngine) -> None:
    store = PostgresStore(engine)
    await store.record(at(0, Accepted(reading("1600.12345678", 0))))
    await store.record(at(10, Accepted(reading("1615.3", 9))))
    await store.record(at(20, Rejected(Rejection.FETCH_FAILED, "HTTP 503")))

    latest = await store.latest("rate")
    assert latest.published is not None
    assert latest.published.value == Decimal("1615.3")
    assert latest.published.as_of == T0 + timedelta(minutes=9)
    assert latest.published.fetched_at == T0 + timedelta(minutes=10)
    assert latest.published.source == "source"
    assert latest.last_attempt_at == T0 + timedelta(minutes=20)
    assert not latest.pending


async def test_state_lists_the_suspects_since_the_last_accepted(engine: AsyncEngine) -> None:
    store = PostgresStore(engine)
    await store.record(at(0, Suspect(reading("1500"))))
    await store.record(at(10, Accepted(reading("1600"))))
    await store.record(at(20, Suspect(reading("1800"))))
    await store.record(at(30, Rejected(Rejection.MALFORMED, "html")))
    await store.record(at(40, Suspect(reading("1801"))))
    await store.record(at(50, Accepted(reading("9999"), confirmed=True), series_id="other"))

    state = await store.state("rate")
    assert state.last_accepted == Decimal(1600)
    assert list(state.suspects) == [Decimal(1800), Decimal(1801)]
    assert (await store.latest("rate")).pending


async def test_a_confirmed_reading_is_published(engine: AsyncEngine) -> None:
    store = PostgresStore(engine)
    await store.record(at(0, Accepted(reading("1600"))))
    await store.record(at(10, Suspect(reading("1800"))))
    await store.record(at(20, Accepted(reading("1801"), confirmed=True)))

    latest = await store.latest("rate")
    assert latest.published is not None
    assert latest.published.value == Decimal(1801)
    assert not latest.pending
    assert list((await store.state("rate")).suspects) == []


async def test_a_rejected_reading_keeps_the_value_the_source_sent(engine: AsyncEngine) -> None:
    store = PostgresStore(engine)
    await store.record(at(0, Rejected(Rejection.IMPLAUSIBLE, "too low", reading("16.00"))))
    async with engine.connect() as connection:
        row = (await connection.execute(observations.select())).one()
    assert row.status == "rejected"
    assert str(row.value) == "16.00"
    assert row.reason == "implausible"


async def test_attempted_since_counts_every_outcome(engine: AsyncEngine) -> None:
    store = PostgresStore(engine)
    await store.record(at(10, Rejected(Rejection.FETCH_FAILED, "timeout")))
    assert await store.attempted_since("rate", T0 + timedelta(minutes=10))
    assert not await store.attempted_since("rate", T0 + timedelta(minutes=11))
    assert not await store.attempted_since("other", T0)


async def test_only_one_runner_holds_a_series(engine: AsyncEngine) -> None:
    first, second = PostgresStore(engine), PostgresStore(engine)
    async with first.exclusive("rate") as alone:
        assert alone
        async with second.exclusive("rate") as also:
            assert not also
        async with second.exclusive("other") as other:
            assert other
    async with second.exclusive("rate") as after:
        assert after


async def test_the_schema_refuses_an_accepted_row_without_a_value(engine: AsyncEngine) -> None:
    with pytest.raises(IntegrityError, match="value_unless_rejected"):
        async with engine.begin() as connection:
            await connection.execute(
                observations.insert().values(
                    series_id="rate", source="s", fetched_at=T0, status="accepted"
                )
            )
