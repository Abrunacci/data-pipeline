from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import Connection, create_engine, select, text, update
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from data_pipeline.core.checks import Range, Rules
from data_pipeline.core.readings import (
    Accepted,
    Observation,
    Outcome,
    Reading,
    Rejected,
    Rejection,
    Suspect,
)
from data_pipeline.core.series import Series
from data_pipeline.runner.collect import collect
from data_pipeline.runner.scheduler import run_slot
from data_pipeline.sources.bitso import BitsoBid
from data_pipeline.storage.postgres import PostgresStore
from data_pipeline.storage.tables import Status, metadata, observations
from tests.integration.conftest import Urls

pytestmark = pytest.mark.anyio

T0 = datetime(2026, 9, 25, 18, 0, tzinfo=UTC)


def at(minutes: int, outcome: Outcome, series_id: str = "rate") -> Observation:
    return Observation(series_id, "source", T0 + timedelta(minutes=minutes), outcome)


def reading(value: str, minutes: int = 0) -> Reading:
    return Reading(Decimal(value), T0 + timedelta(minutes=minutes))


async def test_an_empty_series_has_nothing_published(store: PostgresStore) -> None:
    latest = await store.latest("rate")
    assert latest.published is None
    assert not latest.pending
    assert latest.last_attempt_at is None
    state = await store.state("rate")
    assert state.last_accepted is None
    assert list(state.suspects) == []


async def test_the_last_accepted_value_is_published_exactly(store: PostgresStore) -> None:
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


async def test_state_lists_the_suspects_since_the_last_accepted(store: PostgresStore) -> None:
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


async def test_order_is_the_order_of_recording_not_the_clock(store: PostgresStore) -> None:
    # The server's clock was corrected backwards between the runs.
    await store.record(at(10, Accepted(reading("1600"))))
    await store.record(at(5, Suspect(reading("1800"))))
    state = await store.state("rate")
    assert state.last_accepted == Decimal(1600)
    assert list(state.suspects) == [Decimal(1800)]
    await store.record(at(5, Accepted(reading("1700"))))

    latest = await store.latest("rate")
    assert latest.published is not None
    assert latest.published.value == Decimal(1700)
    assert latest.last_attempt_at == T0 + timedelta(minutes=5)
    state = await store.state("rate")
    assert state.last_accepted == Decimal(1700)
    assert list(state.suspects) == []


async def test_a_confirmed_reading_is_published(store: PostgresStore) -> None:
    await store.record(at(0, Accepted(reading("1600"))))
    await store.record(at(10, Suspect(reading("1800"))))
    await store.record(at(20, Accepted(reading("1801"), confirmed=True)))

    latest = await store.latest("rate")
    assert latest.published is not None
    assert latest.published.value == Decimal(1801)
    assert not latest.pending
    assert list((await store.state("rate")).suspects) == []


async def test_a_rejected_reading_keeps_the_value_the_source_sent(
    store: PostgresStore, engine: AsyncEngine
) -> None:
    await store.record(at(0, Rejected(Rejection.IMPLAUSIBLE, "too low", reading("16.00"))))
    async with engine.connect() as connection:
        row = (await connection.execute(observations.select())).one()
    assert row.status == "rejected"
    assert str(row.value) == "16.00"
    assert row.reason == "implausible"


async def test_attempted_in_looks_at_the_last_attempt(store: PostgresStore) -> None:
    slot = (T0 + timedelta(minutes=10), T0 + timedelta(minutes=20))
    await store.record(at(10, Rejected(Rejection.FETCH_FAILED, "timeout")))
    assert await store.attempted_in("rate", *slot)
    assert not await store.attempted_in(
        "rate", T0 + timedelta(minutes=20), T0 + timedelta(minutes=30)
    )
    assert not await store.attempted_in("other", *slot)


async def test_an_attempt_stamped_by_a_clock_running_ahead_does_not_fill_the_slot(
    store: PostgresStore,
) -> None:
    # Stamped an hour ahead, before the clock was corrected: the 18:10 slot has not run.
    await store.record(at(70, Accepted(reading("1600", 70))))
    assert not await store.attempted_in(
        "rate", T0 + timedelta(minutes=10), T0 + timedelta(minutes=20)
    )


async def test_only_one_runner_holds_a_series(store: PostgresStore, urls: Urls) -> None:
    other_process = PostgresStore(
        create_async_engine(urls.app), create_async_engine(urls.app, poolclass=NullPool)
    )
    async with store.exclusive("rate") as alone:
        assert alone
        async with other_process.exclusive("rate") as also:
            assert not also
        async with other_process.exclusive("other") as other:
            assert other
    async with other_process.exclusive("rate") as after:
        assert after


RULES = Rules(
    Range(Decimal(500), Decimal(50_000)),
    timedelta(minutes=30),
    Decimal("0.05"),
    Decimal("0.005"),
)
BITSO = BitsoBid("usdt_ars")


def bitso_answer(bid: str, as_of: datetime) -> httpx.Response:
    body = (
        '{"success": true, "payload": {"book": "usdt_ars",'
        f' "bid": "{bid}", "created_at": "{as_of.isoformat()}"}}}}'
    )
    return httpx.Response(200, content=body.encode())


async def test_locks_do_not_starve_the_query_pool(urls: Urls) -> None:
    # A pool of one connection, and more series than that running in the same slot.
    small = create_async_engine(urls.app, pool_size=1, max_overflow=0, pool_timeout=2)
    lock_engine = create_async_engine(urls.app, poolclass=NullPool)
    store = PostgresStore(small, lock_engine)
    series = [
        Series(f"rate_{n}", "test", (BITSO.name,), timedelta(minutes=10), RULES) for n in range(4)
    ]

    async def slow_bitso(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.2)  # every run holds its lock while the others query
        return bitso_answer("1615.3", datetime.now(UTC))

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(slow_bitso)) as client:
            ran = await asyncio.gather(
                *(
                    run_slot(s, {BITSO.name: BITSO}, client, store, lambda: datetime.now(UTC))
                    for s in series
                )
            )
        assert ran == [True] * 4
        for s in series:
            assert (await store.latest(s.id)).published is not None
    finally:
        await small.dispose()
        await lock_engine.dispose()


async def test_a_jump_is_confirmed_through_the_real_store(
    store: PostgresStore, engine: AsyncEngine
) -> None:
    series = Series("rate", "test", (BITSO.name,), timedelta(minutes=10), RULES)
    bids = iter(["1600", "1800", "1805", "1801"])
    clock = [T0]  # the runs are 10 minutes apart

    def bitso(request: httpx.Request) -> httpx.Response:
        return bitso_answer(next(bids), clock[0])

    outcomes = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(bitso)) as client:
        for n in range(4):
            clock[0] = T0 + timedelta(minutes=10 * n)
            [observation] = await collect(
                series, {BITSO.name: BITSO}, client, store, lambda: clock[0]
            )
            outcomes.append(type(observation.outcome).__name__)
            published = (await store.latest("rate")).published
            assert published is not None
            if n < 3:
                assert published.value == Decimal(1600)

    assert outcomes == ["Accepted", "Suspect", "Suspect", "Accepted"]
    latest = await store.latest("rate")
    assert latest.published is not None
    assert latest.published.value == Decimal(1801)
    assert not latest.pending
    async with engine.connect() as connection:
        result = await connection.execute(select(observations.c.status).order_by(observations.c.id))
        statuses: list[str] = list(result.scalars())
        assert statuses == [Status.ACCEPTED, Status.SUSPECT, Status.SUSPECT, Status.CONFIRMED]


async def test_the_app_role_can_only_read_and_append(
    store: PostgresStore, engine: AsyncEngine
) -> None:
    await store.record(at(0, Accepted(reading("1600"))))
    for statement in (update(observations).values(value=1), observations.delete()):
        with pytest.raises(ProgrammingError, match="permission denied"):
            async with engine.begin() as connection:
                await connection.execute(statement)


async def test_the_schema_refuses_an_accepted_row_without_a_value(engine: AsyncEngine) -> None:
    with pytest.raises(IntegrityError, match="value_unless_rejected"):
        async with engine.begin() as connection:
            await connection.execute(
                observations.insert().values(
                    series_id="rate", source="s", fetched_at=T0, status="accepted"
                )
            )


def test_the_migrations_build_the_schema_in_tables_py(urls: Urls) -> None:
    """Build ``tables.py`` in its own schema and compare what Postgres stored for both, down to
    each check constraint and index definition, which Alembic's comparison does not look at."""
    owner = create_engine(urls.owner)
    try:
        with owner.begin() as connection:
            connection.execute(text("DROP SCHEMA IF EXISTS expected CASCADE"))
            connection.execute(text("CREATE SCHEMA expected"))
            metadata.create_all(
                connection.execution_options(schema_translate_map={None: "expected"})
            )
            migrated, expected = (
                _definitions(connection, schema) for schema in ("public", "expected")
            )
            connection.execute(text("DROP SCHEMA expected CASCADE"))
    finally:
        owner.dispose()
    assert migrated == expected


def _definitions(connection: Connection, schema: str) -> dict[str, object]:
    """Every table of ``schema`` as Postgres stored it, with the schema name taken out of the
    definitions so two schemas compare. Alembic's own table is left out."""

    def rows(sql: str) -> list[tuple[object, ...]]:
        found = connection.execute(text(sql), {"schema": schema, "prefix": f"{schema}."}).all()
        return [tuple(row) for row in found]

    tables = "c.relkind = 'r' AND c.relname <> 'alembic_version'"
    return {
        "columns": rows(
            "SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,"
            " replace(pg_get_expr(d.adbin, d.adrelid), :prefix, '')"
            " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum"
            f" WHERE n.nspname = :schema AND {tables} AND a.attnum > 0 AND NOT a.attisdropped"
            " ORDER BY 1, 2"
        ),
        "constraints": rows(
            "SELECT c.relname, k.conname, pg_get_constraintdef(k.oid)"
            " FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            f" WHERE n.nspname = :schema AND {tables} ORDER BY 1, 2"
        ),
        "indexes": rows(
            "SELECT tablename, indexname, replace(indexdef, :prefix, '') FROM pg_indexes"
            " WHERE schemaname = :schema AND tablename <> 'alembic_version' ORDER BY 1, 2"
        ),
    }
