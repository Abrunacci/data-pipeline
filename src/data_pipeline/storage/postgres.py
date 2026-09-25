"""The ``Store`` on Postgres."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Row, Select, insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from data_pipeline.core.readings import Accepted, Control, Observation, Rejected, Suspect
from data_pipeline.runner.store import Latest, Published, SeriesState
from data_pipeline.storage.tables import PUBLISHED, Status
from data_pipeline.storage.tables import observations as obs

type _PublishedRow = tuple[int, Decimal, datetime, datetime, str]


class PostgresStore:
    """``engine`` runs the queries. ``lock_engine`` only holds the per-series locks, each on a
    connection kept for a whole run: give it no pool (``NullPool``), so however many series run
    at once, queries never wait for a connection held by a lock."""

    def __init__(self, engine: AsyncEngine, lock_engine: AsyncEngine) -> None:
        self._engine = engine
        self._lock_engine = lock_engine.execution_options(isolation_level="AUTOCOMMIT")

    async def record(self, observation: Observation) -> None:
        row: dict[str, object]
        match observation.outcome:
            case Accepted(reading=reading, confirmed=confirmed, detail=detail):
                row = {
                    "status": Status.CONFIRMED if confirmed else Status.ACCEPTED,
                    "value": reading.value,
                    "as_of": reading.as_of,
                    "detail": detail,
                }
            case Suspect(reading=reading, detail=detail):
                row = {
                    "status": Status.SUSPECT,
                    "value": reading.value,
                    "as_of": reading.as_of,
                    "detail": detail,
                }
            case Control(reading=reading):
                row = {"status": Status.CONTROL, "value": reading.value, "as_of": reading.as_of}
            case Rejected(reason=reason, detail=detail, reading=reading):
                row = {
                    "status": Status.REJECTED,
                    "value": None if reading is None else reading.value,
                    "as_of": None if reading is None else reading.as_of,
                    "reason": reason.value,
                    "detail": detail,
                }
        async with self._engine.begin() as connection:
            await connection.execute(
                insert(obs).values(
                    series_id=observation.series_id,
                    source=observation.source,
                    fetched_at=observation.fetched_at,
                    **row,
                )
            )

    async def state(self, series_id: str, since: datetime) -> SeriesState:
        async with self._engine.connect() as connection:
            last = (await connection.execute(_last_published(series_id))).first()
            suspects = select(obs.c.value).where(
                obs.c.series_id == series_id,
                obs.c.status == Status.SUSPECT,
                obs.c.fetched_at >= since,
            )
            if last is not None:
                suspects = suspects.where(obs.c.id > last.id)
            result = await connection.execute(suspects.order_by(obs.c.id))
            values: list[object] = list(result.scalars())
        return SeriesState(
            last_accepted=None if last is None else _decimal(last.value),
            suspects=[_decimal(value) for value in values],
        )

    async def latest(self, series_id: str) -> Latest:
        of_series = obs.c.series_id == series_id
        newest_first = obs.c.id.desc()
        async with self._engine.connect() as connection:
            last = (await connection.execute(_last_published(series_id))).first()
            newest_valid = (
                await connection.execute(
                    select(obs.c.status, obs.c.fetched_at)
                    .where(of_series, obs.c.status.not_in((Status.REJECTED, Status.CONTROL)))
                    .order_by(newest_first)
                    .limit(1)
                )
            ).first()
            last_attempt_at = await connection.scalar(
                select(obs.c.fetched_at).where(of_series).order_by(newest_first).limit(1)
            )
        return Latest(
            published=None if last is None else _published(last),
            suspect_at=(
                newest_valid.fetched_at
                if newest_valid is not None and newest_valid.status == Status.SUSPECT
                else None
            ),
            last_attempt_at=last_attempt_at,
        )

    async def attempted_in(self, series_id: str, start: datetime, end: datetime) -> bool:
        async with self._engine.connect() as connection:
            fetched_at = await connection.scalar(
                select(obs.c.fetched_at)
                .where(obs.c.series_id == series_id)
                .order_by(obs.c.id.desc())
                .limit(1)
            )
        return fetched_at is not None and start <= fetched_at < end

    @asynccontextmanager
    async def exclusive(self, series_id: str) -> AsyncIterator[bool]:
        # A session-level advisory lock on its own connection, in autocommit so the connection
        # is never "idle in transaction" while the run waits for a source.
        key = {"key": f"data-pipeline:series:{series_id}"}
        async with self._lock_engine.connect() as connection:
            locked = await connection.scalar(
                text("SELECT pg_try_advisory_lock(hashtextextended(:key, 0))"), key
            )
            try:
                yield locked is True
            finally:
                if locked is True:
                    await connection.execute(
                        text("SELECT pg_advisory_unlock(hashtextextended(:key, 0))"), key
                    )


def _last_published(series_id: str) -> Select[_PublishedRow]:
    return (
        select(obs.c.id, obs.c.value, obs.c.as_of, obs.c.fetched_at, obs.c.source)
        .where(obs.c.series_id == series_id, obs.c.status.in_(PUBLISHED))
        .order_by(obs.c.id.desc())
        .limit(1)
    )


def _decimal(value: object) -> Decimal:
    assert isinstance(value, Decimal)
    return value


def _published(row: Row[_PublishedRow]) -> Published:
    return Published(
        value=_decimal(row.value), as_of=row.as_of, fetched_at=row.fetched_at, source=row.source
    )
