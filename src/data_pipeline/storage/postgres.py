"""The ``Store`` on Postgres."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Row, Select, func, insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from data_pipeline.core.readings import Accepted, Observation, Rejected, Suspect
from data_pipeline.runner.store import Latest, Published, SeriesState
from data_pipeline.storage.tables import observations as obs

_PUBLISHED = ("accepted", "confirmed")


class PostgresStore:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(self, observation: Observation) -> None:
        row: dict[str, object]
        match observation.outcome:
            case Accepted(reading=reading, confirmed=confirmed):
                row = {
                    "status": "confirmed" if confirmed else "accepted",
                    "value": reading.value,
                    "as_of": reading.as_of,
                }
            case Suspect(reading=reading):
                row = {"status": "suspect", "value": reading.value, "as_of": reading.as_of}
            case Rejected(reason=reason, detail=detail, reading=reading):
                row = {
                    "status": "rejected",
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

    async def state(self, series_id: str) -> SeriesState:
        async with self._engine.connect() as connection:
            last = (await connection.execute(self._last_published(series_id))).first()
            since = select(obs.c.value).where(
                obs.c.series_id == series_id, obs.c.status == "suspect"
            )
            if last is not None:
                since = since.where(obs.c.fetched_at > last.fetched_at)
            result = await connection.execute(since.order_by(obs.c.fetched_at))
            suspects: list[object] = list(result.scalars())
            return SeriesState(
                last_accepted=None if last is None else _decimal(last.value),
                suspects=[_decimal(value) for value in suspects],
            )

    async def latest(self, series_id: str) -> Latest:
        async with self._engine.connect() as connection:
            last = (await connection.execute(self._last_published(series_id))).first()
            newest_valid = (
                await connection.execute(
                    select(obs.c.status)
                    .where(obs.c.series_id == series_id, obs.c.status != "rejected")
                    .order_by(obs.c.fetched_at.desc())
                    .limit(1)
                )
            ).scalar()
            last_attempt_at = (
                await connection.execute(
                    select(func.max(obs.c.fetched_at)).where(obs.c.series_id == series_id)
                )
            ).scalar()
        return Latest(
            published=None if last is None else _published(last),
            pending=newest_valid == "suspect",
            last_attempt_at=last_attempt_at,
        )

    async def attempted_since(self, series_id: str, moment: datetime) -> bool:
        async with self._engine.connect() as connection:
            found = await connection.execute(
                select(obs.c.id)
                .where(obs.c.series_id == series_id, obs.c.fetched_at >= moment)
                .limit(1)
            )
            return found.first() is not None

    @asynccontextmanager
    async def exclusive(self, series_id: str) -> AsyncIterator[bool]:
        # A session-level advisory lock, held on its own connection for the whole run.
        key = {"key": f"data-pipeline:series:{series_id}"}
        async with self._engine.connect() as connection:
            locked: object = (
                await connection.execute(text("SELECT pg_try_advisory_lock(hashtext(:key))"), key)
            ).scalar_one()
            try:
                yield bool(locked)
            finally:
                if locked:
                    await connection.execute(text("SELECT pg_advisory_unlock(hashtext(:key))"), key)
                await connection.commit()

    @staticmethod
    def _last_published(series_id: str) -> Select[tuple[Decimal, datetime, datetime, str]]:
        return (
            select(obs.c.value, obs.c.as_of, obs.c.fetched_at, obs.c.source)
            .where(obs.c.series_id == series_id, obs.c.status.in_(_PUBLISHED))
            .order_by(obs.c.fetched_at.desc())
            .limit(1)
        )


def _decimal(value: object) -> Decimal:
    assert isinstance(value, Decimal)
    return value


def _published(row: Row[tuple[Decimal, datetime, datetime, str]]) -> Published:
    return Published(
        value=_decimal(row.value), as_of=row.as_of, fetched_at=row.fetched_at, source=row.source
    )
