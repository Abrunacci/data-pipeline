"""The HTTP API, and the composition root: it builds the engine, the HTTP client, the store and
the scheduler in its ``lifespan``, and nothing else creates them."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager, suppress
from datetime import UTC, date, datetime
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from data_pipeline.api.schemas import (
    Health,
    History,
    LatestRates,
    day_value,
    history_range,
    latest_rate,
)
from data_pipeline.config import Settings, load_series
from data_pipeline.core.schedule import BUENOS_AIRES
from data_pipeline.core.series import Series
from data_pipeline.core.sources import HistorySource, Source
from data_pipeline.runner.scheduler import run_series
from data_pipeline.runner.store import Store
from data_pipeline.sources import available_history_sources, available_sources
from data_pipeline.storage.postgres import PostgresStore
from data_pipeline.storage.tables import observations

logger = logging.getLogger(__name__)

# Every source answers in well under a second; a slow one fails and the next slot tries again.
HTTP_TIMEOUT = httpx.Timeout(10.0)
# A database that does not answer in this long is down, for the health check and the API.
DATABASE_TIMEOUT_SECONDS = 5
HEALTH_TIMEOUT_SECONDS = 3


def create_app(settings: Settings) -> FastAPI:
    sources = available_sources()
    histories = available_history_sources()
    series = load_series(settings.series_file, sources, histories)
    by_id = {s.id: s for s in series}

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = _create_engine(settings.database_url, pooled=True)
        # Each series holds a lock connection for its whole run; see PostgresStore.
        lock_engine = _create_engine(settings.database_url, pooled=False)
        store = PostgresStore(engine, lock_engine)
        async with httpx.AsyncClient(
            timeout=HTTP_TIMEOUT, headers={"User-Agent": settings.user_agent}
        ) as client:
            app.state.engine = engine
            app.state.store = store
            tasks = (
                _start(series, sources, histories, client, store) if settings.run_scheduler else []
            )
            try:
                yield
            finally:
                for task in tasks:
                    task.cancel()
                for task in tasks:
                    with suppress(asyncio.CancelledError):
                        await task
                await engine.dispose()
                await lock_engine.dispose()

    app = FastAPI(title="data-pipeline", lifespan=lifespan)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware, allow_origins=list(settings.cors_origins), allow_methods=["GET"]
        )

    # Only for a database that cannot be reached or does not answer in time. Any other database
    # error is a bug, and stays a 500 with its traceback in the log.
    @app.exception_handler(OperationalError)
    @app.exception_handler(PoolTimeoutError)
    async def database_unavailable(request: Request, error: Exception) -> JSONResponse:
        logger.error("database unavailable on %s: %s", request.url.path, error)
        return JSONResponse(
            {"detail": "database_unavailable"}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )

    @app.get("/health", responses={503: {"model": Health}})
    async def health(
        engine: Annotated[AsyncEngine, Depends(_engine)], response: Response
    ) -> Health:
        # It reads the app's own table, so a missing schema or grant fails the deploy's check.
        try:
            async with asyncio.timeout(HEALTH_TIMEOUT_SECONDS), engine.connect() as connection:
                await connection.execute(select(observations.c.id).limit(1))
        except (SQLAlchemyError, OSError, TimeoutError) as error:
            # An expected failure while the database is down: one line, no traceback, so a probe
            # every few seconds does not flood the log.
            logger.error("health check failed: %s", error)
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return Health(status="database_unavailable")
        return Health(status="ok")

    @app.get("/v1/rates/latest", responses={503: {"description": "The database is down."}})
    async def latest(store: Annotated[Store, Depends(_store)]) -> LatestRates:
        now = datetime.now(UTC)
        return LatestRates(
            rates={s.id: latest_rate(await store.latest(s.id), s, now) for s in series}
        )

    @app.get(
        "/v1/rates/{series_id}/history",
        responses={
            404: {"description": "No such series."},
            422: {"description": "A bad date range."},
            503: {"description": "The database is down."},
        },
    )
    async def history(
        series_id: str,
        store: Annotated[Store, Depends(_store)],
        first: Annotated[date | None, Query(alias="from")] = None,
        last: Annotated[date | None, Query(alias="to")] = None,
    ) -> History:
        """One value a day, the last published that day, with days in Buenos Aires time. By
        default the last 30 days; at most 400 days per request."""
        if series_id not in by_id:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown_series")
        days = history_range(first, last, datetime.now(BUENOS_AIRES).date())
        found = await store.daily(series_id, days.first, days.last, BUENOS_AIRES)
        return History(
            series=series_id,
            first=days.first,
            last=days.last,
            days=[day_value(day) for day in found],
        )

    return app


def _start(
    series: tuple[Series, ...],
    sources: Mapping[str, Source],
    histories: Mapping[str, HistorySource],
    client: httpx.AsyncClient,
    store: Store,
) -> list[asyncio.Task[None]]:
    return [
        asyncio.create_task(run_series(s, sources, histories, client, store), name=f"series:{s.id}")
        for s in series
    ]


def _create_engine(url: str, *, pooled: bool) -> AsyncEngine:
    connect_args = {
        "connect_timeout": DATABASE_TIMEOUT_SECONDS,
        # Timestamps come back in UTC whatever the server's time zone, and no query runs for
        # longer than the database timeout.
        "options": f"-c timezone=UTC -c statement_timeout={DATABASE_TIMEOUT_SECONDS}s",
    }
    if not pooled:
        return create_async_engine(url, poolclass=NullPool, connect_args=connect_args)
    return create_async_engine(
        url,
        pool_size=3,
        max_overflow=2,
        pool_timeout=DATABASE_TIMEOUT_SECONDS,
        connect_args=connect_args,
    )


def _engine(request: Request) -> AsyncEngine:
    engine: AsyncEngine = request.app.state.engine
    return engine


def _store(request: Request) -> Store:
    store: Store = request.app.state.store
    return store
