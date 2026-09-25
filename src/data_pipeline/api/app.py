"""The HTTP API, and the composition root: it builds the engine, the HTTP client, the store and
the scheduler in its ``lifespan``, and nothing else creates them."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from data_pipeline.api.schemas import Health, LatestRates, latest_rate
from data_pipeline.config import Settings, load_series
from data_pipeline.core.series import Series
from data_pipeline.core.sources import Source
from data_pipeline.runner.scheduler import run_forever
from data_pipeline.runner.store import Store
from data_pipeline.sources import available_sources
from data_pipeline.storage.postgres import PostgresStore

logger = logging.getLogger(__name__)

# Every source answers in well under a second; a slow one fails and the next slot tries again.
HTTP_TIMEOUT = httpx.Timeout(10.0)


def create_app(settings: Settings) -> FastAPI:
    sources = available_sources()
    series = load_series(settings.series_file, sources)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.database_url, pool_size=3, max_overflow=2)
        store = PostgresStore(engine)
        async with httpx.AsyncClient(
            timeout=HTTP_TIMEOUT, headers={"User-Agent": settings.user_agent}
        ) as client:
            app.state.engine = engine
            app.state.store = store
            tasks = _start(series, sources, client, store) if settings.run_scheduler else []
            try:
                yield
            finally:
                for task in tasks:
                    task.cancel()
                for task in tasks:
                    with suppress(asyncio.CancelledError):
                        await task
                await engine.dispose()

    app = FastAPI(title="data-pipeline", lifespan=lifespan)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware, allow_origins=list(settings.cors_origins), allow_methods=["GET"]
        )

    @app.get("/health", responses={503: {"model": Health}})
    async def health(
        engine: Annotated[AsyncEngine, Depends(_engine)], response: Response
    ) -> Health:
        try:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        except (SQLAlchemyError, OSError):
            logger.exception("health check could not reach the database")
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return Health(status="database_unavailable")
        return Health(status="ok")

    @app.get("/v1/rates/latest")
    async def latest(store: Annotated[Store, Depends(_store)]) -> LatestRates:
        now = datetime.now(UTC)
        return LatestRates(
            rates={s.id: latest_rate(await store.latest(s.id), s.rules, now) for s in series}
        )

    return app


def _start(
    series: tuple[Series, ...],
    sources: Mapping[str, Source],
    client: httpx.AsyncClient,
    store: Store,
) -> list[asyncio.Task[None]]:
    return [
        asyncio.create_task(run_forever(s, sources, client, store), name=f"series:{s.id}")
        for s in series
    ]


def _engine(request: Request) -> AsyncEngine:
    engine: AsyncEngine = request.app.state.engine
    return engine


def _store(request: Request) -> Store:
    store: Store = request.app.state.store
    return store
