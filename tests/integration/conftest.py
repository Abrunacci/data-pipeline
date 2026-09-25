"""A real Postgres in Docker, migrated with the repo's Alembic migrations, for the session."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.community.postgres import PostgresContainer

ROOT = Path(__file__).parents[2]


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    with PostgresContainer("postgres:17-alpine", driver="psycopg") as postgres:
        url = postgres.get_connection_url()
        with pytest.MonkeyPatch.context() as env:
            env.setenv("DATABASE_URL", url)
            command.upgrade(Config(ROOT / "alembic.ini"), "head")
        yield url


@pytest.fixture
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(database_url)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE observations"))
    yield engine
    await engine.dispose()
