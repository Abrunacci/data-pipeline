"""A real Postgres in Docker, set up like the server: an owner runs the migrations, and the app
connects as a separate role with only the privileges the migrations grant it."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer

from data_pipeline.storage.postgres import PostgresStore

ROOT = Path(__file__).parents[2]
# The image the server runs (infra: ansible/roles/postgres/defaults/main.yml).
POSTGRES_IMAGE = (
    "postgres:17.11-trixie@sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f"
)
APP_USER = "pipeline_app"
APP_PASSWORD = "app"  # a throwaway test container


@dataclass(frozen=True)
class Urls:
    owner: str
    app: str


@pytest.fixture(scope="session")
def urls() -> Iterator[Urls]:
    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as postgres:
        owner = postgres.get_connection_url()
        admin = create_engine(owner)
        with admin.begin() as connection:
            connection.execute(text(f"CREATE ROLE {APP_USER} LOGIN PASSWORD '{APP_PASSWORD}'"))
        admin.dispose()
        with pytest.MonkeyPatch.context() as env:
            env.setenv("MIGRATION_DATABASE_URL", owner)
            env.setenv("APP_DB_USER", APP_USER)
            command.upgrade(Config(ROOT / "alembic.ini"), "head")
        app = make_url(owner).set(username=APP_USER, password=APP_PASSWORD)
        yield Urls(owner=owner, app=app.render_as_string(hide_password=False))


@pytest.fixture
def database_url(urls: Urls) -> str:
    """The URL the app uses."""
    return urls.app


@pytest.fixture
async def engine(urls: Urls) -> AsyncIterator[AsyncEngine]:
    owner = create_async_engine(urls.owner)
    async with owner.begin() as connection:
        await connection.execute(text("TRUNCATE observations"))
    await owner.dispose()
    engine = create_async_engine(urls.app)
    yield engine
    await engine.dispose()


@pytest.fixture
async def store(engine: AsyncEngine, urls: Urls) -> AsyncIterator[PostgresStore]:
    lock_engine = create_async_engine(urls.app, poolclass=NullPool)
    yield PostgresStore(engine, lock_engine)
    await lock_engine.dispose()
