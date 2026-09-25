"""A real Postgres in Docker, set up like the server: the database owner, not a superuser, runs
the migrations, and the app connects as a role with only the privileges they grant it."""

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
OWNER = "pipeline"
APP_USER = "pipeline_app"
# Can connect and nothing else: for errors that are bugs, not outages.
NO_GRANTS = "no_grants"
PASSWORD = "test"  # a throwaway test container


@dataclass(frozen=True)
class Urls:
    owner: str
    app: str
    no_grants: str


@pytest.fixture(scope="session")
def urls() -> Iterator[Urls]:
    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as postgres:
        superuser = make_url(postgres.get_connection_url())
        database = superuser.database
        admin = create_engine(superuser)
        with admin.begin() as connection:
            quote = connection.dialect.identifier_preparer.quote
            db = quote(str(database))
            roles = [quote(role) for role in (OWNER, APP_USER, NO_GRANTS)]
            for role in roles:
                connection.execute(text(f"CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}'"))
            connection.execute(text(f"ALTER DATABASE {db} OWNER TO {roles[0]}"))
            connection.execute(text(f"REVOKE CONNECT ON DATABASE {db} FROM PUBLIC"))
            connection.execute(text(f"GRANT CONNECT ON DATABASE {db} TO {', '.join(roles)}"))
        admin.dispose()
        owner = superuser.set(username=OWNER, password=PASSWORD).render_as_string(
            hide_password=False
        )
        with pytest.MonkeyPatch.context() as env:
            env.setenv("MIGRATION_DATABASE_URL", owner)
            env.setenv("APP_DB_USER", APP_USER)
            command.upgrade(Config(ROOT / "alembic.ini"), "head")

        def as_role(role: str) -> str:
            return superuser.set(username=role, password=PASSWORD).render_as_string(
                hide_password=False
            )

        yield Urls(owner=owner, app=as_role(APP_USER), no_grants=as_role(NO_GRANTS))


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
