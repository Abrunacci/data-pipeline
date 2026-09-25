"""Alembic environment. Migrations run online against MIGRATION_DATABASE_URL, or DATABASE_URL.

The server's infra keeps the previous release for rollbacks and never undoes a migration, so
every migration must keep working with the code of the release before it.
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine

from data_pipeline.storage.tables import metadata

url = os.environ.get("MIGRATION_DATABASE_URL") or os.environ["DATABASE_URL"]
engine = create_engine(url)
with engine.connect() as connection:
    context.configure(connection=connection, target_metadata=metadata)
    with context.begin_transaction():
        context.run_migrations()
engine.dispose()
