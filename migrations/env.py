"""Alembic environment. Migrations run online against ``MIGRATION_DATABASE_URL`` (the database
owner, on the server), or ``DATABASE_URL`` locally.

On the server, infra runs the migrations while the previous release is still serving, and a
rollback never undoes one. So every migration must work with the code of the release before it,
and it gives up after ``lock_timeout`` rather than queue the running app behind a table lock.
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine

from data_pipeline.storage.tables import metadata

url = os.environ.get("MIGRATION_DATABASE_URL") or os.environ.get("DATABASE_URL")
if not url:
    raise SystemExit("Set MIGRATION_DATABASE_URL or DATABASE_URL to run the migrations.")
if context.is_offline_mode():
    raise SystemExit("Offline migrations (--sql) are not supported.")

engine = create_engine(url)
# begin(), not connect(): SET opens a transaction, and Alembic runs inside it and would leave
# the commit to us.
with engine.begin() as connection:
    connection.exec_driver_sql("SET LOCAL lock_timeout = '5s'")
    context.configure(connection=connection, target_metadata=metadata)
    with context.begin_transaction():
        context.run_migrations()
engine.dispose()
