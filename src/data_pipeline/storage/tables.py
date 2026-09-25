"""The database schema. Alembic migrations in ``migrations/`` create it; keep both in step."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Index,
    MetaData,
    Numeric,
    Table,
    Text,
    text,
)

metadata = MetaData()

# One row per attempt to read a series from a source, whatever its outcome. Nothing is updated
# or deleted: confirming a run of suspects adds an accepted row, it does not change the suspects.
observations = Table(
    "observations",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("series_id", Text, nullable=False),
    Column("source", Text, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    # accepted | confirmed | suspect | rejected. "confirmed" is an accepted reading that ended a
    # run of suspects.
    Column("status", Text, nullable=False),
    # Exact, with no fixed scale: a rejected value is kept as the source sent it.
    Column("value", Numeric, nullable=True),
    Column("as_of", DateTime(timezone=True), nullable=True),
    Column("reason", Text, nullable=True),
    Column("detail", Text, nullable=True),
    CheckConstraint(
        "status IN ('accepted', 'confirmed', 'suspect', 'rejected')", name="status_known"
    ),
    CheckConstraint("(value IS NULL) = (as_of IS NULL)", name="value_with_as_of"),
    CheckConstraint(
        "(status = 'rejected') = (reason IS NOT NULL)", name="reason_only_when_rejected"
    ),
    CheckConstraint("status = 'rejected' OR value IS NOT NULL", name="value_unless_rejected"),
    Index("observations_series_fetched", "series_id", text("fetched_at DESC")),
    Index(
        "observations_series_published",
        "series_id",
        text("fetched_at DESC"),
        postgresql_where=text("status IN ('accepted', 'confirmed')"),
    ),
)
