"""The database schema. Alembic migrations in ``migrations/`` create it, and
``tests/integration/test_migrations.py`` checks that both match."""

from __future__ import annotations

from enum import StrEnum

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


class Status(StrEnum):
    ACCEPTED = "accepted"
    # An accepted reading that ended a run of suspects.
    CONFIRMED = "confirmed"
    SUSPECT = "suspect"
    REJECTED = "rejected"


PUBLISHED = (Status.ACCEPTED, Status.CONFIRMED)


def _sql_list(statuses: tuple[Status, ...]) -> str:
    return ", ".join(f"'{status}'" for status in statuses)


metadata = MetaData()

# One row per attempt to read a series from a source, whatever its outcome. Nothing is updated
# or deleted: confirming a run of suspects adds an accepted row, it does not change the suspects.
# Rows of a series are ordered by id, their insertion order (one runner per series at a time),
# not by fetched_at, which moves if the server's clock is corrected.
observations = Table(
    "observations",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("series_id", Text, nullable=False),
    Column("source", Text, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("status", Text, nullable=False),
    # Exact, with no fixed scale: a rejected value is kept as the source sent it.
    Column("value", Numeric, nullable=True),
    Column("as_of", DateTime(timezone=True), nullable=True),
    Column("reason", Text, nullable=True),
    Column("detail", Text, nullable=True),
    CheckConstraint(f"status IN ({_sql_list(tuple(Status))})", name="status_known"),
    CheckConstraint("(value IS NULL) = (as_of IS NULL)", name="value_with_as_of"),
    CheckConstraint(
        f"(status = '{Status.REJECTED}') = (reason IS NOT NULL)", name="reason_only_when_rejected"
    ),
    CheckConstraint(
        f"status = '{Status.REJECTED}' OR value IS NOT NULL", name="value_unless_rejected"
    ),
    Index("observations_series_id", "series_id", text("id DESC")),
    Index(
        "observations_series_published",
        "series_id",
        text("id DESC"),
        postgresql_where=text(f"status IN ({_sql_list(PUBLISHED)})"),
    ),
)
