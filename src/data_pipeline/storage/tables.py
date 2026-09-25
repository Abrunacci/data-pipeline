"""The database schema. Alembic migrations in ``migrations/`` create it, and
``test_the_migrations_build_the_schema_in_tables_py`` (tests/integration) checks that both
match."""

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

from data_pipeline.core.readings import HeldBack


class Status(StrEnum):
    ACCEPTED = "accepted"
    # An accepted reading that ended a run of suspects.
    CONFIRMED = "confirmed"
    SUSPECT = "suspect"
    # A reading of the series' control source: recorded, never published.
    CONTROL = "control"
    REJECTED = "rejected"
    # A past value loaded once from a history source: part of the history, never published as
    # the current value and not an attempt of the schedule.
    BACKFILL = "backfill"


PUBLISHED = (Status.ACCEPTED, Status.CONFIRMED)
# What the daily history is made of.
HISTORY = (*PUBLISHED, Status.BACKFILL)


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
    # Why a row was rejected (required) or held back (a suspect's HeldBack; None on the ones
    # recorded before it was kept). Other rows have none.
    Column("reason", Text, nullable=True),
    Column("detail", Text, nullable=True),
    CheckConstraint(f"status IN ({_sql_list(tuple(Status))})", name="status_known"),
    CheckConstraint("(value IS NULL) = (as_of IS NULL)", name="value_with_as_of"),
    CheckConstraint(
        f"status IN ('{Status.REJECTED}', '{Status.SUSPECT}') OR reason IS NULL",
        name="reason_only_when_rejected_or_suspect",
    ),
    CheckConstraint(
        f"status <> '{Status.REJECTED}' OR reason IS NOT NULL", name="reason_when_rejected"
    ),
    CheckConstraint(
        f"status <> '{Status.SUSPECT}' OR reason IS NULL"
        f" OR reason IN ({', '.join(repr(why.value) for why in HeldBack)})",
        name="suspect_reason_known",
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
    Index(
        "observations_series_history",
        "series_id",
        "as_of",
        postgresql_where=text(f"status IN ({_sql_list(HISTORY)})"),
    ),
)
