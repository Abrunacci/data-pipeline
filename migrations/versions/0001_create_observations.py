"""Create observations

Revision ID: 0001
Revises: none
"""

import os
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "observations",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("series_id", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('accepted', 'confirmed', 'suspect', 'rejected')", name="status_known"
        ),
        sa.CheckConstraint("(value IS NULL) = (as_of IS NULL)", name="value_with_as_of"),
        sa.CheckConstraint(
            "(status = 'rejected') = (reason IS NOT NULL)", name="reason_only_when_rejected"
        ),
        sa.CheckConstraint(
            "status = 'rejected' OR value IS NOT NULL", name="value_unless_rejected"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "observations_series_id", "observations", ["series_id", sa.literal_column("id DESC")]
    )
    op.create_index(
        "observations_series_published",
        "observations",
        ["series_id", sa.literal_column("id DESC")],
        postgresql_where=sa.text("status IN ('accepted', 'confirmed')"),
    )
    # The app only reads and appends: the database enforces that nothing is updated or deleted.
    role = op.get_bind().dialect.identifier_preparer.quote(os.environ["APP_DB_USER"])
    op.execute(f"GRANT SELECT, INSERT ON observations TO {role}")
    op.execute(f"GRANT USAGE ON SEQUENCE observations_id_seq TO {role}")


def downgrade() -> None:
    op.drop_index("observations_series_published", table_name="observations")
    op.drop_index("observations_series_id", table_name="observations")
    op.drop_table("observations")
