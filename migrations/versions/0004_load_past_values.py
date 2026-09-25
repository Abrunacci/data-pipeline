"""Load past values

Revision ID: 0004
Revises: 0003
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Past values loaded from a history source have their own status, "backfill". The release before
# this one never writes it. After a rollback it would still publish the same current values, but
# until a series' next run it would take the backfill rows for the newest attempt and the newest
# decision: it could skip one slot, show a wrong last_attempt_at, and show
# pending_confirmation false for a suspect that is still pending.
#
# The constraint and the index are built under a lock, with the other pending migrations in one
# transaction: fine at this table's size. See CONTRIBUTING for a large table.


def upgrade() -> None:
    op.drop_constraint("status_known", "observations", type_="check")
    op.create_check_constraint(
        "status_known",
        "observations",
        "status IN ('accepted', 'confirmed', 'suspect', 'control', 'rejected', 'backfill')",
    )
    op.create_index(
        "observations_series_history",
        "observations",
        ["series_id", "as_of"],
        postgresql_where=sa.text("status IN ('accepted', 'confirmed', 'backfill')"),
    )


def downgrade() -> None:
    # Development only: it deletes every loaded past value.
    op.drop_index("observations_series_history", table_name="observations")
    op.execute("DELETE FROM observations WHERE status = 'backfill'")
    op.drop_constraint("status_known", "observations", type_="check")
    op.create_check_constraint(
        "status_known",
        "observations",
        "status IN ('accepted', 'confirmed', 'suspect', 'control', 'rejected')",
    )
