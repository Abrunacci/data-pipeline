"""Record control readings

Revision ID: 0002
Revises: 0001
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The release before this one never writes "control" and keeps working after this migration,
# if a deploy is rolled back: it publishes the same values. Its one blind spot is
# pending_confirmation, which it reads from the newest row that is not rejected: a control row
# is only the newest when a run stopped between the control and its decision.
#
# Recreating the constraint scans the table under a lock. With a few thousand rows a day that
# takes milliseconds; see CONTRIBUTING (Database) for how to change constraints on a large table.


def upgrade() -> None:
    op.drop_constraint("status_known", "observations", type_="check")
    op.create_check_constraint(
        "status_known",
        "observations",
        "status IN ('accepted', 'confirmed', 'suspect', 'control', 'rejected')",
    )


def downgrade() -> None:
    # Destructive: it deletes every control reading. Infra never runs downgrades; this is for
    # development only.
    op.execute("DELETE FROM observations WHERE status = 'control'")
    op.drop_constraint("status_known", "observations", type_="check")
    op.create_check_constraint(
        "status_known",
        "observations",
        "status IN ('accepted', 'confirmed', 'suspect', 'rejected')",
    )
