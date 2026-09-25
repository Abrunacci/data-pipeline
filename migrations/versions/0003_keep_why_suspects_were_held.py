"""Keep why suspects were held back

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# A suspect's reason ("jump" or "disagreement") goes in the reason column, which only rejected
# rows used. Suspects recorded before stay without one. The release before this one writes
# suspects without a reason, which both new constraints allow, and never reads the reason of a
# suspect, so it keeps working after a rollback.


def upgrade() -> None:
    op.drop_constraint("reason_only_when_rejected", "observations", type_="check")
    op.create_check_constraint(
        "reason_only_when_rejected_or_suspect",
        "observations",
        "status IN ('rejected', 'suspect') OR reason IS NULL",
    )
    op.create_check_constraint(
        "reason_when_rejected", "observations", "status <> 'rejected' OR reason IS NOT NULL"
    )


def downgrade() -> None:
    # Development only: it forgets why suspects were held back.
    op.execute("UPDATE observations SET reason = NULL WHERE status = 'suspect'")
    op.drop_constraint("reason_when_rejected", "observations", type_="check")
    op.drop_constraint("reason_only_when_rejected_or_suspect", "observations", type_="check")
    op.create_check_constraint(
        "reason_only_when_rejected", "observations", "(status = 'rejected') = (reason IS NOT NULL)"
    )
