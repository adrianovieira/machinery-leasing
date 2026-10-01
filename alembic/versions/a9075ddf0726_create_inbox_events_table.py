"""create_inbox_events_table

Revision ID: a9075ddf0726
Revises: 4d1baf4ed54e
Create Date: 2026-10-01 09:03:26.158563

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a9075ddf0726"
down_revision: str | Sequence[str] | None = "4d1baf4ed54e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "inbox_events",
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("consumer_group", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "PROCESSING",
                "COMPLETED",
                "RETRYING",
                "FAILED",
                name="inbox_status_enum",
            ),
            server_default="PROCESSING",
            nullable=False,
        ),
        sa.Column(
            "received_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index(
        "idx_inbox_consumer_group", "inbox_events", ["consumer_group"], unique=False
    )
    op.create_index("idx_inbox_status", "inbox_events", ["status"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("idx_inbox_status", table_name="inbox_events")
    op.drop_index("idx_inbox_consumer_group", table_name="inbox_events")
    op.drop_table("inbox_events")
