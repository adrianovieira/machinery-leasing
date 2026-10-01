"""create transactions table

Revision ID: 001_create_transactions_table
Revises:
Create Date: 2026-09-30 22:39:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "001_create_transactions_table"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("customer_id", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Numeric(precision=15, scale=2), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "PROCESSING",
                "APPROVED",
                "REJECTED",
                "RETRYING",
                "FAILED",
                name="transaction_status_enum",
            ),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column("failure_reason", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_customer_id", "transactions", ["customer_id"], unique=False)
    op.create_index("idx_status", "transactions", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_status", table_name="transactions")
    op.drop_index("idx_customer_id", table_name="transactions")
    op.drop_table("transactions")
