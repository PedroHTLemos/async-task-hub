"""dead_letters: status e rastro do reprocessamento

Entradas já existentes ficam com status 'open' pelo server_default.

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "dead_letters",
        sa.Column("status", sa.String(), nullable=False, server_default="open"),
    )
    op.add_column(
        "dead_letters",
        sa.Column("reprocessed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "dead_letters",
        sa.Column("reprocessed_task_id", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dead_letters", "reprocessed_task_id")
    op.drop_column("dead_letters", "reprocessed_at")
    op.drop_column("dead_letters", "status")
