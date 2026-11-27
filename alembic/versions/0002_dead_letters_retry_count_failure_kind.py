"""dead_letters: retry_count vira Integer e ganha failure_kind

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "dead_letters",
        "retry_count",
        existing_type=sa.String(),
        type_=sa.Integer(),
        postgresql_using="COALESCE(retry_count, '0')::integer",
    )
    op.alter_column(
        "dead_letters",
        "retry_count",
        existing_type=sa.Integer(),
        server_default="0",
        nullable=False,
    )
    op.add_column(
        "dead_letters",
        sa.Column("failure_kind", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dead_letters", "failure_kind")
    op.alter_column(
        "dead_letters",
        "retry_count",
        existing_type=sa.Integer(),
        server_default=None,
        nullable=True,
    )
    op.alter_column(
        "dead_letters",
        "retry_count",
        existing_type=sa.Integer(),
        type_=sa.String(),
        postgresql_using="retry_count::varchar",
    )
