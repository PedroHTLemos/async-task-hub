"""tasks: file_path e índice único parcial para uploads em andamento

Antes de criar o índice, marca como failed as tarefas em andamento
duplicadas (mesmo file_hash), mantendo só a mais recente de cada hash.

O downgrade remove o índice e a coluna, mas NÃO restaura os status alterados
por este upgrade: as tarefas marcadas como 'superseded by migration 0003'
continuam failed.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-27

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

IN_PROGRESS_SQL = "('pending', 'processing', 'retrying')"
INDEX_NAME = "uq_tasks_file_hash_in_progress"


def upgrade() -> None:
    op.add_column("tasks", sa.Column("file_path", sa.String(), nullable=True))

    op.execute(
        f"""
        UPDATE tasks
        SET status = 'failed', error_message = 'superseded by migration 0003'
        WHERE id IN (
            SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY file_hash
                           ORDER BY created_at DESC, id DESC
                       ) AS rn
                FROM tasks
                WHERE status IN {IN_PROGRESS_SQL}
                  AND file_hash IS NOT NULL
            ) ranked
            WHERE rn > 1
        )
        """
    )

    op.create_index(
        INDEX_NAME,
        "tasks",
        ["file_hash"],
        unique=True,
        postgresql_where=sa.text(f"status IN {IN_PROGRESS_SQL}"),
    )


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name="tasks")
    op.drop_column("tasks", "file_path")
