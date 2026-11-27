"""
Rotina de tarefas travadas no Postgres (app/workers/maintenance.py).

Chamamos a tarefa diretamente, sem o beat. Assim o teste valida o
FOR UPDATE SKIP LOCKED e o COALESCE no dialeto do Postgres, que o SQLite
ignorava.
"""
import threading
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from app.core import database, dead_letters
from app.core.config import settings
from app.core.dead_letters import DLQ_REDIS_KEY
from app.models import TaskRecord
from app.workers.maintenance import sweep_stale_tasks
from tests.integration.helpers import dead_letters_for, task_row


def _insert_task(task_id: str, status: str, created_at: datetime, file_path: str | None = None) -> None:
    with database.SessionLocal() as session:
        session.add(
            TaskRecord(
                id=task_id,
                status=status,
                file_hash=f"hash-{task_id}",
                file_path=file_path,
                created_at=created_at,
            )
        )
        session.commit()


def _ago(seconds: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def test_stale_pending_task_is_failed_and_dead_lettered(monkeypatch):
    """Tarefa pending antiga vira failed e ganha uma entrada transitória na DLQ."""
    monkeypatch.setattr(settings, "stale_task_timeout_seconds", 600)
    _insert_task("travada", "pending", created_at=_ago(7200), file_path="/x/travada.jpg")

    result = sweep_stale_tasks()

    assert result["stale_failed"] == 1
    assert task_row("travada")["status"] == "failed"

    entries = dead_letters_for("travada")
    assert len(entries) == 1
    assert entries[0]["failure_kind"] == "transient"
    assert entries[0]["error_message"].startswith("stale: no progress for 600s")
    assert dead_letters.redis_client.llen(DLQ_REDIS_KEY) == 1


def test_sweep_skips_row_locked_by_another_transaction(monkeypatch):
    """Com a linha travada por outra transação, a rotina pula a linha sem esperar por ela."""
    monkeypatch.setattr(settings, "stale_task_timeout_seconds", 600)
    _insert_task("bloqueada", "pending", created_at=_ago(7200), file_path="/x/bloqueada.jpg")

    holder = database.engine.connect()
    holder.begin()
    holder.execute(text("SELECT id FROM tasks WHERE id = 'bloqueada' FOR UPDATE")).all()
    try:
        results = {}

        def run_sweep():
            results["value"] = sweep_stale_tasks()

        # Se a rotina esperasse pelo lock em vez de pular a linha, esta thread travaria.
        worker = threading.Thread(target=run_sweep, daemon=True)
        worker.start()
        worker.join(timeout=10)
        assert not worker.is_alive(), "a rotina ficou esperando o lock em vez de pular a linha"
    finally:
        holder.rollback()
        holder.close()

    assert results["value"]["stale_failed"] == 0
    assert task_row("bloqueada")["status"] == "pending"
    assert dead_letters_for("bloqueada") == []
