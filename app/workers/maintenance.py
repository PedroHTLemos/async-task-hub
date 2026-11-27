import logging
import os
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import func

from app.core.config import settings
from app.core import database
from app.core.dead_letters import record_dead_letter
from app.core.task_status import IN_PROGRESS
from app.models import TaskRecord
from app.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

SWEEP_BATCH_SIZE = 100


def _fail_stale_tasks(db) -> int:
    """Marca como failed as tarefas em andamento sem progresso há mais tempo
    que stale_task_timeout_seconds. Cada uma vai para a DLQ com failure_kind
    transient, o que libera o índice único para um novo upload do arquivo."""
    timeout = settings.stale_task_timeout_seconds
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout)

    stale = (
        db.query(TaskRecord)
        .filter(
            TaskRecord.status.in_(IN_PROGRESS),
            func.coalesce(TaskRecord.updated_at, TaskRecord.created_at) < cutoff,
        )
        .with_for_update(skip_locked=True)
        .limit(SWEEP_BATCH_SIZE)
        .all()
    )

    for task in stale:
        record_dead_letter(
            db,
            task.id,
            task.file_path,
            f"stale: no progress for {timeout}s (last status: {task.status})",
            retry_count=0,
            failure_kind="transient",
        )

    return len(stale)


def _remove_old_part_files() -> int:
    """Remove .part órfãos, deixados por uploads que morreram no meio."""
    if not os.path.isdir(settings.upload_dir):
        return 0

    cutoff = time.time() - settings.part_file_max_age_seconds
    removed = 0
    for name in os.listdir(settings.upload_dir):
        if not name.endswith(".part"):
            continue
        path = os.path.join(settings.upload_dir, name)
        try:
            if os.path.getmtime(path) < cutoff:
                os.remove(path)
                removed += 1
        except FileNotFoundError:
            continue
    return removed


@celery_app.task(name="sweep_stale_tasks", ignore_result=True)
def sweep_stale_tasks():
    db = database.SessionLocal()
    try:
        stale_failed = _fail_stale_tasks(db)
    finally:
        db.close()

    part_files_removed = _remove_old_part_files()

    if stale_failed or part_files_removed:
        logger.info(
            "sweep: %d tarefa(s) travada(s) marcada(s) como failed, %d .part removido(s)",
            stale_failed,
            part_files_removed,
        )

    return {"stale_failed": stale_failed, "part_files_removed": part_files_removed}
