import json
import logging

import redis

from app.core.config import settings
from app.core.task_status import FAILED
from app.models import DeadLetter, TaskRecord

logger = logging.getLogger(__name__)

DLQ_REDIS_KEY = "dlq:failed_tasks"

redis_client = redis.from_url(
    settings.redis_url,
    socket_connect_timeout=settings.redis_socket_timeout_seconds,
    socket_timeout=settings.redis_socket_timeout_seconds,
)


def _notification_payload(task_id: str, file_path: str | None, error_message: str) -> str:
    """Mesmo valor no LPUSH e no LREM: o LREM só remove um item idêntico."""
    return json.dumps({"task_id": task_id, "file_path": file_path, "error": error_message})


def record_dead_letter(
    db,
    task_id: str,
    file_path: str | None,
    error_message: str,
    retry_count: int,
    failure_kind: str,
) -> None:
    """Registra a falha na DLQ. O Postgres é a fonte da verdade.

    Marca a tarefa como failed e insere o DeadLetter no mesmo commit, então
    os dois acontecem juntos ou não acontecem. Se esse commit falhar, a
    exceção sobe, porque não há outro lugar onde registrar a falha. Depois do
    commit, o lpush no Redis é só uma notificação: se ele falhar, registramos
    um aviso e seguimos, porque a linha na tabela já existe.
    """
    task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
    if task:
        task.status = FAILED
        task.error_message = error_message

    db.add(
        DeadLetter(
            task_id=task_id,
            file_path=file_path,
            error_message=error_message,
            retry_count=retry_count,
            failure_kind=failure_kind,
        )
    )
    db.commit()

    try:
        redis_client.lpush(DLQ_REDIS_KEY, _notification_payload(task_id, file_path, error_message))
    except Exception:
        logger.warning("Falha ao notificar a DLQ no Redis para a task %s", task_id, exc_info=True)


def remove_notification(dead_letter: DeadLetter) -> None:
    """Remove da lista do Redis a notificação de uma entrada já reprocessada.

    Como no registro, o Postgres é a fonte da verdade: se o LREM falhar, a
    entrada continua marcada como reprocessed e só sobra um item na lista.
    """
    payload = _notification_payload(
        dead_letter.task_id, dead_letter.file_path, dead_letter.error_message
    )
    try:
        redis_client.lrem(DLQ_REDIS_KEY, 0, payload)
    except Exception:
        logger.warning(
            "Falha ao remover a notificação da DLQ no Redis para a entrada %s",
            dead_letter.id,
            exc_info=True,
        )
