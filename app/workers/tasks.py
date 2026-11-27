import json
import logging
import os
import warnings

import redis
from PIL import Image
from app.workers.backoff import compute_backoff
from app.workers.celery_app import celery_app
from app.workers.errors import PermanentTaskError, classify_decode_error
from app.core.database import SessionLocal
from app.core.config import settings
from app.models import TaskRecord, DeadLetter

Image.MAX_IMAGE_PIXELS = settings.max_image_pixels

logger = logging.getLogger(__name__)

OUTPUT_DIR = "app/static/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DLQ_REDIS_KEY = "dlq:failed_tasks"

redis_client = redis.from_url(settings.redis_url)


def _open_image(path: str) -> Image.Image:
    """Abre a imagem tratando decompression bomb como erro.

    Pillow só levanta DecompressionBombError acima de 2x MAX_IMAGE_PIXELS;
    entre 1x e 2x emite apenas um DecompressionBombWarning. Transformamos o
    warning em erro para o limite valer de verdade.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        try:
            return Image.open(path)
        except Image.DecompressionBombWarning as exc:
            raise Image.DecompressionBombError(str(exc)) from exc


def _decode_image(path: str) -> Image.Image:
    """Fase de decodificação: abre, decodifica, converte e reduz a imagem.

    Erros que indicam arquivo inválido viram PermanentTaskError. Os demais
    são relançados como estão, para seguir pelo caminho de retry.
    """
    try:
        with _open_image(path) as src:
            src.load()  # forces decode now, so corrupted files raise here, not lazily later
            img = src.convert("RGB")
        img.thumbnail((800, 800))
        return img
    except Exception as exc:
        if classify_decode_error(exc):
            raise PermanentTaskError(str(exc)) from exc
        raise


def _send_to_dlq(
    db,
    task_id: str,
    file_path: str,
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
        task.status = "failed"
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
        redis_client.lpush(
            DLQ_REDIS_KEY,
            json.dumps({"task_id": task_id, "file_path": file_path, "error": error_message}),
        )
    except Exception:
        logger.warning("Falha ao notificar a DLQ no Redis para a task %s", task_id, exc_info=True)


def _mark_retrying(db, task_id: str, error_message: str) -> None:
    task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
    if task:
        task.status = "retrying"
        task.error_message = error_message
        db.commit()


@celery_app.task(
    name="process_image_task",
    bind=True,
    max_retries=settings.task_max_retries,
)
def process_image_task(self, task_id: str, file_path: str):
    db = SessionLocal()
    try:
        task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
        if not task:
            return {"status": "failed", "error": "task not found"}

        task.status = "processing"
        db.commit()

        output_filename = f"{task_id}.jpg"
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        img = _decode_image(file_path)
        # Gravação fica fora da decodificação: erro de disco é sempre passageiro.
        img.save(output_path, "JPEG", quality=85)

        task.status = "completed"
        task.result_url = f"/static/processed/{output_filename}"
        db.commit()

        return {"status": "completed", "result_url": task.result_url}

    except PermanentTaskError as exc:
        db.rollback()
        _send_to_dlq(
            db,
            task_id,
            file_path,
            str(exc),
            retry_count=self.request.retries,
            failure_kind="permanent",
        )
        return {"status": "failed", "error": str(exc), "dead_lettered": True}

    except Exception as exc:
        db.rollback()

        if self.request.retries >= self.max_retries:
            _send_to_dlq(
                db,
                task_id,
                file_path,
                str(exc),
                retry_count=self.request.retries,
                failure_kind="transient",
            )
            return {"status": "failed", "error": str(exc), "dead_lettered": True}

        _mark_retrying(db, task_id, str(exc))
        countdown = compute_backoff(
            self.request.retries,
            settings.retry_backoff_base_seconds,
            settings.retry_backoff_max_seconds,
            settings.retry_backoff_jitter_seconds,
        )
        raise self.retry(exc=exc, countdown=countdown)

    finally:
        db.close()
