import logging
import os
import warnings

from celery.signals import worker_process_init
from PIL import Image
from app.core.dead_letters import record_dead_letter
from app.core.task_status import COMPLETED, FAILED, PROCESSING, RETRYING
from app.workers.backoff import compute_backoff
from app.workers.celery_app import celery_app
from app.workers.errors import PermanentTaskError, classify_decode_error
from app.core import database
from app.core.config import settings
from app.models import TaskRecord

Image.MAX_IMAGE_PIXELS = settings.max_image_pixels

logger = logging.getLogger(__name__)

@worker_process_init.connect
def _ensure_output_dir(**kwargs) -> None:
    # Roda em cada processo filho do worker, antes da primeira tarefa. Criar o
    # diretório na importação faria isso em todo processo que importa o módulo,
    # inclusive a API e o beat, que não escrevem nele.
    os.makedirs(settings.output_dir, exist_ok=True)


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


def _mark_retrying(db, task_id: str, error_message: str) -> None:
    task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
    if task:
        task.status = RETRYING
        task.error_message = error_message
        db.commit()


# O status fica no Postgres e nenhum código lê resultados do Celery. Com
# ignore_result, o publish não consulta o backend Redis, que levava ~90 s para
# desistir quando o Redis estava fora do ar.
@celery_app.task(
    name="process_image_task",
    bind=True,
    max_retries=settings.task_max_retries,
    ignore_result=True,
)
def process_image_task(self, task_id: str, file_path: str):
    db = database.SessionLocal()
    try:
        task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
        if not task:
            return {"status": FAILED, "error": "task not found"}

        # Mensagem que chega depois de a tarefa já ter sido concluída ou
        # marcada como falha (ex.: pela rotina de tarefas travadas).
        # PROCESSING não entra aqui de propósito: com acks_late, uma mensagem
        # reentregue depois de um worker morrer precisa ser processada.
        if task.status in (COMPLETED, FAILED):
            return {"status": "skipped", "reason": f"task already {task.status}"}

        task.status = PROCESSING
        db.commit()

        output_filename = f"{task_id}.jpg"
        output_path = os.path.join(settings.output_dir, output_filename)

        img = _decode_image(file_path)
        # Gravação fica fora da decodificação: erro de disco é sempre passageiro.
        img.save(output_path, "JPEG", quality=85)

        task.status = COMPLETED
        task.result_url = f"/static/processed/{output_filename}"
        db.commit()

        return {"status": COMPLETED, "result_url": task.result_url}

    except PermanentTaskError as exc:
        db.rollback()
        record_dead_letter(
            db,
            task_id,
            file_path,
            str(exc),
            retry_count=self.request.retries,
            failure_kind="permanent",
        )
        return {"status": FAILED, "error": str(exc), "dead_lettered": True}

    except Exception as exc:
        db.rollback()

        if self.request.retries >= self.max_retries:
            record_dead_letter(
                db,
                task_id,
                file_path,
                str(exc),
                retry_count=self.request.retries,
                failure_kind="transient",
            )
            return {"status": FAILED, "error": str(exc), "dead_lettered": True}

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
