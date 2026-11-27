import contextlib
import hashlib
import os
import uuid
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.dead_letters import record_dead_letter
from app.models import TaskRecord, DeadLetter
from app.schemas.task import TaskCreatedResponse, TaskStatusResponse
from app.workers.tasks import process_image_task
from app.core.config import settings
from app.core.file_types import detect_image_type
from app.core.task_status import COMPLETED, IN_PROGRESS, PENDING
from app.core.limiter import limiter

router = APIRouter()

UPLOAD_DIR = "app/static/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


def _upload_rate_limit() -> str:
    # slowapi chama este callable a cada requisição, então a leitura de
    # settings.rate_limit_per_minute acontece no momento do request.
    return f"{settings.rate_limit_per_minute}/minute"


UPLOAD_CHUNK_SIZE = 1024 * 1024


def _read_upload_to_part(source, part_path: str, max_bytes: int) -> tuple[str, str]:
    """Copia o upload para `part_path` em blocos, calculando o SHA-256 e
    validando tamanho e tipo. Devolve (hash, extensão detectada).

    Levanta HTTPException 415 se o tipo não for imagem permitida e 413 se o
    arquivo passar de `max_bytes`. Quem chama remove o `.part` em caso de erro.
    """
    hasher = hashlib.sha256()
    total = 0
    extension = None

    with open(part_path, "wb") as buffer:
        while True:
            chunk = source.read(UPLOAD_CHUNK_SIZE)
            if not chunk:
                break

            if extension is None:
                extension = detect_image_type(chunk[:12])
                if extension is None:
                    raise HTTPException(
                        status_code=415,
                        detail="Unsupported file type. Allowed: JPEG, PNG, WebP",
                    )

            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large. Max size is {settings.max_upload_mb} MB",
                )

            hasher.update(chunk)
            buffer.write(chunk)

    if extension is None:
        raise HTTPException(
            status_code=415,
            detail="Unsupported file type. Allowed: JPEG, PNG, WebP",
        )

    return hasher.hexdigest(), extension


def _find_in_progress(db: Session, file_hash: str) -> TaskRecord | None:
    return (
        db.query(TaskRecord)
        .filter(TaskRecord.file_hash == file_hash, TaskRecord.status.in_(IN_PROGRESS))
        .first()
    )


@router.post("/process/image", response_model=TaskCreatedResponse)
@limiter.limit(_upload_rate_limit)
def process_image(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Enfileira o processamento de uma imagem.

    Um arquivo igual a um já processado (`completed`) gera um registro novo
    com o `result_url` reaproveitado, para preservar o histórico. Um arquivo
    igual a outro ainda em andamento devolve o `task_id` existente, porque
    ainda não há resultado para reaproveitar.
    """
    task_id = str(uuid.uuid4())
    part_path = os.path.join(UPLOAD_DIR, f"{task_id}.part")
    max_bytes = settings.max_upload_mb * 1024 * 1024

    try:
        file_hash, extension = _read_upload_to_part(file.file, part_path, max_bytes)

        existing = (
            db.query(TaskRecord)
            .filter(TaskRecord.file_hash == file_hash, TaskRecord.status == COMPLETED)
            .first()
        )

        if existing:
            task_record = TaskRecord(
                id=task_id,
                status=COMPLETED,
                file_hash=file_hash,
                result_url=existing.result_url,
            )
            db.add(task_record)
            db.commit()

            return TaskCreatedResponse(task_id=task_id, status=COMPLETED)

        in_progress = _find_in_progress(db, file_hash)
        if in_progress:
            return TaskCreatedResponse(task_id=in_progress.id, status=in_progress.status)

        saved_path = os.path.join(UPLOAD_DIR, f"{task_id}.{extension}")

        # O registro é a trava: o índice único parcial só deixa um upload em
        # andamento por hash. Quem perde a corrida recebe o ID de quem ganhou.
        db.add(TaskRecord(id=task_id, status=PENDING, file_hash=file_hash, file_path=saved_path))
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            winner = _find_in_progress(db, file_hash)
            if winner is None:
                raise
            return TaskCreatedResponse(task_id=winner.id, status=winner.status)

        # Só depois do commit o arquivo vai para o caminho final.
        os.replace(part_path, saved_path)

        try:
            process_image_task.delay(task_id, saved_path)
        except Exception:
            # Marca como failed, o que libera o índice. O arquivo fica no disco
            # para ser reprocessado depois.
            record_dead_letter(
                db,
                task_id,
                saved_path,
                error_message="failed to enqueue task",
                retry_count=0,
                failure_kind="transient",
            )
            raise HTTPException(
                status_code=503,
                detail="Task queue unavailable, try again later",
            )

        return TaskCreatedResponse(task_id=task_id, status=PENDING)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.remove(part_path)


@router.get("/tasks/dead-letters")
def list_dead_letters(db: Session = Depends(get_db)):
    dead_letters = db.query(DeadLetter).order_by(DeadLetter.failed_at.desc()).limit(50).all()
    return [
        {
            "id": dl.id,
            "task_id": dl.task_id,
            "file_path": dl.file_path,
            "error_message": dl.error_message,
            "retry_count": dl.retry_count,
            "failure_kind": dl.failure_kind,
            "failed_at": dl.failed_at,
        }
        for dl in dead_letters
    ]
@router.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task_status(task_id: str, db: Session = Depends(get_db)):
    task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return TaskStatusResponse(
        task_id=task.id,
        status=task.status,
        result_url=task.result_url,
        error_message=task.error_message,
        created_at=task.created_at,
    )