import contextlib
import hashlib
import os
import uuid
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.database import get_db
from app.core.dead_letters import record_dead_letter, remove_notification
from app.core.file_types import detect_image_type
from app.core.limiter import limiter
from app.core.security import require_admin
from app.core.task_status import (
    COMPLETED,
    DEAD_LETTER_OPEN,
    DEAD_LETTER_REPROCESSED,
    IN_PROGRESS,
    PENDING,
)
from app.models import DeadLetter, TaskRecord
from app.schemas.task import (
    DeadLetterResponse,
    ReprocessResponse,
    TaskCreatedResponse,
    TaskStatusResponse,
)
from app.workers.tasks import process_image_task

router = APIRouter()


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
    part_path = os.path.join(settings.upload_dir, f"{task_id}.part")
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

        saved_path = os.path.join(settings.upload_dir, f"{task_id}.{extension}")

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
        except Exception as exc:
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
            ) from exc

        return TaskCreatedResponse(task_id=task_id, status=PENDING)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.remove(part_path)


def _sha256_of_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as source:
        while True:
            chunk = source.read(UPLOAD_CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _conflict(db: Session, detail: str, **extra) -> JSONResponse:
    db.rollback()
    return JSONResponse(status_code=409, content={"detail": detail, **extra})


@router.get(
    "/tasks/dead-letters",
    response_model=list[DeadLetterResponse],
    dependencies=[Depends(require_admin)],
)
def list_dead_letters(
    status: Literal[DEAD_LETTER_OPEN, DEAD_LETTER_REPROCESSED] | None = Query(default=None),
    db: Session = Depends(get_db),
):
    query = db.query(DeadLetter)
    if status is not None:
        query = query.filter(DeadLetter.status == status)
    return query.order_by(DeadLetter.failed_at.desc()).limit(50).all()


@router.post(
    "/tasks/dead-letters/{dead_letter_id}/reprocess",
    response_model=ReprocessResponse,
    dependencies=[Depends(require_admin)],
)
def reprocess_dead_letter(
    dead_letter_id: str,
    force: bool = False,
    db: Session = Depends(get_db),
):
    """Reprocessa uma entrada da DLQ, numa única transação.

    Só falhas transientes são reprocessadas, a menos que force=true. Se já
    existe tarefa em andamento para o mesmo arquivo, nada é criado. Se já
    existe tarefa concluída, a entrada aponta para ela sem enfileirar. Em
    qualquer outro caso, cria uma tarefa nova e preserva a antiga como failed.
    """
    dead_letter = (
        db.query(DeadLetter).filter(DeadLetter.id == dead_letter_id).with_for_update().first()
    )
    if dead_letter is None:
        db.rollback()
        raise HTTPException(status_code=404, detail="Dead letter not found")

    if dead_letter.status == DEAD_LETTER_REPROCESSED:
        return _conflict(
            db,
            "Dead letter already reprocessed",
            reprocessed_task_id=dead_letter.reprocessed_task_id,
        )

    if dead_letter.failure_kind != "transient" and not force:
        return _conflict(
            db,
            "Only transient failures can be reprocessed; use force=true to override",
        )

    if not dead_letter.file_path or not os.path.isfile(dead_letter.file_path):
        return _conflict(db, "File is no longer available")

    file_hash = _sha256_of_file(dead_letter.file_path)

    in_progress = _find_in_progress(db, file_hash)
    if in_progress:
        return _conflict(
            db,
            "A task for this file is already in progress",
            task_id=in_progress.id,
        )

    completed = (
        db.query(TaskRecord)
        .filter(TaskRecord.file_hash == file_hash, TaskRecord.status == COMPLETED)
        .first()
    )
    now = datetime.now(UTC)

    if completed:
        dead_letter.status = DEAD_LETTER_REPROCESSED
        dead_letter.reprocessed_at = now
        dead_letter.reprocessed_task_id = completed.id
        db.commit()
        remove_notification(dead_letter)
        return ReprocessResponse(
            dead_letter_id=dead_letter_id,
            task_id=completed.id,
            status=COMPLETED,
        )

    new_task_id = str(uuid.uuid4())
    db.add(
        TaskRecord(
            id=new_task_id,
            status=PENDING,
            file_hash=file_hash,
            file_path=dead_letter.file_path,
        )
    )
    dead_letter.status = DEAD_LETTER_REPROCESSED
    dead_letter.reprocessed_at = now
    dead_letter.reprocessed_task_id = new_task_id

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        winner = _find_in_progress(db, file_hash)
        if winner is None:
            raise
        return _conflict(
            db,
            "A task for this file is already in progress",
            task_id=winner.id,
        )

    remove_notification(dead_letter)

    file_path = dead_letter.file_path
    try:
        process_image_task.delay(new_task_id, file_path)
    except Exception as exc:
        record_dead_letter(
            db,
            new_task_id,
            file_path,
            error_message="failed to enqueue task",
            retry_count=0,
            failure_kind="transient",
        )
        raise HTTPException(
            status_code=503,
            detail="Task queue unavailable, try again later",
        ) from exc

    return ReprocessResponse(
        dead_letter_id=dead_letter_id,
        task_id=new_task_id,
        status=PENDING,
    )


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
