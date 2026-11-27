"""
Utilitários dos testes de integração: espera determinística e leitura do banco.

Os testes não usam tempo fixo de espera. Eles repetem uma verificação com
`wait_for` até o resultado aparecer ou até o prazo estourar.
"""

import hashlib
import io
import time

import httpx
from PIL import Image

from app.core import database
from app.models import DeadLetter, TaskRecord


def wait_for(predicate, timeout=10.0, interval=0.05, description="condição"):
    """Repete `predicate` até devolver um valor verdadeiro e devolve esse valor.

    Se o prazo estourar, levanta AssertionError com a descrição do que se esperava.
    """
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise AssertionError(f"{description} não aconteceu em {timeout}s")
        time.sleep(interval)


def jpeg_bytes(size=(1600, 1200)) -> bytes:
    """Gera uma imagem JPEG válida, com conteúdo não trivial para o arquivo ter tamanho real."""
    image = Image.effect_noise(size, 64).convert("RGB")
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()


def sha256_of(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def post_image(client: httpx.Client, content: bytes, name: str = "imagem.jpg") -> httpx.Response:
    return client.post("/process/image", files={"file": (name, content, "image/jpeg")})


def task_status(task_id: str) -> str | None:
    with database.SessionLocal() as session:
        task = session.get(TaskRecord, task_id)
        return task.status if task else None


def task_row(task_id: str) -> dict | None:
    """Lê a tarefa numa sessão nova.

    Devolve um dict, para não depender de objetos ligados à sessão.
    """
    with database.SessionLocal() as session:
        task = session.get(TaskRecord, task_id)
        if task is None:
            return None
        return {
            "status": task.status,
            "error_message": task.error_message,
            "result_url": task.result_url,
            "file_hash": task.file_hash,
            "file_path": task.file_path,
        }


def dead_letters_for(task_id: str) -> list[dict]:
    with database.SessionLocal() as session:
        rows = session.query(DeadLetter).filter(DeadLetter.task_id == task_id).all()
        return [
            {
                "id": row.id,
                "failure_kind": row.failure_kind,
                "retry_count": row.retry_count,
                "status": row.status,
                "reprocessed_task_id": row.reprocessed_task_id,
                "error_message": row.error_message,
            }
            for row in rows
        ]


def count_tasks(file_hash: str | None = None) -> int:
    with database.SessionLocal() as session:
        query = session.query(TaskRecord)
        if file_hash is not None:
            query = query.filter(TaskRecord.file_hash == file_hash)
        return query.count()
