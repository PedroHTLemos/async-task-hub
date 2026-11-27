"""
Concorrência real contra Postgres: índice único parcial e SELECT ... FOR UPDATE.

Os testes usam uvicorn de verdade e threads de cliente. Para provar que a
corrida aconteceu, as requisições são forçadas a se encontrar numa barreira
logo antes do INSERT, e o teste confirma que todas passaram por ela.
"""
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy import text

from app.api import tasks as tasks_module
from app.core import database
from app.core.config import settings
from app.core.task_status import FAILED
from app.models import DeadLetter, TaskRecord
from app.workers import tasks as worker_tasks
from tests.integration.helpers import (
    count_tasks,
    jpeg_bytes,
    post_image,
    sha256_of,
    task_status,
    wait_for,
)

CONCURRENT_UPLOADS = 10


def test_ten_simultaneous_uploads_of_same_file_create_one_task(api_base_url, monkeypatch):
    """Dez uploads simultâneos do mesmo arquivo geram uma única tarefa e um único arquivo salvo."""
    content = jpeg_bytes((1200, 900))
    file_hash = sha256_of(content)

    barrier = threading.Barrier(CONCURRENT_UPLOADS, timeout=15)
    original_lookup = tasks_module._find_in_progress
    lock = threading.Lock()
    first_lookup_threads: set[int] = set()
    threads_past_barrier: list[int] = []

    def lookup_with_barrier(db, lookup_hash):
        result = original_lookup(db, lookup_hash)
        ident = threading.get_ident()
        with lock:
            is_first_lookup = ident not in first_lookup_threads
            first_lookup_threads.add(ident)
        # Só a primeira busca de cada requisição espera. Todas leram "nada em andamento"
        # antes de qualquer INSERT, então a corrida no índice acontece de verdade.
        if is_first_lookup:
            barrier.wait()
            with lock:
                threads_past_barrier.append(ident)
        return result

    monkeypatch.setattr(tasks_module, "_find_in_progress", lookup_with_barrier)

    def send(client: httpx.Client) -> httpx.Response:
        return post_image(client, content)

    with httpx.Client(base_url=api_base_url, timeout=30) as client:
        with ThreadPoolExecutor(max_workers=CONCURRENT_UPLOADS) as pool:
            futures = [pool.submit(send, client) for _ in range(CONCURRENT_UPLOADS)]
            responses = [future.result() for future in futures]

    for response in responses:
        assert response.status_code == 200, response.text

    # A barreira foi atingida pelas 10 threads: prova que a simultaneidade aconteceu.
    assert len(first_lookup_threads) == CONCURRENT_UPLOADS
    assert len(threads_past_barrier) == CONCURRENT_UPLOADS
    assert not barrier.broken

    task_ids = {response.json()["task_id"] for response in responses}
    assert len(task_ids) == 1, f"esperava um único task_id, veio {task_ids}"
    task_id = task_ids.pop()

    assert count_tasks(file_hash) == 1

    upload_dir = Path(settings.upload_dir)
    assert [path.name for path in upload_dir.iterdir()] == [f"{task_id}.jpg"]

    wait_for(lambda: task_status(task_id) == "completed", description="tarefa completed")


def _insert_transient_failure(content: bytes) -> str:
    """Cria uma tarefa failed e uma entrada transitória da DLQ apontando para um arquivo válido."""
    file_path = Path(settings.upload_dir) / f"{uuid4()}.jpg"
    file_path.write_bytes(content)

    original_id = str(uuid4())
    with database.SessionLocal() as session:
        session.add(
            TaskRecord(
                id=original_id,
                status=FAILED,
                file_hash=sha256_of(content),
                file_path=str(file_path),
                error_message="falha simulada",
            )
        )
        dead_letter = DeadLetter(
            task_id=original_id,
            file_path=str(file_path),
            error_message="falha simulada",
            retry_count=settings.task_max_retries,
            failure_kind="transient",
        )
        session.add(dead_letter)
        session.commit()
        return dead_letter.id


def _count_lock_waiters_on_dead_letters() -> int:
    """Conta sessões que estão esperando um lock dentro de um SELECT ... FOR UPDATE em dead_letters."""
    query = text(
        """
        SELECT count(*) FROM pg_stat_activity
        WHERE datname = current_database()
          AND wait_event_type = 'Lock'
          AND query ILIKE '%dead_letters%'
          AND query ILIKE '%FOR UPDATE%'
        """
    )
    with database.engine.connect() as connection:
        return connection.execute(query).scalar_one()


def _second_request_is_blocked(timeout: float) -> bool:
    try:
        wait_for(
            lambda: _count_lock_waiters_on_dead_letters() >= 1,
            timeout=timeout,
            description="segunda requisição esperando o lock",
        )
    except AssertionError:
        return False
    return True


def test_two_simultaneous_reprocess_calls_on_same_entry(api, admin_headers, monkeypatch):
    """Duas chamadas simultâneas de reprocessamento: a segunda espera o lock e recebe 409."""
    dead_letter_id = _insert_transient_failure(jpeg_bytes((800, 600)))

    entered = threading.Event()
    release = threading.Event()
    original_sha = tasks_module._sha256_of_file
    calls: list[str] = []

    def sha_with_pause(path: str) -> str:
        # Só a primeira chamada pausa. Ela acontece com o FOR UPDATE já adquirido.
        is_first = not calls
        calls.append(path)
        if is_first:
            entered.set()
            release.wait(timeout=15)
        return original_sha(path)

    monkeypatch.setattr(tasks_module, "_sha256_of_file", sha_with_pause)

    # Segura o worker na tarefa criada pelo reprocessamento: ela fica em andamento
    # até a liberação. Sem isso, o worker poderia concluí-la antes da hora, e o
    # resultado do teste dependeria da velocidade dele.
    worker_release = threading.Event()
    original_decode = worker_tasks._decode_image

    def decode_held(path: str):
        worker_release.wait(timeout=15)
        return original_decode(path)

    monkeypatch.setattr(worker_tasks, "_decode_image", decode_held)

    url = f"/tasks/dead-letters/{dead_letter_id}/reprocess"
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            future_a = pool.submit(api.post, url, headers=admin_headers)
            assert entered.wait(timeout=15), "a primeira requisição não chegou ao ponto de pausa"

            future_b = pool.submit(api.post, url, headers=admin_headers)
            blocked = _second_request_is_blocked(timeout=5)
        finally:
            # Sempre libera A e o worker, mesmo que o teste falhe, para não segurar o lock.
            release.set()
            worker_release.set()

        response_a = future_a.result(timeout=15)
        response_b = future_b.result(timeout=15)

    assert response_a.status_code == 200, response_a.text
    assert response_b.status_code == 409, response_b.text
    assert response_b.json()["detail"] == "Dead letter already reprocessed"

    new_task_id = response_a.json()["task_id"]
    wait_for(lambda: task_status(new_task_id) == "completed", description="tarefa reprocessada completed")

    # Uma única tarefa nova: a original mais a reprocessada.
    assert count_tasks() == 2

    # A verificação da espera vem por último. Se a serialização falhar, a mensagem de
    # detalhe acima já aponta a causa.
    assert blocked, "a segunda requisição não ficou bloqueada no FOR UPDATE"
