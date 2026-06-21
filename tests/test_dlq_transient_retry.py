"""
Teste isolado: simula uma falha TRANSITÓRIA (não relacionada a imagem
corrompida — ex: erro genérico de I/O/rede) e confirma que o Celery
tenta novamente até `max_retries` antes de mandar a task para a DLQ.

Não depende de Docker/Postgres/Redis reais: o Celery roda em modo "eager"
(síncrono, dentro do próprio processo de teste) e as dependências externas
(SessionLocal, redis_client) são substituídas por mocks.
"""
from unittest.mock import MagicMock, patch

import pytest

from app.workers.celery_app import celery_app
from app.workers import tasks as worker_tasks


class FakeTransientError(Exception):
    """Exceção genérica que NÃO está na lista de falhas permanentes —
    deve acionar o caminho de retry, não o de DLQ direto."""


@pytest.fixture(autouse=True)
def eager_mode():
    """Faz o Celery executar a task de forma síncrona no próprio teste,
    incluindo o comportamento real de `self.retry()` (que reexecuta a task
    incrementando `self.request.retries`)."""
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = False
    yield
    celery_app.conf.task_always_eager = False


@pytest.fixture
def fake_task_record():
    record = MagicMock()
    record.id = "fake-task-id"
    return record


def test_transient_failure_retries_then_dead_letters(fake_task_record):
    fake_db = MagicMock()
    fake_db.query.return_value.filter.return_value.first.return_value = fake_task_record

    with patch.object(worker_tasks, "SessionLocal", return_value=fake_db), \
         patch.object(worker_tasks, "redis_client") as mock_redis, \
         patch.object(worker_tasks.Image, "open", side_effect=FakeTransientError("simulated network blip")):

        result = worker_tasks.process_image_task.apply(
            args=("fake-task-id", "/fake/path/image.jpg")
        )

    output = result.result

    # Esgotou as 3 tentativas (max_retries=3) e caiu na DLQ.
    assert output["dead_lettered"] is True
    assert output["status"] == "failed"
    assert "simulated network blip" in output["error"]

    # A task de verdade foi marcada como "retrying" e só virou "failed" no final.
    assert fake_task_record.status == "failed"

    # Foi para a DLQ: Postgres (via db.add) e Redis (via lpush) — exatamente uma vez,
    # só depois de esgotar os retries.
    assert fake_db.add.call_count == 1
    mock_redis.lpush.assert_called_once()


def test_permanent_failure_skips_retries_entirely(fake_task_record):
    """Contraste: erro de imagem corrompida deve ir direto pra DLQ,
    sem passar pelo caminho de retry (retry_count deve ficar em 0)."""
    fake_db = MagicMock()
    fake_db.query.return_value.filter.return_value.first.return_value = fake_task_record

    from PIL import UnidentifiedImageError

    with patch.object(worker_tasks, "SessionLocal", return_value=fake_db), \
         patch.object(worker_tasks, "redis_client") as mock_redis, \
         patch.object(worker_tasks.Image, "open", side_effect=UnidentifiedImageError("bad image")) as mock_open:

        result = worker_tasks.process_image_task.apply(
            args=("fake-task-id", "/fake/path/image.jpg")
        )

        # Só uma chamada a Image.open: nenhuma tentativa de retry foi feita.
        assert mock_open.call_count == 1

    output = result.result

    assert output["dead_lettered"] is True
    assert fake_db.add.call_count == 1
    mock_redis.lpush.assert_called_once()