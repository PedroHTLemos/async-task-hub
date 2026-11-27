"""
Fluxo ponta a ponta com Postgres, Redis, broker e worker reais.

Cobre o caminho feliz, o retry com backoff passando pelo broker, a DLQ e o
reprocessamento, além da falha permanente que vai direto para a DLQ.
"""
import errno
from pathlib import Path

from PIL import Image

from app.core import dead_letters
from app.core.config import settings
from app.core.dead_letters import DLQ_REDIS_KEY
from app.workers.celery_app import celery_app
from tests.integration.helpers import (
    dead_letters_for,
    jpeg_bytes,
    post_image,
    task_row,
    task_status,
    wait_for,
)


def test_upload_reaches_completed_with_resized_jpeg(api, redis_url):
    """O upload responde pending e, depois, a tarefa vira completed com a imagem reduzida."""
    # O broker é o Redis do container, e não o memory:// do ambiente de CI.
    assert celery_app.conf.broker_url == redis_url

    response = post_image(api, jpeg_bytes((1600, 1200)))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    task_id = response.json()["task_id"]

    wait_for(lambda: task_status(task_id) == "completed", description="tarefa completed")

    output = Path(settings.output_dir) / f"{task_id}.jpg"
    assert output.is_file()
    assert output.read_bytes()[:3] == b"\xff\xd8\xff"
    with Image.open(output) as image:
        assert image.format == "JPEG"
        assert max(image.size) <= 800

    row = task_row(task_id)
    assert row["result_url"] == f"/static/processed/{task_id}.jpg"


def test_transient_failure_retries_through_broker_then_dlq_then_reprocess(api, admin_headers, monkeypatch):
    """Falha de disco passa pelos retries reais, vai para a DLQ e é reprocessada depois."""
    monkeypatch.setattr(settings, "retry_backoff_base_seconds", 0.01)
    monkeypatch.setattr(settings, "retry_backoff_max_seconds", 0.05)
    monkeypatch.setattr(settings, "retry_backoff_jitter_seconds", 0.0)

    attempts = []

    def failing_save(self, *args, **kwargs):
        attempts.append(1)
        raise OSError(errno.EIO, "disco com erro simulado")

    # A imagem é gerada antes do patch: o helper também usa Image.Image.save.
    content = jpeg_bytes()

    with monkeypatch.context() as mp:
        mp.setattr(Image.Image, "save", failing_save)

        response = post_image(api, content)
        assert response.status_code == 200, response.text
        task_id = response.json()["task_id"]

        wait_for(lambda: task_status(task_id) == "failed", timeout=15, description="tarefa failed")

    # Uma tentativa inicial mais um retry por vez, cada um passando pelo broker.
    assert len(attempts) == settings.task_max_retries + 1

    row = task_row(task_id)
    assert "disco com erro simulado" in row["error_message"]

    entries = dead_letters_for(task_id)
    assert len(entries) == 1
    assert entries[0]["failure_kind"] == "transient"
    assert entries[0]["retry_count"] == settings.task_max_retries
    assert entries[0]["status"] == "open"
    assert dead_letters.redis_client.llen(DLQ_REDIS_KEY) == 1

    # Sem o patch, o reprocessamento pelo endpoint administrativo deve concluir a tarefa.
    response = api.post(
        f"/tasks/dead-letters/{entries[0]['id']}/reprocess",
        headers=admin_headers,
    )
    assert response.status_code == 200, response.text
    new_task_id = response.json()["task_id"]
    assert new_task_id != task_id

    wait_for(lambda: task_status(new_task_id) == "completed", description="tarefa reprocessada completed")
    assert (Path(settings.output_dir) / f"{new_task_id}.jpg").is_file()

    # A tarefa antiga continua failed, como histórico. A entrada da DLQ aponta para a nova.
    assert task_status(task_id) == "failed"
    entry = dead_letters_for(task_id)[0]
    assert entry["status"] == "reprocessed"
    assert entry["reprocessed_task_id"] == new_task_id
    assert dead_letters.redis_client.llen(DLQ_REDIS_KEY) == 0


def test_truncated_jpeg_is_permanent_dead_letter_and_cannot_be_reprocessed(api, admin_headers):
    """Um JPEG truncado é erro permanente: vai para a DLQ sem retry e não reprocessa sem force."""
    full = jpeg_bytes((800, 600))
    truncated = full[: len(full) // 2]

    response = post_image(api, truncated)
    assert response.status_code == 200, response.text
    task_id = response.json()["task_id"]

    wait_for(lambda: task_status(task_id) == "failed", description="tarefa failed")

    entries = dead_letters_for(task_id)
    assert len(entries) == 1
    assert entries[0]["failure_kind"] == "permanent"
    assert entries[0]["retry_count"] == 0

    response = api.post(
        f"/tasks/dead-letters/{entries[0]['id']}/reprocess",
        headers=admin_headers,
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == (
        "Only transient failures can be reprocessed; use force=true to override"
    )
