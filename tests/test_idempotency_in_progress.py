"""
Idempotência para uploads em andamento, corrida entre uploads iguais, falha
no enfileiramento e proteção do worker contra mensagens tardias.
"""
import hashlib
import io
import os
from unittest.mock import MagicMock, patch

import kombu.exceptions
import pytest
from PIL import Image

import app.api.tasks as tasks_module
from app.core import dead_letters
from app.models import DeadLetter, TaskRecord
from app.workers import tasks as worker_tasks
from app.workers.celery_app import celery_app


def _png_bytes(color="red") -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (20, 20), color=color).save(buffer, format="PNG")
    return buffer.getvalue()


def _upload(client, data: bytes, name="foto.png"):
    return client.post("/process/image", files={"file": (name, data, "image/png")})


def _set_status(db_session, task_id, status):
    record = db_session.query(TaskRecord).filter(TaskRecord.id == task_id).first()
    record.status = status
    db_session.commit()


@pytest.mark.parametrize("status", ["pending", "processing", "retrying"])
def test_second_upload_while_in_progress_returns_existing_task(client, db_session, tmp_path, status):
    png = _png_bytes()
    first_id = _upload(client, png).json()["task_id"]
    _set_status(db_session, first_id, status)
    files_before = sorted(os.listdir(tmp_path))

    second = _upload(client, png, name="copia.png")

    assert second.status_code == 200
    assert second.json() == {"task_id": first_id, "status": status}
    assert client.mock_delay.call_count == 1  # só o primeiro upload enfileirou
    assert db_session.query(TaskRecord).count() == 1
    assert sorted(os.listdir(tmp_path)) == files_before  # nenhum arquivo novo nem .part


def test_upload_after_failed_task_creates_a_new_one(client, db_session):
    png = _png_bytes()
    first_id = _upload(client, png).json()["task_id"]
    _set_status(db_session, first_id, "failed")

    second = _upload(client, png)

    assert second.status_code == 200
    assert second.json()["task_id"] != first_id
    assert second.json()["status"] == "pending"
    assert db_session.query(TaskRecord).count() == 2
    assert client.mock_delay.call_count == 2


def test_race_between_identical_uploads_returns_winner_id(client, db_session, tmp_path, monkeypatch):
    png = _png_bytes()
    winner = TaskRecord(
        id="vencedora",
        status="pending",
        file_hash=hashlib.sha256(png).hexdigest(),
        file_path=None,
    )
    db_session.add(winner)
    db_session.commit()

    # Simula a janela da corrida: a primeira busca não vê a tarefa em andamento,
    # então o INSERT é tentado e o índice único parcial rejeita.
    original = tasks_module._find_in_progress
    calls = {"n": 0}

    def racing_lookup(db, file_hash):
        calls["n"] += 1
        if calls["n"] == 1:
            return None
        return original(db, file_hash)

    monkeypatch.setattr(tasks_module, "_find_in_progress", racing_lookup)

    response = _upload(client, png)

    assert response.status_code == 200
    assert response.json() == {"task_id": "vencedora", "status": "pending"}
    assert calls["n"] == 2
    assert db_session.query(TaskRecord).count() == 1
    assert os.listdir(tmp_path) == []  # o .part foi apagado e nada foi salvo
    client.mock_delay.assert_not_called()


def test_enqueue_failure_returns_503_and_dead_letters_task(client, db_session, tmp_path, mock_redis_dlq):
    client.mock_delay.side_effect = kombu.exceptions.OperationalError("broker fora do ar")
    png = _png_bytes()

    response = _upload(client, png)

    assert response.status_code == 503
    assert response.json() == {"detail": "Task queue unavailable, try again later"}

    record = db_session.query(TaskRecord).one()
    assert record.status == "failed"
    assert record.error_message == "failed to enqueue task"

    dead_letter = db_session.query(DeadLetter).one()
    assert dead_letter.failure_kind == "transient"
    assert dead_letter.retry_count == 0
    assert dead_letter.file_path == record.file_path

    # O arquivo final continua no disco, para ser reprocessado depois.
    assert os.path.exists(record.file_path)


def test_new_upload_of_same_file_is_accepted_after_enqueue_failure(client, db_session, mock_redis_dlq):
    png = _png_bytes()
    client.mock_delay.side_effect = kombu.exceptions.OperationalError("broker fora do ar")
    assert _upload(client, png).status_code == 503

    client.mock_delay.side_effect = None
    retry = _upload(client, png)

    assert retry.status_code == 200
    assert retry.json()["status"] == "pending"
    assert db_session.query(TaskRecord).count() == 2


def test_saved_file_path_uses_detected_extension(client, db_session, tmp_path):
    response = _upload(client, _png_bytes(), name="foto.jpg")

    record = db_session.query(TaskRecord).filter(TaskRecord.id == response.json()["task_id"]).one()
    assert record.file_path == os.path.join(str(tmp_path), f"{record.id}.png")
    assert os.path.exists(record.file_path)


# --- worker ---


@pytest.fixture
def eager():
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = False
    yield
    celery_app.conf.task_always_eager = False


def test_worker_skips_task_already_failed(eager, mock_redis_dlq):
    record = MagicMock()
    record.id = "fake-task-id"
    record.status = "failed"
    fake_db = MagicMock()
    fake_db.query.return_value.filter.return_value.first.return_value = record

    with patch.object(worker_tasks, "SessionLocal", return_value=fake_db), \
         patch.object(worker_tasks.Image, "open") as mock_open:
        result = worker_tasks.process_image_task.apply(args=("fake-task-id", "/fake/x.png"))

    assert result.result["status"] == "skipped"
    mock_open.assert_not_called()


@pytest.fixture
def mock_redis_dlq():
    with patch.object(dead_letters, "redis_client") as client:
        yield client
