"""
Reprocessamento de entradas da DLQ (POST /tasks/dead-letters/{id}/reprocess)
e filtros administrativos de GET /tasks/dead-letters.
"""
import hashlib
import io
import uuid
from unittest.mock import MagicMock, patch

import kombu.exceptions
import pytest
from PIL import Image

from app.core import dead_letters
from app.core.config import settings
from app.core.dead_letters import record_dead_letter, remove_notification
from app.models import DeadLetter, TaskRecord

ADMIN_KEY = "test-key"


@pytest.fixture
def admin(monkeypatch):
    monkeypatch.setattr(settings, "admin_api_key", ADMIN_KEY)
    return {"X-Admin-Key": ADMIN_KEY}


@pytest.fixture
def mock_redis():
    with patch.object(dead_letters, "redis_client") as client:
        yield client


def _png_bytes(color="red") -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (20, 20), color=color).save(buffer, format="PNG")
    return buffer.getvalue()


def _write(tmp_path, name, data: bytes) -> str:
    path = tmp_path / name
    path.write_bytes(data)
    return str(path)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _seed_failed(db, *, file_path, file_hash, failure_kind="transient", dl_status=None):
    """Cria uma tarefa failed e a entrada da DLQ correspondente."""
    task_id = str(uuid.uuid4())
    db.add(TaskRecord(id=task_id, status="failed", file_hash=file_hash, file_path=file_path))
    dead_letter_id = str(uuid.uuid4())
    dead_letter = DeadLetter(
        id=dead_letter_id,
        task_id=task_id,
        file_path=file_path,
        error_message="boom",
        retry_count=0,
        failure_kind=failure_kind,
    )
    if dl_status is not None:
        dead_letter.status = dl_status
    db.add(dead_letter)
    db.commit()
    return dead_letter_id, task_id


def _reprocess(client, dead_letter_id, headers, force=False):
    url = f"/tasks/dead-letters/{dead_letter_id}/reprocess"
    if force:
        url += "?force=true"
    return client.post(url, headers=headers)


def test_successful_reprocess_creates_new_task_and_keeps_original_failed(
    client, db_session, tmp_path, admin, mock_redis
):
    data = _png_bytes()
    path = _write(tmp_path, "foto.png", data)
    dl_id, original_task_id = _seed_failed(db_session, file_path=path, file_hash=_sha(data))

    response = _reprocess(client, dl_id, admin)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["dead_letter_id"] == dl_id
    new_task_id = body["task_id"]
    assert new_task_id != original_task_id

    client.mock_delay.assert_called_once_with(new_task_id, path)

    db_session.expire_all()
    dead_letter = db_session.get(DeadLetter, dl_id)
    assert dead_letter.status == "reprocessed"
    assert dead_letter.reprocessed_at is not None
    assert dead_letter.reprocessed_task_id == new_task_id
    assert db_session.get(TaskRecord, original_task_id).status == "failed"


def test_second_reprocess_of_same_entry_is_rejected(client, db_session, tmp_path, admin, mock_redis):
    data = _png_bytes()
    path = _write(tmp_path, "foto.png", data)
    dl_id, _ = _seed_failed(db_session, file_path=path, file_hash=_sha(data))

    first = _reprocess(client, dl_id, admin)
    second = _reprocess(client, dl_id, admin)

    assert second.status_code == 409
    assert second.json()["detail"] == "Dead letter already reprocessed"
    assert second.json()["reprocessed_task_id"] == first.json()["task_id"]
    assert client.mock_delay.call_count == 1


def test_missing_file_is_rejected_and_entry_stays_open(client, db_session, tmp_path, admin, mock_redis):
    dl_id, _ = _seed_failed(
        db_session,
        file_path=str(tmp_path / "sumiu.png"),
        file_hash=_sha(b"qualquer"),
    )

    response = _reprocess(client, dl_id, admin)

    assert response.status_code == 409
    assert response.json()["detail"] == "File is no longer available"
    db_session.expire_all()
    assert db_session.get(DeadLetter, dl_id).status == "open"
    client.mock_delay.assert_not_called()


def test_permanent_failure_needs_force(client, db_session, tmp_path, admin, mock_redis):
    data = _png_bytes()
    path = _write(tmp_path, "foto.png", data)
    dl_id, _ = _seed_failed(db_session, file_path=path, file_hash=_sha(data), failure_kind="permanent")

    refused = _reprocess(client, dl_id, admin)
    assert refused.status_code == 409
    assert refused.json()["detail"] == (
        "Only transient failures can be reprocessed; use force=true to override"
    )

    forced = _reprocess(client, dl_id, admin, force=True)
    assert forced.status_code == 200


def test_legacy_entry_without_failure_kind_needs_force(client, db_session, tmp_path, admin, mock_redis):
    data = _png_bytes()
    path = _write(tmp_path, "foto.png", data)
    dl_id, _ = _seed_failed(db_session, file_path=path, file_hash=_sha(data), failure_kind=None)

    refused = _reprocess(client, dl_id, admin)
    assert refused.status_code == 409

    forced = _reprocess(client, dl_id, admin, force=True)
    assert forced.status_code == 200


def test_in_progress_task_for_same_file_blocks_reprocess(client, db_session, tmp_path, admin, mock_redis):
    data = _png_bytes()
    path = _write(tmp_path, "foto.png", data)
    dl_id, _ = _seed_failed(db_session, file_path=path, file_hash=_sha(data))
    in_progress_id = "em-andamento"
    db_session.add(TaskRecord(id=in_progress_id, status="processing", file_hash=_sha(data)))
    db_session.commit()

    response = _reprocess(client, dl_id, admin)

    assert response.status_code == 409
    assert response.json()["detail"] == "A task for this file is already in progress"
    assert response.json()["task_id"] == in_progress_id
    db_session.expire_all()
    assert db_session.get(DeadLetter, dl_id).status == "open"
    client.mock_delay.assert_not_called()


def test_completed_task_for_same_file_is_reused_without_enqueue(client, db_session, tmp_path, admin, mock_redis):
    data = _png_bytes()
    path = _write(tmp_path, "foto.png", data)
    dl_id, _ = _seed_failed(db_session, file_path=path, file_hash=_sha(data))
    completed_id = "ja-concluida"
    db_session.add(
        TaskRecord(
            id=completed_id,
            status="completed",
            file_hash=_sha(data),
            result_url=f"/static/processed/{completed_id}.jpg",
        )
    )
    db_session.commit()

    response = _reprocess(client, dl_id, admin)

    assert response.status_code == 200
    assert response.json() == {"dead_letter_id": dl_id, "task_id": completed_id, "status": "completed"}
    client.mock_delay.assert_not_called()
    db_session.expire_all()
    dead_letter = db_session.get(DeadLetter, dl_id)
    assert dead_letter.status == "reprocessed"
    assert dead_letter.reprocessed_task_id == completed_id


def test_enqueue_failure_returns_503_and_records_new_dead_letter(client, db_session, tmp_path, admin, mock_redis):
    data = _png_bytes()
    path = _write(tmp_path, "foto.png", data)
    dl_id, _ = _seed_failed(db_session, file_path=path, file_hash=_sha(data))
    client.mock_delay.side_effect = kombu.exceptions.OperationalError("broker fora do ar")

    response = _reprocess(client, dl_id, admin)

    assert response.status_code == 503
    db_session.expire_all()
    assert db_session.get(DeadLetter, dl_id).status == "reprocessed"

    new_entries = db_session.query(DeadLetter).filter(DeadLetter.id != dl_id).all()
    assert len(new_entries) == 1
    new_entry = new_entries[0]
    assert new_entry.failure_kind == "transient"
    assert new_entry.status == "open"
    assert db_session.get(TaskRecord, new_entry.task_id).status == "failed"


def test_unknown_dead_letter_returns_404(client, admin):
    response = client.post("/tasks/dead-letters/nao-existe/reprocess", headers=admin)

    assert response.status_code == 404


@pytest.mark.parametrize("method, url", [
    ("get", "/tasks/dead-letters"),
    ("post", "/tasks/dead-letters/qualquer/reprocess"),
])
@pytest.mark.parametrize("headers", [{}, {"X-Admin-Key": "errada"}])
def test_wrong_or_missing_admin_key_returns_401(client, admin, method, url, headers):
    response = getattr(client, method)(url, headers=headers)

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid admin key"}


@pytest.mark.parametrize("method, url", [
    ("get", "/tasks/dead-letters"),
    ("post", "/tasks/dead-letters/qualquer/reprocess"),
])
def test_empty_admin_key_disables_admin_endpoints(client, monkeypatch, method, url):
    monkeypatch.setattr(settings, "admin_api_key", "")

    response = getattr(client, method)(url, headers={"X-Admin-Key": ""})

    assert response.status_code == 503
    assert response.json() == {"detail": "Admin API is not configured"}


def test_lrem_payload_matches_lpush_payload(mock_redis):
    db = MagicMock()
    record_dead_letter(
        db,
        "task-1",
        "/app/static/uploads/x.png",
        "falha de disco",
        retry_count=0,
        failure_kind="transient",
    )
    pushed_payload = mock_redis.lpush.call_args.args[1]

    dead_letter = DeadLetter(
        id="dl-1",
        task_id="task-1",
        file_path="/app/static/uploads/x.png",
        error_message="falha de disco",
    )
    remove_notification(dead_letter)

    mock_redis.lrem.assert_called_once_with(dead_letters.DLQ_REDIS_KEY, 0, pushed_payload)


def test_list_filters_by_status(client, db_session, tmp_path, admin):
    data = _png_bytes()
    path = _write(tmp_path, "foto.png", data)
    open_id, _ = _seed_failed(db_session, file_path=path, file_hash=_sha(data))
    reprocessed_id, _ = _seed_failed(
        db_session, file_path=path, file_hash="outro-hash", dl_status="reprocessed"
    )

    response = client.get("/tasks/dead-letters?status=open", headers=admin)

    assert response.status_code == 200
    ids = [item["id"] for item in response.json()]
    assert open_id in ids
    assert reprocessed_id not in ids
