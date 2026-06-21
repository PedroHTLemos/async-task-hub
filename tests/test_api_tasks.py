"""
Testes de integração da API (rotas em app/api/tasks.py), usando o
TestClient do FastAPI + SQLite em memória + Celery mockado (ver conftest.py).
"""
from app.models import DeadLetter, TaskRecord


def test_upload_enqueues_task_and_persists_record(client, db_session, sample_image_bytes):
    response = client.post(
        "/process/image",
        files={"file": ("photo.jpg", sample_image_bytes, "image/jpeg")},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert "task_id" in body

    # O Celery foi de fato chamado para processar essa imagem nova.
    client.mock_delay.assert_called_once()

    record = db_session.query(TaskRecord).filter(TaskRecord.id == body["task_id"]).first()
    assert record is not None
    assert record.status == "pending"
    assert record.file_hash  # hash foi calculado e persistido


def test_uploading_same_file_twice_is_idempotent(client, db_session, sample_image_bytes):
    # Primeiro upload: vai para a fila normalmente.
    first_response = client.post(
        "/process/image",
        files={"file": ("photo.jpg", sample_image_bytes, "image/jpeg")},
    )
    first_task_id = first_response.json()["task_id"]

    # Simula o worker tendo terminado de processar essa primeira task.
    record = db_session.query(TaskRecord).filter(TaskRecord.id == first_task_id).first()
    record.status = "completed"
    record.result_url = f"/static/processed/{first_task_id}.jpg"
    db_session.commit()

    client.mock_delay.reset_mock()

    # Segundo upload, MESMO conteúdo de arquivo (hash idêntico).
    second_response = client.post(
        "/process/image",
        files={"file": ("photo_copia.jpg", sample_image_bytes, "image/jpeg")},
    )

    assert second_response.status_code == 200
    body = second_response.json()

    # Já volta "completed" na hora, sem passar pela fila.
    assert body["status"] == "completed"
    client.mock_delay.assert_not_called()

    # O novo registro reaproveita o result_url do primeiro processamento.
    second_record = db_session.query(TaskRecord).filter(TaskRecord.id == body["task_id"]).first()
    assert second_record.result_url == record.result_url
    assert second_record.file_hash == record.file_hash
    # E é um registro novo, não o mesmo do primeiro upload (histórico preservado).
    assert second_record.id != first_task_id


def test_uploading_different_files_does_not_trigger_idempotency(client, db_session):
    image_a = _jpeg_bytes(color="red")
    image_b = _jpeg_bytes(color="green")

    response_a = client.post("/process/image", files={"file": ("a.jpg", image_a, "image/jpeg")})
    response_b = client.post("/process/image", files={"file": ("b.jpg", image_b, "image/jpeg")})

    assert response_a.json()["status"] == "pending"
    assert response_b.json()["status"] == "pending"
    assert client.mock_delay.call_count == 2  # cada imagem diferente dispara sua própria task


def test_get_task_status_returns_persisted_record(client, db_session):
    record = TaskRecord(
        id="known-task-id",
        status="completed",
        file_hash="abc123",
        result_url="/static/processed/known-task-id.jpg",
    )
    db_session.add(record)
    db_session.commit()

    response = client.get("/tasks/known-task-id")

    assert response.status_code == 200
    body = response.json()
    assert body["task_id"] == "known-task-id"
    assert body["status"] == "completed"
    assert body["result_url"] == "/static/processed/known-task-id.jpg"


def test_get_task_status_unknown_id_returns_404(client):
    response = client.get("/tasks/this-id-does-not-exist")
    assert response.status_code == 404


def test_dead_letters_endpoint_lists_failures(client, db_session):
    dead_letter = DeadLetter(
        task_id="failed-task-id",
        file_path="/code/app/static/uploads/failed-task-id.jpg",
        error_message="cannot identify image file",
        retry_count="0",
    )
    db_session.add(dead_letter)
    db_session.commit()

    response = client.get("/tasks/dead-letters")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["task_id"] == "failed-task-id"
    assert body[0]["error_message"] == "cannot identify image file"


def test_dead_letters_route_does_not_collide_with_task_id_route(client):
    """Garante que /tasks/dead-letters é resolvido pela rota específica e não
    é interpretado como /tasks/{task_id} com task_id='dead-letters'."""
    response = client.get("/tasks/dead-letters")

    # Se a rota dinâmica tivesse capturado primeiro, isso daria 404
    # (TaskStatusResponse exige campos que não existem para um id inexistente).
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def _jpeg_bytes(color: str) -> bytes:
    import io
    from PIL import Image

    buffer = io.BytesIO()
    Image.new("RGB", (100, 100), color=color).save(buffer, format="JPEG")
    buffer.seek(0)
    return buffer.read()