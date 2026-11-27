"""
Validação do upload em POST /process/image: tamanho (middleware + endpoint),
tipo real pelos magic bytes, limpeza de arquivos temporários e proteção
contra decompression bomb no worker.
"""
import io
import os

import pytest
from PIL import Image

import app.api.tasks as tasks_module
from app.core.config import settings
from app.core.file_types import detect_image_type
from app.models import TaskRecord
from app.workers.tasks import _open_image

JPEG_HEADER = b"\xff\xd8\xff\xe0" + b"\x00" * 16
MB = 1024 * 1024


def _png_bytes(size=(20, 20)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", size, color="red").save(buffer, format="PNG")
    return buffer.getvalue()


def _webp_bytes() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (20, 20), color="green").save(buffer, format="WEBP")
    return buffer.getvalue()


def _saved_files(tmp_path):
    return sorted(os.listdir(tmp_path))


def _spy_on_endpoint_read(monkeypatch):
    """Registra quando o endpoint chega a ler o upload. Serve para provar que
    o middleware rejeitou o corpo antes de a aplicação ser chamada."""
    calls = []
    original = tasks_module._read_upload_to_part

    def spy(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(tasks_module, "_read_upload_to_part", spy)
    return calls


def test_text_renamed_to_jpg_is_rejected_with_415(client, db_session, tmp_path):
    response = client.post(
        "/process/image",
        files={"file": ("foto.jpg", b"isto e texto, nao uma imagem", "image/jpeg")},
    )

    assert response.status_code == 415
    assert response.json() == {"detail": "Unsupported file type. Allowed: JPEG, PNG, WebP"}
    client.mock_delay.assert_not_called()
    assert db_session.query(TaskRecord).count() == 0
    assert _saved_files(tmp_path) == []


def test_real_png_sent_as_jpg_is_saved_with_png_extension(client, tmp_path):
    response = client.post(
        "/process/image",
        files={"file": ("foto.jpg", _png_bytes(), "image/jpeg")},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "pending"

    saved = _saved_files(tmp_path)
    assert len(saved) == 1
    assert saved[0].endswith(".png")


def test_real_webp_is_saved_with_webp_extension(client, tmp_path):
    response = client.post(
        "/process/image",
        files={"file": ("foto.bin", _webp_bytes(), "application/octet-stream")},
    )

    assert response.status_code == 200
    saved = _saved_files(tmp_path)
    assert len(saved) == 1
    assert saved[0].endswith(".webp")


def test_file_above_limit_is_rejected_by_content_length(client, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "max_upload_mb", 1)
    endpoint_calls = _spy_on_endpoint_read(monkeypatch)

    payload = JPEG_HEADER + b"\x00" * int(1.2 * MB)
    response = client.post(
        "/process/image",
        files={"file": ("grande.jpg", payload, "image/jpeg")},
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "File too large. Max size is 1 MB"}
    assert endpoint_calls == []  # rejeitado pelo middleware, antes do endpoint
    client.mock_delay.assert_not_called()
    assert _saved_files(tmp_path) == []


def test_file_above_limit_within_middleware_margin_is_rejected_by_endpoint(
    client, tmp_path, monkeypatch
):
    # Corpo abaixo de limite + 64 KB (passa pelo middleware), mas arquivo acima
    # de 1 MB: quem barra é a leitura em blocos do endpoint.
    monkeypatch.setattr(settings, "max_upload_mb", 1)
    endpoint_calls = _spy_on_endpoint_read(monkeypatch)

    payload = JPEG_HEADER + b"\x00" * (MB + 1000)
    response = client.post(
        "/process/image",
        files={"file": ("quase.jpg", payload, "image/jpeg")},
    )

    assert response.status_code == 413
    assert len(endpoint_calls) == 1
    assert _saved_files(tmp_path) == []  # o .part foi apagado
    client.mock_delay.assert_not_called()


def test_chunked_body_without_content_length_is_rejected_by_receive_count(
    client, tmp_path, monkeypatch
):
    monkeypatch.setattr(settings, "max_upload_mb", 1)
    endpoint_calls = _spy_on_endpoint_read(monkeypatch)

    boundary = "limiteteste"
    file_part = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="grande.jpg"\r\n'
        "Content-Type: image/jpeg\r\n\r\n"
    ).encode()
    payload = JPEG_HEADER + b"\x00" * int(1.2 * MB)
    body = file_part + payload + f"\r\n--{boundary}--\r\n".encode()

    def chunks():
        step = 64 * 1024
        for start in range(0, len(body), step):
            yield body[start : start + step]

    request = client.build_request(
        "POST",
        "/process/image",
        content=chunks(),
        headers={"content-type": f"multipart/form-data; boundary={boundary}"},
    )
    # Confirma que a requisição realmente foi enviada sem Content-Length.
    assert "content-length" not in request.headers
    assert request.headers.get("transfer-encoding") == "chunked"

    response = client.send(request)

    assert response.status_code == 413
    assert response.json() == {"detail": "File too large. Max size is 1 MB"}
    assert endpoint_calls == []  # só a contagem no receive explica o 413 aqui
    client.mock_delay.assert_not_called()
    assert _saved_files(tmp_path) == []


def test_empty_file_is_rejected_with_415(client, tmp_path):
    response = client.post(
        "/process/image",
        files={"file": ("vazio.jpg", b"", "image/jpeg")},
    )

    assert response.status_code == 415
    client.mock_delay.assert_not_called()
    assert _saved_files(tmp_path) == []


def test_repeated_completed_upload_leaves_no_new_files(client, db_session, tmp_path):
    png = _png_bytes()
    first = client.post("/process/image", files={"file": ("a.png", png, "image/png")})
    first_id = first.json()["task_id"]

    record = db_session.query(TaskRecord).filter(TaskRecord.id == first_id).first()
    record.status = "completed"
    record.result_url = f"/static/processed/{first_id}.jpg"
    db_session.commit()

    files_before = _saved_files(tmp_path)
    client.mock_delay.reset_mock()

    second = client.post("/process/image", files={"file": ("copia.png", png, "image/png")})

    assert second.status_code == 200
    assert second.json()["status"] == "completed"
    client.mock_delay.assert_not_called()
    assert _saved_files(tmp_path) == files_before
    assert not any(name.endswith(".part") for name in _saved_files(tmp_path))


# --- testes unitários, sem o client ---


@pytest.mark.parametrize(
    "header, expected",
    [
        (JPEG_HEADER, "jpg"),
        (b"\x89PNG\r\n\x1a\n" + b"\x00" * 4, "png"),
        (b"RIFF\x24\x00\x00\x00WEBPVP8 ", "webp"),
        (b"isto e texto puro", None),
        (b"", None),
        (b"RIFF\x00", None),
    ],
)
def test_detect_image_type(header, expected):
    assert detect_image_type(header) == expected


def test_open_image_rejects_decompression_bomb_in_warning_range(tmp_path, monkeypatch):
    # 40x40 = 1600 px: entre 1x (1000) e 2x (2000) o limite. Sem o filtro de
    # warning, o Pillow abriria a imagem e só emitiria um aviso.
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1000)
    path = tmp_path / "bomba.png"
    Image.new("RGB", (40, 40)).save(path, format="PNG")

    with pytest.raises(Image.DecompressionBombError):
        _open_image(str(path))


def test_open_image_accepts_image_under_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1000)
    path = tmp_path / "pequena.png"
    Image.new("RGB", (20, 20)).save(path, format="PNG")

    with _open_image(str(path)) as img:
        assert img.size == (20, 20)
