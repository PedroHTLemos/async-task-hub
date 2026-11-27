"""
Teste do rate limit de POST /process/image.

Prova que o limite vem de settings.rate_limit_per_minute e não de um valor
fixo no decorador.
"""
import io

from PIL import Image

from app.core.config import settings


def _jpeg_bytes(color: str) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (50, 50), color=color).save(buffer, format="JPEG")
    return buffer.getvalue()


def test_rate_limit_reads_value_from_settings(client, monkeypatch):
    monkeypatch.setattr(settings, "rate_limit_per_minute", 2)

    responses = [
        client.post(
            "/process/image",
            files={"file": (f"{color}.jpg", _jpeg_bytes(color), "image/jpeg")},
        )
        for color in ("red", "green", "blue")
    ]

    assert [r.status_code for r in responses] == [200, 200, 429]
