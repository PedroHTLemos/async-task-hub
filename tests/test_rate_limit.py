"""
Teste do rate limit de POST /process/image.

Prova que o limite vem de settings.rate_limit_per_minute e não de um valor
fixo no decorador.
"""

import io
from unittest.mock import MagicMock

import redis
from PIL import Image

from app.core.config import settings
from app.core.limiter import limiter


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


def test_rate_limit_falls_back_to_memory_when_storage_is_down(client, monkeypatch):
    """Redis fora do ar: o upload continua respondendo, e o limite passa a ser
    contado em memória, em vez de a API responder 500."""
    monkeypatch.setattr(settings, "rate_limit_per_minute", 2)

    broken_storage = MagicMock()
    broken_storage.hit.side_effect = redis.exceptions.ConnectionError("redis fora do ar")
    monkeypatch.setattr(limiter, "_limiter", broken_storage)
    # Storage em memória responde check() como saudável; aqui ele falha também,
    # como um Redis fora do ar, para a sonda de recuperação não desligar o fallback.
    monkeypatch.setattr(limiter._storage, "check", lambda: False)

    try:
        responses = [
            client.post(
                "/process/image",
                files={"file": (f"{color}.jpg", _jpeg_bytes(color), "image/jpeg")},
            )
            for color in ("red", "green", "blue")
        ]

        assert limiter._storage_dead is True  # o fallback de fato entrou
        assert [r.status_code for r in responses] == [200, 200, 429]
    finally:
        limiter._storage_dead = False
        if getattr(limiter, "_fallback_storage", None) is not None:
            limiter._fallback_storage.reset()
