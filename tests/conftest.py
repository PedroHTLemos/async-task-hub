"""
Fixtures compartilhadas pelos testes da API.

Usamos SQLite em memória (isolado por teste) no lugar do Postgres real, e
mockamos o `.delay()` do Celery — assim a suíte roda rápido, sem depender de
Docker, Postgres ou Redis estarem de pé.
"""
import io
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from PIL import Image
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import settings
from app.core.database import Base, get_db
from app.core.limiter import limiter
from app.main import app


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """Zera a contagem do rate limit antes de cada teste, para que uploads de
    um teste não sejam contados no seguinte."""
    limiter.reset()
    yield


@pytest.fixture()
def db_session():
    """Banco SQLite em memória, recriado do zero a cada teste."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    Base.metadata.create_all(bind=engine)

    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def client(db_session, monkeypatch, tmp_path):
    """TestClient da API com o banco substituído pelo SQLite de teste e o
    Celery mockado, pra nenhum teste de API depender de infraestrutura real."""

    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    # Redireciona o diretório de upload para uma pasta temporária só do
    # teste, em vez de gravar arquivos de verdade dentro do repositório.
    monkeypatch.setattr(settings, "upload_dir", str(tmp_path))

    mock_delay = MagicMock()
    monkeypatch.setattr("app.api.tasks.process_image_task.delay", mock_delay)

    with TestClient(app) as test_client:
        test_client.mock_delay = mock_delay  # type: ignore[attr-defined]
        yield test_client

    app.dependency_overrides.clear()


@pytest.fixture()
def sample_image_bytes():
    """Gera uma imagem JPEG válida em memória (não precisa de arquivo no disco)."""
    buffer = io.BytesIO()
    Image.new("RGB", (100, 100), color="blue").save(buffer, format="JPEG")
    buffer.seek(0)
    return buffer.read()