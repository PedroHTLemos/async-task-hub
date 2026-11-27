"""
Infraestrutura da suíte de integração.

Sobe Postgres e Redis reais em containers (Testcontainers), aplica as
migrations com `alembic upgrade head` e troca as dependências do app em tempo
de execução: engine e SessionLocal do banco, cliente Redis da DLQ e broker do
Celery.

A troca é feita em tempo de execução porque o tests/conftest.py da raiz já
importa app.main, e a configuração é lida na importação. Por isso as trocas
passam por um MonkeyPatch de sessão e são desfeitas no fim da execução.

O worker do Celery roda no próprio processo de teste, com o pool solo. A API
é servida por uvicorn numa thread de fundo, para que requisições concorrentes
cheguem de fato ao mesmo tempo.
"""
import os
import shutil
import socket
import subprocess
import sys
import threading
from pathlib import Path

import httpx
import pytest
import redis
import uvicorn
from celery.contrib.testing.worker import start_worker
from limits.storage import MemoryStorage
from limits.strategies import FixedWindowRateLimiter
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.core import database, dead_letters
from app.core.config import settings
from app.core.database import get_db
from app.core.limiter import limiter
from app.main import app
from app.workers.celery_app import celery_app
from tests.integration.helpers import wait_for

ROOT = Path(__file__).resolve().parents[2]
ADMIN_KEY = "chave-de-integracao"


def pytest_collection_modifyitems(items):
    """Marca como `integration` todo teste desta pasta, para `pytest -m integration` selecioná-los."""
    folder = Path(__file__).parent
    for item in items:
        if folder in Path(item.path).parents:
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def _session_patch():
    """MonkeyPatch de escopo de sessão: as trocas valem para a sessão inteira."""
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="session")
def postgres_container():
    # Import local: a suíte rápida (sem -m integration) não deve carregar o testcontainers.
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def redis_container():
    from testcontainers.redis import RedisContainer

    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture(scope="session")
def redis_url(redis_container):
    # Esta versão do testcontainers não expõe get_connection_url para Redis.
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"


@pytest.fixture(scope="session")
def migrated_pg_url(postgres_container):
    """Cria o schema com as migrations, e não com create_all, para testar também a 0003 (índice parcial)."""
    url = postgres_container.get_connection_url()
    env = {**os.environ, "DATABASE_URL": url}
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"alembic upgrade head falhou:\n{result.stderr}")
    return url


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _empty_dir(folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    for entry in folder.iterdir():
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()


@pytest.fixture(scope="session", autouse=True)
def integration_environment(_session_patch, migrated_pg_url, redis_url, tmp_path_factory):
    """Troca as dependências do app pelas dos containers e mantém o worker do Celery de pé."""
    mp = _session_patch

    engine = create_engine(migrated_pg_url, pool_size=20, max_overflow=10)
    mp.setattr(database, "engine", engine)
    mp.setattr(
        database,
        "SessionLocal",
        sessionmaker(autocommit=False, autoflush=False, bind=engine),
    )

    # Cada requisição recebe uma sessão própria. Os testes de concorrência dependem disso.
    def session_per_request():
        session = database.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    mp.setitem(app.dependency_overrides, get_db, session_per_request)

    mp.setattr(dead_letters, "redis_client", redis.Redis.from_url(redis_url))

    # Antes de qualquer conexão: o broker precisa apontar para o Redis do container.
    # O Settings.broker_url do Celery lê primeiro a variável CELERY_BROKER_URL, que a
    # raiz define como memory://. Por isso trocamos a variável também, além da config.
    mp.setenv("CELERY_BROKER_URL", redis_url)
    mp.setitem(celery_app.conf, "broker_url", redis_url)
    mp.setitem(celery_app.conf, "task_always_eager", False)

    mp.setattr(settings, "upload_dir", str(tmp_path_factory.mktemp("uploads")))
    mp.setattr(settings, "output_dir", str(tmp_path_factory.mktemp("processed")))
    # Rate limit alto para não interferir nos testes de concorrência.
    mp.setattr(settings, "rate_limit_per_minute", 100_000)
    # O limiter fica em memória. Assim, a URL de RATE_LIMIT_STORAGE_URI do ambiente
    # não entra nos testes, e o reset do fixture da raiz não depende de Redis.
    storage = MemoryStorage()
    mp.setattr(limiter, "_storage", storage)
    mp.setattr(limiter, "_limiter", FixedWindowRateLimiter(storage))

    # Pool solo: as tarefas rodam na thread do worker, no mesmo processo. Assim
    # os patches feitos pelos testes (por exemplo em Image.Image.save) valem para
    # o worker. Com prefork, os processos filhos herdariam o estado no momento do
    # fork, e os patches posteriores não chegariam até eles.
    with start_worker(
        celery_app,
        pool="solo",
        perform_ping_check=False,
        shutdown_timeout=10,
    ):
        yield

    engine.dispose()


@pytest.fixture(autouse=True)
def isolated_state(integration_environment):
    """Zera o estado antes de cada teste: tabelas, Redis e diretórios de arquivos."""
    with database.engine.begin() as connection:
        connection.execute(text("TRUNCATE tasks, dead_letters"))
    dead_letters.redis_client.flushdb()
    _empty_dir(Path(settings.upload_dir))
    _empty_dir(Path(settings.output_dir))
    yield


@pytest.fixture(scope="session")
def api_base_url(integration_environment):
    """Sobe a API com uvicorn numa thread de fundo, numa porta livre."""
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, name="uvicorn-integracao", daemon=True)
    thread.start()
    wait_for(lambda: server.started, timeout=15, description="uvicorn iniciar")
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=15)


@pytest.fixture()
def api(api_base_url):
    with httpx.Client(base_url=api_base_url, timeout=30) as client:
        yield client


@pytest.fixture()
def admin_headers(monkeypatch):
    monkeypatch.setattr(settings, "admin_api_key", ADMIN_KEY)
    return {"x-admin-key": ADMIN_KEY}
