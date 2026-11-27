"""
Testes do worker: classificação de falhas por fase, DLQ consistente,
rollback antes de tocar no banco no tratamento de erro, e backoff
exponencial com jitter.

Como em test_dlq_transient_retry.py, o Celery roda em modo eager e o banco e
o Redis são mocks. Os arquivos de imagem são reais, gravados em tmp_path.
"""
import errno
import io
import random
from unittest.mock import MagicMock, patch

import pytest
import redis
from PIL import Image, UnidentifiedImageError
from sqlalchemy.exc import OperationalError

from app.core.config import settings
from app.core import dead_letters
from app.workers import tasks as worker_tasks
from app.workers.backoff import compute_backoff
from app.workers.celery_app import celery_app
from app.workers.errors import PermanentTaskError, classify_decode_error


@pytest.fixture(autouse=True)
def eager_mode():
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = False
    yield
    celery_app.conf.task_always_eager = False


@pytest.fixture
def fake_task_record():
    record = MagicMock()
    record.id = "fake-task-id"
    return record


@pytest.fixture
def fake_db(fake_task_record):
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = fake_task_record
    return db


@pytest.fixture
def mock_redis():
    with patch.object(dead_letters, "redis_client") as client:
        yield client


@pytest.fixture
def output_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(worker_tasks, "OUTPUT_DIR", str(tmp_path / "out"))
    (tmp_path / "out").mkdir()
    return tmp_path / "out"


def _run(fake_db, path):
    with patch.object(worker_tasks, "SessionLocal", return_value=fake_db):
        return worker_tasks.process_image_task.apply(args=("fake-task-id", str(path)))


def _real_jpeg_bytes(size=(300, 300)) -> bytes:
    """JPEG com ruído, para que o arquivo seja grande o bastante para ser cortado."""
    img = Image.new("RGB", size)
    pixels = img.load()
    rng = random.Random(0)
    for x in range(size[0]):
        for y in range(0, size[1], 3):
            pixels[x, y] = (rng.randrange(256), rng.randrange(256), rng.randrange(256))
    buffer = io.BytesIO()
    img.save(buffer, format="JPEG", quality=95)
    return buffer.getvalue()


def _write(path, data: bytes):
    path.write_bytes(data)
    return path


def test_truncated_jpeg_goes_straight_to_dlq(tmp_path, fake_db, fake_task_record, mock_redis, output_dir):
    data = _real_jpeg_bytes()
    path = _write(tmp_path / "cortada.jpg", data[: len(data) // 2])

    with patch.object(worker_tasks.process_image_task, "retry") as mock_retry:
        result = _run(fake_db, path)

    mock_retry.assert_not_called()
    output = result.result
    assert output["dead_lettered"] is True
    dead_letter = fake_db.add.call_args.args[0]
    assert dead_letter.failure_kind == "permanent"
    assert dead_letter.retry_count == 0


def test_text_file_named_jpg_is_permanent(tmp_path, fake_db, fake_task_record, mock_redis, output_dir):
    path = _write(tmp_path / "texto.jpg", b"isto nao e uma imagem nenhuma" * 10)

    _run(fake_db, path)

    assert fake_db.add.call_args.args[0].failure_kind == "permanent"
    assert fake_db.add.call_args.args[0].retry_count == 0


def test_missing_file_is_permanent(tmp_path, fake_db, fake_task_record, mock_redis, output_dir):
    _run(fake_db, tmp_path / "nao_existe.jpg")

    dead_letter = fake_db.add.call_args.args[0]
    assert dead_letter.failure_kind == "permanent"
    assert dead_letter.retry_count == 0


def test_decompression_bomb_is_permanent(tmp_path, fake_db, fake_task_record, mock_redis, output_dir, monkeypatch):
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1000)
    path = tmp_path / "bomba.png"
    Image.new("RGB", (40, 40)).save(path, format="PNG")

    _run(fake_db, path)

    dead_letter = fake_db.add.call_args.args[0]
    assert dead_letter.failure_kind == "permanent"
    assert dead_letter.retry_count == 0


def test_eio_on_save_is_retried_until_dead_letter(tmp_path, fake_db, fake_task_record, mock_redis, output_dir, monkeypatch):
    path = _write(tmp_path / "ok.jpg", _real_jpeg_bytes())
    monkeypatch.setattr(Image.Image, "save", MagicMock(side_effect=OSError(errno.EIO, "disco com erro")))

    with patch.object(worker_tasks, "compute_backoff", return_value=0.0) as mock_backoff:
        result = _run(fake_db, path)

    assert result.result["dead_lettered"] is True
    assert mock_backoff.call_count == settings.task_max_retries
    dead_letter = fake_db.add.call_args.args[0]
    assert dead_letter.failure_kind == "transient"
    assert dead_letter.retry_count == settings.task_max_retries


def test_eio_on_open_is_transient_because_of_errno(tmp_path, fake_db, fake_task_record, mock_redis, output_dir):
    # Mesmo sendo OSError, errno preenchido significa erro de I/O passageiro.
    path = _write(tmp_path / "ok.jpg", _real_jpeg_bytes())

    with patch.object(worker_tasks.Image, "open", side_effect=OSError(errno.EIO, "falha de leitura")), \
         patch.object(worker_tasks, "compute_backoff", return_value=0.0) as mock_backoff:
        _run(fake_db, path)

    assert mock_backoff.call_count == settings.task_max_retries
    dead_letter = fake_db.add.call_args.args[0]
    assert dead_letter.failure_kind == "transient"
    assert dead_letter.retry_count == settings.task_max_retries


def test_retry_countdown_comes_from_compute_backoff(tmp_path, fake_db, fake_task_record, mock_redis, output_dir):
    path = _write(tmp_path / "ok.jpg", _real_jpeg_bytes())

    with patch.object(worker_tasks.Image, "open", side_effect=OSError(errno.EIO, "falha")), \
         patch.object(worker_tasks, "compute_backoff", return_value=0.0) as mock_backoff:
        _run(fake_db, path)

    retries_seen = [c.args[0] for c in mock_backoff.call_args_list]
    assert retries_seen == [0, 1, 2]
    for c in mock_backoff.call_args_list:
        assert c.args[1:] == (
            settings.retry_backoff_base_seconds,
            settings.retry_backoff_max_seconds,
            settings.retry_backoff_jitter_seconds,
        )


def test_redis_failure_does_not_break_dead_letter(tmp_path, fake_db, fake_task_record, mock_redis, output_dir):
    path = _write(tmp_path / "cortada.jpg", _real_jpeg_bytes()[:500])
    mock_redis.lpush.side_effect = redis.exceptions.ConnectionError("redis fora do ar")

    result = _run(fake_db, path)

    assert result.successful()
    assert result.result["dead_lettered"] is True
    fake_db.commit.assert_called()  # a DLQ no Postgres aconteceu antes do lpush
    assert fake_db.add.call_count == 1


def test_db_error_rolls_back_before_any_query_in_except(tmp_path, fake_db, fake_task_record, mock_redis, output_dir):
    path = _write(tmp_path / "ok.jpg", _real_jpeg_bytes())
    # Só o primeiro commit (status "processing") falha; os demais funcionam.
    fake_db.commit.side_effect = [
        OperationalError("SELECT 1", {}, Exception("conexão perdida")),
        None,
        None,
        None,
    ]

    with patch.object(worker_tasks, "compute_backoff", return_value=0.0):
        _run(fake_db, path)

    names = [str(c[0]) for c in fake_db.mock_calls]
    first_commit = names.index("commit")
    rollback = names.index("rollback")
    next_query = next(i for i, n in enumerate(names) if n.startswith("query") and i > first_commit)
    assert first_commit < rollback < next_query


# --- testes unitários ---


@pytest.mark.parametrize(
    "exc, expected",
    [
        (UnidentifiedImageError("não é imagem"), True),
        (Image.DecompressionBombError("bomba"), True),
        (FileNotFoundError(errno.ENOENT, "sumiu"), True),
        (OSError(None, "truncated"), True),
        (SyntaxError("formato malformado"), True),
        (ValueError("valor inválido"), True),
        (OSError(errno.EIO, "falha de disco"), False),
        (OSError(errno.ENOSPC, "disco cheio"), False),
        (RuntimeError("outra coisa"), False),
    ],
)
def test_classify_decode_error(exc, expected):
    assert classify_decode_error(exc) is expected


def test_permanent_task_error_is_an_exception():
    assert issubclass(PermanentTaskError, Exception)


def test_compute_backoff_grows_with_attempts():
    rng = random.Random(0)
    values = [compute_backoff(r, base=2.0, maximum=60.0, jitter=1.0, rng=rng) for r in range(5)]
    assert values == sorted(values)
    assert values[0] < values[1] < values[2] < values[3]


def test_compute_backoff_exact_values_without_jitter():
    values = [compute_backoff(r, base=2.0, maximum=60.0, jitter=0.0) for r in range(7)]
    assert values == [2, 4, 8, 16, 32, 60, 60]


def test_compute_backoff_jitter_stays_in_range():
    rng = random.Random(42)
    for r in range(6):
        for _ in range(200):
            delay = compute_backoff(r, base=2.0, maximum=60.0, jitter=1.0, rng=rng)
            base_delay = min(2.0 * 2**r, 60.0)
            assert base_delay <= delay < base_delay + 1.0
