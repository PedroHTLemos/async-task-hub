"""
Rotina de tarefas travadas e de .part órfãos (app/workers/maintenance.py),
e validação da relação entre stale_task_timeout e o backoff.
"""
import os
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from sqlalchemy.orm import sessionmaker

from app.core import dead_letters
from app.core.config import Settings, settings
from app.models import DeadLetter, TaskRecord
from app.workers import maintenance


@pytest.fixture
def sweep(db_session, tmp_path, monkeypatch):
    monkeypatch.setattr(maintenance, "SessionLocal", sessionmaker(bind=db_session.get_bind()))
    monkeypatch.setattr(settings, "upload_dir", str(tmp_path))
    monkeypatch.setattr(settings, "stale_task_timeout_seconds", 600)
    monkeypatch.setattr(settings, "part_file_max_age_seconds", 3600)
    with patch.object(dead_letters, "redis_client"):
        yield maintenance.sweep_stale_tasks


def _add_task(db, task_id, status, created_at, updated_at=None, file_path=None):
    db.add(
        TaskRecord(
            id=task_id,
            status=status,
            file_hash=f"hash-{task_id}",
            file_path=file_path,
            created_at=created_at,
            updated_at=updated_at,
        )
    )
    db.commit()


def _ago(seconds):
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def test_stale_pending_task_is_failed_and_dead_lettered(db_session, sweep):
    _add_task(db_session, "travada", "pending", created_at=_ago(7200), file_path="/x/travada.png")

    sweep.apply()

    db_session.expire_all()
    assert db_session.get(TaskRecord, "travada").status == "failed"
    dead_letter = db_session.query(DeadLetter).one()
    assert dead_letter.task_id == "travada"
    assert dead_letter.failure_kind == "transient"
    assert dead_letter.file_path == "/x/travada.png"
    assert dead_letter.error_message.startswith("stale: no progress for 600s")


def test_stale_retrying_task_is_swept_by_updated_at(db_session, sweep):
    _add_task(db_session, "retentando", "retrying", created_at=_ago(7200), updated_at=_ago(7200))

    sweep.apply()

    db_session.expire_all()
    assert db_session.get(TaskRecord, "retentando").status == "failed"


def test_recent_pending_task_is_not_touched(db_session, sweep):
    _add_task(db_session, "recente", "pending", created_at=_ago(10))

    sweep.apply()

    db_session.expire_all()
    assert db_session.get(TaskRecord, "recente").status == "pending"
    assert db_session.query(DeadLetter).count() == 0


def test_old_completed_task_is_not_touched(db_session, sweep):
    _add_task(db_session, "antiga-ok", "completed", created_at=_ago(7200))

    sweep.apply()

    db_session.expire_all()
    assert db_session.get(TaskRecord, "antiga-ok").status == "completed"


def test_old_part_files_are_removed_and_recent_kept(tmp_path, db_session, sweep):
    old_part = tmp_path / "antigo.part"
    recent_part = tmp_path / "recente.part"
    other = tmp_path / "foto.jpg"
    for path in (old_part, recent_part, other):
        path.write_bytes(b"x")
    two_hours_ago = time.time() - 7200
    os.utime(old_part, (two_hours_ago, two_hours_ago))
    os.utime(other, (two_hours_ago, two_hours_ago))

    sweep.apply()

    assert not old_part.exists()
    assert recent_part.exists()
    assert other.exists()  # só .part é candidato


def test_sweep_returns_counts(tmp_path, db_session, sweep):
    _add_task(db_session, "travada-1", "pending", created_at=_ago(7200))
    _add_task(db_session, "travada-2", "retrying", created_at=_ago(7200), updated_at=_ago(7200))
    _add_task(db_session, "recente", "pending", created_at=_ago(10))
    _add_task(db_session, "ok", "completed", created_at=_ago(7200))
    old_part = tmp_path / "antigo.part"
    old_part.write_bytes(b"x")
    two_hours_ago = time.time() - 7200
    os.utime(old_part, (two_hours_ago, two_hours_ago))

    result = sweep.apply().result

    assert result == {"stale_failed": 2, "part_files_removed": 1}


def test_stale_timeout_must_exceed_max_backoff():
    with pytest.raises(ValidationError):
        Settings(
            database_url="sqlite://",
            redis_url="redis://localhost:6379/0",
            celery_broker_url="memory://",
            celery_result_backend="cache+memory://",
            stale_task_timeout_seconds=30,
            retry_backoff_max_seconds=60,
        )


def test_default_settings_satisfy_the_stale_timeout_rule():
    valid = Settings(
        database_url="sqlite://",
        redis_url="redis://localhost:6379/0",
        celery_broker_url="memory://",
        celery_result_backend="cache+memory://",
    )
    assert valid.stale_task_timeout_seconds > valid.retry_backoff_max_seconds + valid.retry_backoff_jitter_seconds
