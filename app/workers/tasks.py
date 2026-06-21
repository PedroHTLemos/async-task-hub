import json
import os
import redis
from PIL import Image, UnidentifiedImageError
from app.workers.celery_app import celery_app
from app.core.database import SessionLocal
from app.core.config import settings
from app.models import TaskRecord, DeadLetter

OUTPUT_DIR = "app/static/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DLQ_REDIS_KEY = "dlq:failed_tasks"

redis_client = redis.from_url(settings.redis_url)

PERMANENT_FAILURE_EXCEPTIONS = (UnidentifiedImageError, FileNotFoundError, OSError)


def _send_to_dlq(db, task_id: str, file_path: str, error_message: str, retry_count: int):
    """Persist the failure for auditing (Postgres) and push it onto a
    dedicated Redis queue, simulating a real message-broker Dead Letter Queue
    that another consumer could drain and reprocess later."""
    dead_letter = DeadLetter(
        task_id=task_id,
        file_path=file_path,
        error_message=error_message,
        retry_count=str(retry_count),
    )
    db.add(dead_letter)
    db.commit()

    redis_client.lpush(
        DLQ_REDIS_KEY,
        json.dumps({"task_id": task_id, "file_path": file_path, "error": error_message}),
    )


@celery_app.task(name="process_image_task", bind=True, max_retries=3, default_retry_delay=10)
def process_image_task(self, task_id: str, file_path: str):
    db = SessionLocal()
    try:
        task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
        if not task:
            return {"status": "failed", "error": "task not found"}

        task.status = "processing"
        db.commit()

        output_filename = f"{task_id}.jpg"
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        with Image.open(file_path) as img:
            img.load()  # forces decode now, so corrupted files raise here, not lazily later
            img = img.convert("RGB")
            img.thumbnail((800, 800))
            img.save(output_path, "JPEG", quality=85)

        task.status = "completed"
        task.result_url = f"/static/processed/{output_filename}"
        db.commit()

        return {"status": "completed", "result_url": task.result_url}

    except PERMANENT_FAILURE_EXCEPTIONS as exc:
        # Unrecoverable: don't waste retries, move straight to the DLQ.
        task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
        if task:
            task.status = "failed"
            task.error_message = str(exc)
            db.commit()

        _send_to_dlq(db, task_id, file_path, str(exc), retry_count=self.request.retries)
        return {"status": "failed", "error": str(exc), "dead_lettered": True}

    except Exception as exc:
        # Transient/unknown failure: retry a few times before giving up.
        if self.request.retries >= self.max_retries:
            task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
            if task:
                task.status = "failed"
                task.error_message = str(exc)
                db.commit()

            _send_to_dlq(db, task_id, file_path, str(exc), retry_count=self.request.retries)
            return {"status": "failed", "error": str(exc), "dead_lettered": True}

        task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
        if task:
            task.status = "retrying"
            task.error_message = str(exc)
            db.commit()

        raise self.retry(exc=exc)

    finally:
        db.close()