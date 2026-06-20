import os
from PIL import Image
from app.workers.celery_app import celery_app
from app.core.database import SessionLocal
from app.models import TaskRecord

OUTPUT_DIR = "/code/app/static/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)


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
            img = img.convert("RGB")
            img.thumbnail((800, 800))
            img.save(output_path, "JPEG", quality=85)

        task.status = "completed"
        task.result_url = f"/static/processed/{output_filename}"
        db.commit()

        return {"status": "completed", "result_url": task.result_url}

    except Exception as exc:
        task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
        if task:
            task.status = "failed"
            task.error_message = str(exc)
            db.commit()
        raise self.retry(exc=exc)

    finally:
        db.close()
