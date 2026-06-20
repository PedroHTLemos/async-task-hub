import os
import shutil
import uuid
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.models import TaskRecord
from app.schemas.task import TaskCreatedResponse, TaskStatusResponse
from app.workers.tasks import process_image_task
from app.core.limiter import limiter

router = APIRouter()

UPLOAD_DIR = "/code/app/static/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/process/image", response_model=TaskCreatedResponse)
@limiter.limit("10/minute")
def process_image(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)):
    task_id = str(uuid.uuid4())
    file_extension = os.path.splitext(file.filename)[1]
    saved_path = os.path.join(UPLOAD_DIR, f"{task_id}{file_extension}")

    with open(saved_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    task_record = TaskRecord(id=task_id, status="pending")
    db.add(task_record)
    db.commit()

    process_image_task.delay(task_id, saved_path)

    return TaskCreatedResponse(task_id=task_id, status="pending")


@router.get("/tasks/{task_id}", response_model=TaskStatusResponse)
def get_task_status(task_id: str, db: Session = Depends(get_db)):
    task = db.query(TaskRecord).filter(TaskRecord.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return TaskStatusResponse(
        task_id=task.id,
        status=task.status,
        result_url=task.result_url,
        error_message=task.error_message,
        created_at=task.created_at,
    )
