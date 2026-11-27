from datetime import datetime

from pydantic import BaseModel, ConfigDict


class TaskCreatedResponse(BaseModel):
    task_id: str
    status: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result_url: str | None = None
    error_message: str | None = None
    created_at: datetime


class DeadLetterResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    task_id: str
    file_path: str | None = None
    error_message: str | None = None
    retry_count: int
    failure_kind: str | None = None
    status: str
    failed_at: datetime | None = None
    reprocessed_at: datetime | None = None
    reprocessed_task_id: str | None = None


class ReprocessResponse(BaseModel):
    dead_letter_id: str
    task_id: str
    status: str
