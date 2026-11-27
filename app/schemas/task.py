from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime


class TaskCreatedResponse(BaseModel):
    task_id: str
    status: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result_url: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime


class DeadLetterResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    task_id: str
    file_path: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int
    failure_kind: Optional[str] = None
    status: str
    failed_at: Optional[datetime] = None
    reprocessed_at: Optional[datetime] = None
    reprocessed_task_id: Optional[str] = None


class ReprocessResponse(BaseModel):
    dead_letter_id: str
    task_id: str
    status: str
