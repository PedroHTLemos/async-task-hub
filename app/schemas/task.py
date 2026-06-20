from pydantic import BaseModel
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
