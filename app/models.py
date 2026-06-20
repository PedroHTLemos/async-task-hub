import uuid
from sqlalchemy import Column, String, DateTime, Text
from sqlalchemy.sql import func
from app.core.database import Base


class TaskRecord(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(String, default="pending", nullable=False)
    file_hash = Column(String, index=True, nullable=True)
    result_url = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class DeadLetter(Base):
    __tablename__ = "dead_letters"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    task_id = Column(String, index=True, nullable=False)
    file_path = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(String, nullable=True)
    failed_at = Column(DateTime(timezone=True), server_default=func.now())