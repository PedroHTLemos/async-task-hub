import uuid

from sqlalchemy import Column, DateTime, Index, Integer, String, Text, text
from sqlalchemy.sql import func

from app.core.database import Base
from app.core.task_status import DEAD_LETTER_OPEN, PENDING


class TaskRecord(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(String, default=PENDING, nullable=False)
    file_hash = Column(String, index=True, nullable=True)
    file_path = Column(String, nullable=True)
    result_url = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index(
            "uq_tasks_file_hash_in_progress",
            "file_hash",
            unique=True,
            postgresql_where=text("status IN ('pending', 'processing', 'retrying')"),
            sqlite_where=text("status IN ('pending', 'processing', 'retrying')"),
        ),
    )


class DeadLetter(Base):
    __tablename__ = "dead_letters"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    task_id = Column(String, index=True, nullable=False)
    file_path = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=False, server_default="0")
    failure_kind = Column(String, nullable=True)
    status = Column(
        String, nullable=False, default=DEAD_LETTER_OPEN, server_default=DEAD_LETTER_OPEN
    )
    reprocessed_at = Column(DateTime(timezone=True), nullable=True)
    reprocessed_task_id = Column(String, nullable=True)
    failed_at = Column(DateTime(timezone=True), server_default=func.now())
