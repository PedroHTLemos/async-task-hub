from celery import Celery
from app.core.config import settings

celery_app = Celery(
    "async_task_hub",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.workers.tasks", "app.workers.maintenance"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,
    task_acks_late=True,
    # Falha rápida quando o broker está fora, em vez de esperar ~90 s.
    broker_connection_timeout=settings.redis_socket_timeout_seconds,
    broker_transport_options={
        "socket_connect_timeout": settings.redis_socket_timeout_seconds,
        "socket_timeout": settings.redis_socket_timeout_seconds,
    },
    task_publish_retry_policy={
        "max_retries": settings.broker_publish_max_retries,
        "interval_start": 0,
        "interval_step": 0.2,
        "interval_max": 0.2,
    },
    broker_connection_retry_on_startup=True,
    beat_schedule={
        "sweep-stale-tasks": {
            "task": "sweep_stale_tasks",
            "schedule": settings.sweep_interval_seconds,
        },
    },
)
