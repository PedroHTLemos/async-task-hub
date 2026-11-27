from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str
    redis_url: str
    celery_broker_url: str
    celery_result_backend: str
    rate_limit_per_minute: int = 10
    rate_limit_storage_uri: str = "memory://"
    max_upload_mb: int = 10
    max_image_pixels: int = 25_000_000
    task_max_retries: int = 3
    retry_backoff_base_seconds: float = 2.0
    retry_backoff_max_seconds: float = 60.0
    retry_backoff_jitter_seconds: float = 1.0
    stale_task_timeout_seconds: int = 600
    sweep_interval_seconds: int = 60
    part_file_max_age_seconds: int = 3600
    redis_socket_timeout_seconds: float = 2.0
    upload_dir: str = "app/static/uploads"
    output_dir: str = "app/static/processed"
    admin_api_key: str = ""
    broker_publish_max_retries: int = 1

    @model_validator(mode="after")
    def _stale_timeout_must_exceed_max_backoff(self):
        # Sem essa folga, a rotina marcaria como travada uma tarefa que só está
        # esperando o backoff de um retry legítimo.
        largest_wait = self.retry_backoff_max_seconds + self.retry_backoff_jitter_seconds
        if self.stale_task_timeout_seconds <= largest_wait:
            raise ValueError(
                f"stale_task_timeout_seconds ({self.stale_task_timeout_seconds}) deve ser "
                f"maior que retry_backoff_max_seconds + retry_backoff_jitter_seconds ({largest_wait})"
            )
        return self


settings = Settings()
