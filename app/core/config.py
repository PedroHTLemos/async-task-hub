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


settings = Settings()
