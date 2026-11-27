from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str
    redis_url: str
    celery_broker_url: str
    celery_result_backend: str
    rate_limit_per_minute: int = 10
    rate_limit_storage_uri: str = "memory://"


settings = Settings()
