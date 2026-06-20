from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    redis_url: str
    celery_broker_url: str
    celery_result_backend: str
    rate_limit_per_minute: int = 10

    class Config:
        env_file = ".env"


settings = Settings()
