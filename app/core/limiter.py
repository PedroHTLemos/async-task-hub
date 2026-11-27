from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings

limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=settings.rate_limit_storage_uri,
    in_memory_fallback_enabled=True,
    storage_options={
        "socket_connect_timeout": settings.redis_socket_timeout_seconds,
        "socket_timeout": settings.redis_socket_timeout_seconds,
    },
)
