import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api import tasks
from app.core import database
from app.core.body_limit import BodyLimitMiddleware
from app.core.config import settings
from app.core.limiter import limiter


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(settings.upload_dir, exist_ok=True)
    yield
    database.engine.dispose()


app = FastAPI(
    title="AsyncTask Hub",
    description="API assíncrona com fila de tarefas, rate limiting e cache",
    version="0.1.0",
    lifespan=lifespan,
)


app.add_middleware(BodyLimitMiddleware)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(tasks.router)


@app.get("/health")
def health_check():
    return {"status": "ok"}
