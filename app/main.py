from contextlib import asynccontextmanager

from fastapi import FastAPI
from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler
from fastapi.staticfiles import StaticFiles
from app.core.limiter import limiter
from app.core.database import engine
from app.api import tasks


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    engine.dispose()


app = FastAPI(
    title="AsyncTask Hub",
    description="API assíncrona com fila de tarefas, rate limiting e cache",
    version="0.1.0",
    lifespan=lifespan,
)


app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(tasks.router)


@app.get("/health")
def health_check():
    return {"status": "ok"}
