from fastapi import FastAPI

app = FastAPI(
    title="AsyncTask Hub",
    description="API assíncrona com fila de tarefas, rate limiting e cache",
    version="0.1.0",
)


@app.get("/health")
def health_check():
    return {"status": "ok"}
