import time
from app.workers.celery_app import celery_app


@celery_app.task(name="process_image_task", bind=True, max_retries=3)
def process_image_task(self, file_path: str):
    """
    Task placeholder. Vamos substituir pela lógica real de
    processamento de imagem nas próximas etapas.
    """
    time.sleep(2)
    return {"status": "completed", "file_path": file_path}
