import json

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.config import settings

UPLOAD_PATH = "/process/image"
MULTIPART_OVERHEAD_BYTES = 64 * 1024


class _BodyTooLarge(BaseException):
    # BaseException, e não Exception: o FastAPI converte qualquer Exception
    # levantada ao ler o corpo em 400. Assim ela chega até este middleware.
    pass


async def _send_too_large(send: Send, max_mb: int) -> None:
    body = json.dumps({"detail": f"File too large. Max size is {max_mb} MB"}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class BodyLimitMiddleware:
    """Rejeita com 413 corpos de POST /process/image acima do limite, antes
    que o Starlette os grave em disco ou em memória.

    É ASGI puro (sem BaseHTTPMiddleware). O limite é lido a cada requisição.
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] != "POST" or scope["path"] != UPLOAD_PATH:
            await self.app(scope, receive, send)
            return

        max_mb = settings.max_upload_mb
        limit = max_mb * 1024 * 1024 + MULTIPART_OVERHEAD_BYTES

        # Primeira checagem: o header Content-Length, quando existe.
        for name, value in scope["headers"]:
            if name == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    declared = 0
                if declared > limit:
                    await _send_too_large(send, max_mb)
                    return
                break

        # Segunda checagem: conta os bytes que realmente chegam. Cobre o caso
        # sem Content-Length (chunked) e o de um header que mente.
        received = 0
        response_started = False

        async def counting_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    raise _BodyTooLarge()
            return message

        async def tracking_send(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, counting_receive, tracking_send)
        except _BodyTooLarge:
            if response_started:
                raise
            await _send_too_large(send, max_mb)
