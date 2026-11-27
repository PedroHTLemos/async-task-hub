def detect_image_type(header: bytes) -> str | None:
    """Identifica o tipo da imagem pelos magic bytes do início do arquivo.

    Devolve "jpg", "png", "webp" ou None para qualquer outra coisa.
    """
    if header.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if len(header) >= 12 and header[0:4] == b"RIFF" and header[8:12] == b"WEBP":
        return "webp"
    return None
