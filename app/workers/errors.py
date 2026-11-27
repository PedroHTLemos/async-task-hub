from PIL import Image, UnidentifiedImageError


class PermanentTaskError(Exception):
    """Falha que não adianta repetir: o arquivo em si está inválido."""


def classify_decode_error(exc: BaseException) -> bool:
    """Diz se um erro da fase de decodificação é permanente.

    Pillow lança OSError com errno None para imagem truncada ou corrompida.
    Erros reais de I/O do sistema operacional trazem errno preenchido e são
    passageiros. Esta função só deve ser chamada para erros de decodificação.
    """
    if isinstance(
        exc,
        (
            UnidentifiedImageError,
            Image.DecompressionBombError,
            FileNotFoundError,
            SyntaxError,
            ValueError,
        ),
    ):
        return True
    if isinstance(exc, OSError) and exc.errno is None:
        return True
    return False
