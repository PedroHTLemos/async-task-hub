import random


def compute_backoff(
    retries: int,
    base: float,
    maximum: float,
    jitter: float,
    rng: random.Random | None = None,
) -> float:
    """min(base * 2**retries, maximum) + uniform(0, jitter)

    O backoff é calculado à mão porque o `retry_backoff` nativo do Celery só
    funciona com `autoretry_for`, que não combina com a lógica própria de DLQ
    do worker. O jitter espalha as novas tentativas no tempo, para que tarefas
    que falharam juntas não voltem todas no mesmo instante.

    `rng` existe para os testes poderem ser determinísticos.
    """
    delay = min(base * (2**retries), maximum)
    draw = rng.random() if rng is not None else random.random()
    return delay + draw * jitter
