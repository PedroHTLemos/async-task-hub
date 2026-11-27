"""
Timeouts curtos nas conexões com o Redis e com o broker.

Sem eles, uma API com o Redis parado levava ~90 s para responder 503. Estes
testes impedem que a configuração seja removida sem perceber.
"""

from app.core import dead_letters
from app.core.config import settings
from app.core.limiter import limiter
from app.workers.celery_app import celery_app
from app.workers.tasks import process_image_task


def test_dead_letter_redis_client_has_socket_timeouts():
    kwargs = dead_letters.redis_client.connection_pool.connection_kwargs

    assert kwargs["socket_connect_timeout"] == settings.redis_socket_timeout_seconds
    assert kwargs["socket_timeout"] == settings.redis_socket_timeout_seconds


def test_limiter_storage_has_socket_timeouts():
    assert (
        limiter._storage_options["socket_connect_timeout"] == settings.redis_socket_timeout_seconds
    )
    assert limiter._storage_options["socket_timeout"] == settings.redis_socket_timeout_seconds


def test_celery_broker_publish_fails_fast():
    conf = celery_app.conf

    assert conf.broker_connection_timeout == settings.redis_socket_timeout_seconds
    assert (
        conf.broker_transport_options["socket_connect_timeout"]
        == settings.redis_socket_timeout_seconds
    )
    assert conf.broker_transport_options["socket_timeout"] == settings.redis_socket_timeout_seconds
    assert conf.task_publish_retry_policy["max_retries"] == settings.broker_publish_max_retries
    assert conf.broker_connection_retry_on_startup is True


def test_process_image_task_does_not_wait_on_result_backend():
    # Sem isso, o publish consulta o backend Redis e pode levar ~90 s.
    assert process_image_task.ignore_result is True
