from datetime import datetime
from http import HTTPStatus

import httpx
import orjson

from .settings import redis_client, settings

client = httpx.AsyncClient()


async def send_payment(
    payment: dict, requested_at: datetime, timeout: httpx.Timeout
) -> bool:
    """Envia um pagamento para o processador de pagamentos externo (default).

    Args:
        payment (dict): O dicionário contendo os dados do pagamento.
        requested_at (datetime): Data e hora do pagamento.
        timeout (httpx.Timeout): O objeto de timeout para a requisição HTTP.

    Returns:
        bool: Se o pagamento foi processado com sucesso.
    """
    url = settings.PAYMENT_PROCESSOR_URL
    data = {**payment, 'requestedAt': requested_at.isoformat()}
    response = await client.post(url, json=data, timeout=timeout)
    if response.status_code == HTTPStatus.OK:
        return True
    return False


async def save_payment(payment: dict, requested_at: datetime) -> None:
    """Salva o pagamento processado.

    Args:
        payment (dict): O dicionário contendo os dados do pagamento.
        requested_at (datetime): Data e hora da requisição do pagamento.
    """
    if isinstance(requested_at, datetime):
        timestamp = requested_at.timestamp()
    else:
        timestamp = float(requested_at)

    data = {**payment, 'requested_at': timestamp}
    payment_json = orjson.dumps(data).decode()
    await redis_client.zadd(settings.PAYMENT_KEY, {payment_json: timestamp})
