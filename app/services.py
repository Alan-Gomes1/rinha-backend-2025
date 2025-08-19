import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from http import HTTPStatus

import httpx
import orjson

from .settings import redis_client, settings

client = httpx.AsyncClient()


async def send_payment(
    correlation_id: str, amount: str, requested_at: datetime,
    timeout: httpx.Timeout
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
    data = {
        'correlationId': correlation_id,
        'amount': amount,
        'requestedAt': requested_at.isoformat(),
    }
    try:
        response = await client.post(url, json=data, timeout=timeout)
        if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            await redis_client.set('failing', 'true')
        response.raise_for_status()
        return True
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        print(f'Error sending payment: {e}. Adding to fallback queue.')
        return False


async def save_payment(
    correlation_id: str, amount: str, requested_at: datetime
) -> None:
    """Salva o pagamento processado.

    Args:
        payment (dict): O dicionário contendo os dados do pagamento.
        requested_at (datetime): Data e hora da requisição do pagamento.
        is_fallback (bool, optional): Se o pagamento é de fallback.
    """
    if isinstance(requested_at, datetime):
        timestamp = requested_at.timestamp()
    else:
        timestamp = float(requested_at)

    data = {
        'correlationId': correlation_id,
        'amount': amount,
        'requested_at': timestamp
    }
    payment_json = orjson.dumps(data).decode()
    await redis_client.zadd(settings.PAYMENT_ZKEY, {payment_json: timestamp})


async def purge_payments() -> None:
    """Remove todos os pagamentos registrados no Redis."""
    await redis_client.delete(
        settings.PAYMENT_ZKEY, settings.PAYMENT_FALLBACK_ZKEY
    )


async def get_summary(
    _from: datetime | None = None, to: datetime | None = None
) -> dict:
    """Gera um resumo dos pagamentos processados dentro de um período de tempo.

    Args:
        _from (datetime | None, optional): Data de início do período.
            Se None, considera desde o início dos registros. Defaults to None.
        to (datetime | None, optional): Data de fim do período.
            Se None, considera até o final dos registros. Defaults to None.
    Returns:
        dict: Um dicionário contendo o resumo dos pagamentos, incluindo o total
        de requisições e o valor total para pagamentos padrão e de fallback.
    """
    min_score = _from.timestamp() if _from else '-inf'
    max_score = to.timestamp() if to else '+inf'

    payments = await redis_client.zrangebyscore(
        settings.PAYMENT_ZKEY, min_score, max_score
    )
    total_requests = len(payments)
    amount = Decimal(0)

    for payment_json in payments:
        payment = orjson.loads(payment_json)
        amount += Decimal(payment['amount'])

    summary = {
        'default': {'totalRequests': total_requests, 'totalAmount': amount},
        'fallback': {'totalRequests': 0, 'totalAmount': 0},
    }
    return summary


async def payment_processor(data: bytes) -> bool:
    """Processa um pagamento recebido da fila e salva no banco em memória.

    Args:
        data (bytes): Os dados do pagamento em formato bytes.
    """
    payment = orjson.loads(data)
    amount = str(payment.get('amount'))
    correlation_id = payment.get('correlationId')

    if await redis_client.get('failing') == b'true':
        return False

    request_at = datetime.now(tz=timezone.utc)
    timeout = httpx.Timeout(settings.TIMEOUT, connect=settings.CONNECT_TIMEOUT)
    if await send_payment(correlation_id, amount, request_at, timeout):
        await save_payment(correlation_id, amount, request_at)
        return True
    return False


async def health_check() -> None:
    """Define o tempo limite de timeout para requisitar o serviço de pagamento.

    Returns:
        int: valor do timeout
    """
    try:
        request = await client.get(
            f'{settings.PAYMENT_PROCESSOR_URL}/service-health'
        )
        response = request.json()
        failing = str(response.get('failing'))
        await redis_client.set('failing', failing)
    except Exception as ex:
        print(f'Error checking health: {ex}')


async def health_check_loop() -> None:
    """Loop de verificação de saúde do serviço de pagamento."""
    while True:
        try:
            await health_check()
        except Exception as ex:
            print(f'Health check failed: {ex}')
        await asyncio.sleep(5)
