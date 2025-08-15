import asyncio
from datetime import datetime, timezone

import httpx
import orjson

from .settings import redis_client, settings


async def add_payment(correlation_id: str, amount: float) -> None:
    """Adiciona os pagamentos na fila

    Args:
        correlation_id (str): identificador da transação
        amount (float): valor da transação
    """
    data = {'correlationId': correlation_id, 'amount': amount}
    payment = orjson.dumps(data).decode()
    await redis_client.lpush(settings.REDIS_QUEUE, payment)


async def add_payment_to_fallback_queue(payment: dict) -> None:
    """Adiciona um pagamento que falhou na fila de fallback.

    Args:
        payment (dict): O pagamento a ser adicionado na fila.
    """
    payment_json = orjson.dumps(payment).decode()
    await redis_client.lpush(settings.REDIS_FALLBACK_QUEUE, payment_json)


async def retry_payment(
    payment: dict, timeout: httpx.Timeout, send_payment: callable,
    save_payment: callable
) -> None:
    """Tenta reenviar um pagamento que falhou em até 10 tentativas,
    com um intervalo de 5 segundos entre cada tentativa.

    Args:
        payment (dict): O dicionário contendo os dados do pagamento.
        timeout (httpx.Timeout): O objeto de timeout para a requisição HTTP.
        send_payment (callable): A função para enviar o pagamento.
        save_payment (callable): A função para salvar o pagamento.
    """
    for _ in range(10):
        await asyncio.sleep(5)
        request_at = datetime.now(tz=timezone.utc)
        if await send_payment(payment, request_at, timeout):
            await save_payment(payment, request_at)
            break
    else:
        print(f'Fail to retry payment {payment["correlationId"]}')
