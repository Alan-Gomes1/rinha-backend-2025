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


async def add_fallback_payment(correlation_id: str, amount: float) -> None:
    """Adiciona um pagamento à fila de fallback.

    Args:
        correlation_id (str): O ID de correlação do pagamento.
        amount (float): O valor do pagamento.
    """
    data = {'correlationId': correlation_id, 'amount': amount}
    payment = orjson.dumps(data).decode()
    await redis_client.lpush(settings.REDIS_FALLBACK_QUEUE, payment)
