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
