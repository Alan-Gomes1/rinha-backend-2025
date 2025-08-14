import asyncio
from datetime import datetime, timezone

import httpx
import orjson

from .services import save_payment
from .settings import redis_client, settings


async def consumer_loop(send_payment: callable) -> None:
    """Inicia o loop principal do consumidor, criando múltiplos workers
    para processar mensagens da fila.

    Args:
        send_payment (callable): A função assíncrona a ser chamada para
        processar cada pagamento.
    """
    async def worker(worder_id: int) -> None:
        """Worker assíncrono para processar mensagens da fila do Redis.

        Args:
            worder_id (int): O ID do worker.
        """
        while True:
            try:
                data = redis_client.brpop(settings.REDIS_QUEUE, timeout=1)
                try:
                    payment = orjson.loads(data)
                    request_at = datetime.now(timezone.utc)
                    timeout = httpx.Timeout(30, connect=1.0)
                    if await send_payment(payment, request_at, timeout):
                        await save_payment(payment, request_at)
                except Exception:
                    print(
                        f'Error payment {payment['correlationId']}: try again'
                    )
                    await redis_client.lpush(settings.REDIS_QUEUE, data)
            except Exception as ex:
                print(f'Error worker {worder_id}: {ex}')

    tasks = [
        asyncio.create_task(worker(i)) for i in range(settings.MAX_WORKERS)
    ]
    asyncio.gather(*tasks)
