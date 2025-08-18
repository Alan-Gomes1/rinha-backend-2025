import asyncio
import orjson

from app.settings import redis_client, settings


async def consumer_loop(
    queue_name: str,
    payment_processor: callable,
    worker_id_prefix: str,
    workers: int,
) -> None:
    """Inicia um loop de consumidor genérico para uma fila específica.

    Args:
        queue_name (str): O nome da fila do Redis para consumir.
        payment_processor (callable): A função para processar cada mensagem.
        worker_id_prefix (str): Um prefixo para identificar os workers.
        workers (int): O número de workers a serem iniciados.
    """

    async def worker(worker_id: int) -> None:
        print(f'{worker_id_prefix} Worker {worker_id} started...')
        while True:
            try:
                message = await redis_client.brpop(queue_name, timeout=1)
                if not message:
                    continue

                _, data = message
                payment = orjson.loads(data)

                if not await payment_processor(data):
                    retry_count = payment.get('retry_count', 0) + 1
                    if retry_count > settings.MAX_RETRIES:
                        await redis_client.lpush(
                            settings.REDIS_DEAD_LETTER_QUEUE, data
                        )
                    else:
                        payment['retry_count'] = retry_count
                        new_data = orjson.dumps(payment)
                        if queue_name == settings.REDIS_QUEUE:
                            await redis_client.lpush(
                                settings.REDIS_FALLBACK_QUEUE, new_data
                            )
                        else:
                            await asyncio.sleep(2**retry_count)
                            await redis_client.lpush(queue_name, new_data)

            except Exception as ex:
                print(f'Error in {worker_id_prefix} worker {worker_id}: {ex}')
                await asyncio.sleep(2)

    tasks = [asyncio.create_task(worker(i)) for i in range(workers)]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    from app.services import health_check_loop, payment_processor

    print('Starting consumer workers...')
    main_consumer_loop = consumer_loop(
        settings.REDIS_QUEUE,
        payment_processor,
        'Main',
        settings.MIN_WORKERS,
    )
    fallback_consumer_loop = consumer_loop(
        settings.REDIS_FALLBACK_QUEUE,
        payment_processor,
        'Fallback',
        settings.MAX_WORKERS,
    )
    health_check_task = health_check_loop()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(
            main_consumer_loop, fallback_consumer_loop, health_check_task
        )
    )