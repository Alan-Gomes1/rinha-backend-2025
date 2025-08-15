import asyncio

from app.settings import redis_client, settings


async def consumer_loop(
    queue_name: str, payment_processor: callable, worker_id_prefix: str,
    workers: int
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

                if await redis_client.get('failing') == 'true':
                    await asyncio.sleep(settings.FALLBACK_WORKER_DELAY)

                _, data = message
                print(f'{worker_id_prefix} Worker {worker_id} received a job.')
                try:
                    await payment_processor(data)
                except Exception as ex:
                    print(f'Error processing payment: {ex}. Re-queuing...')
                    await redis_client.lpush(queue_name, data)

            except Exception as ex:
                print(f'Error in {worker_id_prefix} worker {worker_id}: {ex}')
                await asyncio.sleep(2)

    tasks = [
        asyncio.create_task(worker(i))
        for i in range(workers)
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    from app.services import health_check_loop, payment_processor

    print('Starting consumer workers...')
    main_consumer_loop = consumer_loop(
        settings.REDIS_QUEUE, payment_processor, 'Main', settings.MAX_WORKERS
    )
    fallback_consumer_loop = consumer_loop(
        settings.REDIS_FALLBACK_QUEUE, payment_processor, 'Fallback',
        settings.MIN_WORKERS
    )
    health_check_task = health_check_loop()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(
            main_consumer_loop, fallback_consumer_loop, health_check_task
        )
    )
