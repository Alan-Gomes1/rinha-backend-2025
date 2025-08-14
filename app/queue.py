import asyncio

from app.settings import redis_client, settings


async def consumer_loop(
    queue_name: str, payment_processor: callable, worker_id_prefix: str
) -> None:
    """Inicia um loop de consumidor genérico para uma fila específica.

    Args:
        queue_name (str): O nome da fila do Redis para consumir.
        payment_processor (callable): A função para processar cada mensagem.
        worker_id_prefix (str): Um prefixo para identificar os workers.
    """

    async def worker(worker_id: int) -> None:
        print(f'{worker_id_prefix} Worker {worker_id} started...')
        while True:
            try:
                message = await redis_client.brpop(queue_name, timeout=1)
                if not message:
                    continue

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
        for i in range(settings.MAX_WORKERS)
    ]
    await asyncio.gather(*tasks)


async def fallback_consumer_loop(
    health_check: callable, payment_processor: callable,
) -> None:
    """Inicia o loop do consumidor para a fila de fallback.

    Args:
        payment_processor (callable): A função para processar cada pagamento.
    """

    async def fallback_worker(worker_id: int) -> None:
        print(f'Fallback Worker {worker_id} started...')
        while True:
            try:
                message = await redis_client.brpop(
                    settings.REDIS_FALLBACK_QUEUE, timeout=1
                )
                if message is None:
                    continue

                _, data = message
                print(f'Fallback Worker {worker_id} received a job.')
                await asyncio.sleep(settings.FALLBACK_WORKER_DELAY)
                await health_check()
                try:
                    await payment_processor(data)
                except Exception as ex:
                    print(
                        f'Error processing fallback payment: {ex}. Re-queuing'
                    )
                    await redis_client.lpush(
                        settings.REDIS_FALLBACK_QUEUE, data
                    )

            except Exception as ex:
                print(f'Error in fallback worker {worker_id}: {ex}')
                await asyncio.sleep(2)

    tasks = [
        asyncio.create_task(fallback_worker(i))
        for i in range(settings.MAX_WORKERS)
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    from app.services import health_check, payment_processor

    print('Starting consumer workers...')
    main_loop = consumer_loop(
        settings.REDIS_QUEUE, payment_processor, 'Main'
    )
    fallback_loop = fallback_consumer_loop(health_check, payment_processor)
    asyncio.run(asyncio.gather(main_loop, fallback_loop))
