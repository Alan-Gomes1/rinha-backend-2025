import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from http import HTTPStatus

from fastapi import BackgroundTasks, FastAPI

from app.models import PaymentRequest
from app.queue import consumer_loop, fallback_consumer_loop
from app.services import get_summary, payment_processor
from app.settings import settings
from app.tasks import add_payment


@asynccontextmanager
async def lifespan(app: FastAPI):
    main_consumer = asyncio.create_task(
        consumer_loop(settings.REDIS_QUEUE, payment_processor, 'Main')
    )
    fallback_consumer = asyncio.create_task(
        fallback_consumer_loop(payment_processor)
    )
    yield
    main_consumer.cancel()
    fallback_consumer.cancel()


app = FastAPI(lifespan=lifespan)


@app.post('/payments', status_code=HTTPStatus.ACCEPTED)
async def payments(pr: PaymentRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(add_payment, pr.correlationId, pr.amount)
    return {'status': 'accepted'}


@app.get('/payments-summary', status_code=HTTPStatus.OK)
async def payments_summary(from_: str | None = None, to: str | None = None):
    from_ = datetime.fromisoformat(from_) if from_ else None
    to = datetime.fromisoformat(to) if to else None
    return await get_summary(from_, to)
