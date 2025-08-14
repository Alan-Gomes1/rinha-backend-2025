import asyncio
from http import HTTPStatus

from fastapi import BackgroundTasks, FastAPI

from app.models import PaymentRequest
from app.queue import consumer_loop
from app.services import send_payment
from app.tasks import add_payment

app = FastAPI()


@app.post('/payments', status_code=HTTPStatus.ACCEPTED)
async def payments(pr: PaymentRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(add_payment, pr.correlationId, pr.amount)
    return {'status': 'accepted'}


if __name__ == '__main__':
    asyncio.run(consumer_loop(send_payment))
