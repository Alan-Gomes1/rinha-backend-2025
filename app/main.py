from http import HTTPStatus

from fastapi import BackgroundTasks, FastAPI

from .models import PaymentRequest
from .queue import init_consumer_loop
from .services import send_payment
from .tasks import add_payment

app = FastAPI()
init_consumer_loop(send_payment)


@app.post('/payments', status_code=HTTPStatus.ACCEPTED)
async def payments(pr: PaymentRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(add_payment, pr.correlationId, pr.amount)
    return {'status': 'accepted'}
