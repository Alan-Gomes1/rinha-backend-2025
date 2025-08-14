from datetime import datetime
from http import HTTPStatus

from fastapi import BackgroundTasks, FastAPI

from app.models import PaymentRequest
from app.services import get_summary
from app.tasks import add_payment

app = FastAPI()


@app.post('/payments', status_code=HTTPStatus.ACCEPTED)
async def payments(pr: PaymentRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(add_payment, pr.correlationId, pr.amount)
    return {'status': 'accepted'}


@app.get('/payments-summary', status_code=HTTPStatus.OK)
async def payments_summary(from_: str | None = None, to: str | None = None):
    from_ = datetime.fromisoformat(from_) if from_ else None
    to = datetime.fromisoformat(to) if to else None
    return await get_summary(from_, to)
