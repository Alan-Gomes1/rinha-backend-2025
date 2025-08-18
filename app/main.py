from contextlib import asynccontextmanager
from datetime import datetime
from http import HTTPStatus

from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import ORJSONResponse

from app.models import PaymentRequest
from app.services import get_summary, load_lua_scripts, purge_payments
from app.tasks import add_payment


@asynccontextmanager
async def lifespan(app: FastAPI):
    await load_lua_scripts()
    yield


app = FastAPI(default_response_class=ORJSONResponse, lifespan=lifespan)


@app.post('/payments', status_code=HTTPStatus.ACCEPTED)
async def payments(pr: PaymentRequest, background_tasks: BackgroundTasks):
    background_tasks.add_task(add_payment, pr.correlationId, pr.amount)
    return {'status': 'accepted'}


@app.get('/payments-summary', status_code=HTTPStatus.OK)
async def payments_summary(from_: str | None = None, to: str | None = None):
    from_ = datetime.fromisoformat(from_) if from_ else None
    to = datetime.fromisoformat(to) if to else None
    return await get_summary(from_, to)


@app.post('/purge-payments', status_code=HTTPStatus.OK)
async def delete_payments():
    await purge_payments()
    return {'message': 'All payments purged.'}
