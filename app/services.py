import asyncio
from datetime import datetime, timezone

import httpx
import orjson

from .settings import redis_client, settings
from .tasks import retry_payment

client = httpx.AsyncClient()
LUA_SUMMARY_SCRIPT = """
local payments = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])
local total_requests = 0
local total_amount = 0

for _, payment_json in ipairs(payments) do
    local payment = cjson.decode(payment_json)
    total_requests = total_requests + 1
    total_amount = total_amount + payment['amount']
end

return {total_requests, total_amount}
"""


async def send_payment(
    payment: dict, requested_at: datetime, timeout: httpx.Timeout
) -> bool:
    """Envia um pagamento para o processador de pagamentos externo (default).

    Args:
        payment (dict): O dicionário contendo os dados do pagamento.
        requested_at (datetime): Data e hora do pagamento.
        timeout (httpx.Timeout): O objeto de timeout para a requisição HTTP.

    Returns:
        bool: Se o pagamento foi processado com sucesso.
    """
    url = settings.PAYMENT_PROCESSOR_URL
    if not requested_at.tzinfo:
        utc_requested_at = requested_at.replace(tzinfo=timezone.utc)
    else:  # Se já tiver fuso horário, converte para UTC
        utc_requested_at = requested_at.astimezone(timezone.utc)
    timestamp = utc_requested_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    data = {**payment, 'requestedAt': timestamp}
    try:
        response = await client.post(url, json=data, timeout=timeout)
        response.raise_for_status()
        return True
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        print(f'Error sending payment: {e}. Adding to fallback queue.')
        return False


async def save_payment(payment: dict, requested_at: datetime) -> None:
    """Salva o pagamento processado.

    Args:
        payment (dict): O dicionário contendo os dados do pagamento.
        requested_at (datetime): Data e hora da requisição do pagamento.
    """
    if isinstance(requested_at, datetime):
        timestamp = requested_at.timestamp()
    else:
        timestamp = float(requested_at)

    data = {**payment, 'requested_at': timestamp}
    payment_json = orjson.dumps(data).decode()
    await redis_client.zadd(settings.PAYMENT_ZKEY, {payment_json: timestamp})


async def get_summary(
    _from: datetime | None = None, to: datetime | None = None
) -> dict:
    """Gera um resumo dos pagamentos processados dentro de um período de tempo.

    Args:
        _from (datetime | None, optional): Data de início do período.
            Se None, considera desde o início dos registros. Defaults to None.
        to (datetime | None, optional): Data de fim do período.
            Se None, considera até o final dos registros. Defaults to None.
    Returns:
        dict: Um dicionário contendo o resumo dos pagamentos, incluindo o total
        de requisições e o valor total para pagamentos padrão e de fallback.
    """
    min_score = _from.timestamp() if _from else '-inf'
    max_score = to.timestamp() if to else '+inf'
    result = await redis_client.eval(
        LUA_SUMMARY_SCRIPT, 1, settings.PAYMENT_ZKEY, min_score, max_score
    )
    total_amount = float(result[1])
    summary = {
        'default': {'totalRequests': result[0], 'totalAmount': total_amount},
        'fallback': {'totalRequests': 0, 'totalAmount': 0.0},
    }
    return summary


async def payment_processor(data: bytes) -> None:
    """Processa um pagamento recebido da fila e salva no banco em memória.

    Args:
        data (bytes): Os dados do pagamento em formato bytes.
    """
    try:
        payment = orjson.loads(data)
        request_at = datetime.now(tz=timezone.utc)
        timeout = httpx.Timeout(30, connect=1.0)
        if await send_payment(payment, request_at, timeout):
            await save_payment(payment, request_at)
        await retry_payment(payment, timeout, send_payment, save_payment)
    except Exception as ex:
        print(f'Error processing payment: {ex}')
        await retry_payment(payment, timeout, send_payment, save_payment)


async def health_check() -> None:
    """Define o tempo limite de timeout para requisitar o serviço de pagamento.

    Returns:
        int: valor do timeout
    """
    timeout_default = 20
    try:
        request = await client.get(
            f'{settings.PAYMENT_PROCESSOR_URL}/service-health'
        )
        response = request.json()
        min_response_time = response.get('minResponseTime', 20)
        max_response_time = max(min_response_time, timeout_default)
        timeout = 1.5 * int(max_response_time)
        failing = str(response.get('failing'))
        await redis_client.set('timeout', timeout)
        await redis_client.set('failing', failing)
    except Exception as ex:
        print(f'Error checking health: {ex}')


async def health_check_loop() -> None:
    """Loop de verificação de saúde do serviço de pagamento."""
    while True:
        try:
            await health_check()
        except Exception as ex:
            print(f'Health check failed: {ex}')
        await asyncio.sleep(5)
