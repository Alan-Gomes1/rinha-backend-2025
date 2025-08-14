from datetime import datetime, timezone

import httpx
import orjson

from .settings import redis_client, settings
from .tasks import add_payment_to_fallback_queue

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
        await add_payment_to_fallback_queue(payment)
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
    payment = orjson.loads(data)
    request_at = datetime.now(tz=timezone.utc)
    timeout = httpx.Timeout(30, connect=1.0)
    if await send_payment(payment, request_at, timeout):
        await save_payment(payment, request_at)


async def health_check() -> int:
    """Define o tempo limite de timeout para requisitar o serviço de pagamento.

    Returns:
        int: valor do timeout
    """
    timeout_default = 30
    try:
        response = await client.get(
            f'{settings.PAYMENT_PROCESSOR_URL}/service-health'
        )
        min_response_time = response.get('minResponseTime', timeout_default)
        return 1.5 * int(min_response_time)
    except Exception as ex:
        print(f'Error checking health: {ex}')
        return timeout_default
