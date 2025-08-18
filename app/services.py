import asyncio
from datetime import datetime, timezone

import httpx
import orjson

from .settings import redis_client, settings
from .tasks import add_fallback_payment

client = httpx.AsyncClient()
LUA_SUMMARY_SCRIPT = """
local default_payments = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])
local fallback_payments = redis.call('ZRANGEBYSCORE', KEYS[2], ARGV[1], ARGV[2])
local default_total_requests = 0
local default_total_amount = 0.0
local fallback_total_requests = 0
local fallback_total_amount = 0.0

for _, payment_json in ipairs(default_payments) do
    local payment = cjson.decode(payment_json)
    default_total_requests = default_total_requests + 1
    default_total_amount = default_total_amount + tonumber(payment['amount'])
end

for _, payment_json in ipairs(fallback_payments) do
    local payment = cjson.decode(payment_json)
    fallback_total_requests = fallback_total_requests + 1
    fallback_total_amount = fallback_total_amount + tonumber(payment['amount'])
end

return {
    default_total_requests,
    default_total_amount,
    fallback_total_requests,
    fallback_total_amount
}
"""
LUA_SUMMARY_SCRIPT_SHA: str | None = None


async def load_lua_scripts() -> None:
    """Carrega os scripts Lua no Redis e armazena os hashes SHA."""
    global LUA_SUMMARY_SCRIPT_SHA
    if not LUA_SUMMARY_SCRIPT_SHA:
        LUA_SUMMARY_SCRIPT_SHA = await redis_client.script_load(
            LUA_SUMMARY_SCRIPT
        )


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
    data = {**payment, 'requestedAt': requested_at.isoformat()}
    try:
        response = await client.post(url, json=data, timeout=timeout)
        response.raise_for_status()
        return True
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        print(f'Error sending payment: {e}. Adding to fallback queue.')
        return False


async def save_payment(
    payment: dict, requested_at: datetime, is_fallback: bool = False
) -> None:
    """Salva o pagamento processado.

    Args:
        payment (dict): O dicionário contendo os dados do pagamento.
        requested_at (datetime): Data e hora da requisição do pagamento.
        is_fallback (bool, optional): Se o pagamento é de fallback.
    """
    if isinstance(requested_at, datetime):
        timestamp = requested_at.timestamp()
    else:
        timestamp = float(requested_at)

    data = {**payment, 'requested_at': timestamp}
    payment_json = orjson.dumps(data).decode()
    key = (
        settings.PAYMENT_FALLBACK_ZKEY
        if is_fallback
        else settings.PAYMENT_ZKEY
    )
    await redis_client.zadd(key, {payment_json: timestamp})


async def purge_payments() -> None:
    """Remove todos os pagamentos registrados no Redis."""
    await redis_client.delete(
        settings.PAYMENT_ZKEY, settings.PAYMENT_FALLBACK_ZKEY
    )


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
    result = await redis_client.evalsha(
        LUA_SUMMARY_SCRIPT_SHA,
        2,
        settings.PAYMENT_ZKEY,
        settings.PAYMENT_FALLBACK_ZKEY,
        min_score,
        max_score,
    )
    summary = {
        'default': {
            'totalRequests': result[0],
            'totalAmount': float(result[1]),
        },
        'fallback': {
            'totalRequests': result[2],
            'totalAmount': float(result[3]),
        },
    }
    return summary


async def payment_processor(data: bytes, is_fallback: bool = False) -> None:
    """Processa um pagamento recebido da fila e salva no banco em memória.

    Args:
        data (bytes): Os dados do pagamento em formato bytes.
        is_fallback (bool, optional): Se o pagamento é de fallback.
    """
    payment = orjson.loads(data)
    correlation_id = payment.get('correlationId')
    processed_key = f'processed:{correlation_id}'
    if await redis_client.get(processed_key):
        print(f'Payment {correlation_id} already processed. Skipping.')
        return

    request_at = datetime.now(tz=timezone.utc)
    timeout = httpx.Timeout(settings.TIMEOUT, connect=settings.CONNECT_TIMEOUT)
    if await send_payment(payment, request_at, timeout):
        await save_payment(payment, request_at, is_fallback)
        await redis_client.set(
            processed_key, 'true', ex=settings.REDIS_KEY_EXPIRATION
        )
    elif not is_fallback:
        await add_fallback_payment(
            payment.get('correlationId'), payment.get('amount')
        )


async def health_check() -> None:
    """Define o tempo limite de timeout para requisitar o serviço de pagamento.

    Returns:
        int: valor do timeout
    """
    try:
        request = await client.get(
            f'{settings.PAYMENT_PROCESSOR_URL}/service-health'
        )
        response = request.json()
        failing = str(response.get('failing'))
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
