from functools import lru_cache

import redis.asyncio as ioredis
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8'
    )
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int
    REDIS_QUEUE: str
    REDIS_FALLBACK_QUEUE: str
    REDIS_DEAD_LETTER_QUEUE: str
    MAX_WORKERS: int
    MIN_WORKERS: int
    FALLBACK_WORKER_DELAY: int
    PAYMENT_PROCESSOR_URL: str
    PAYMENT_ZKEY: str
    PAYMENT_FALLBACK_ZKEY: str
    MAX_RETRIES: int
    TIMEOUT: int
    CONNECT_TIMEOUT: float
    REDIS_KEY_EXPIRATION: int
    PAYMENT_KEY: str


@lru_cache()
def get_settings() -> Settings:
    """Gera uma instância do arquivo de configurações .env

    Returns:
        Settings: configurações
    """
    return Settings()


def get_redis_client(settings: Settings) -> ioredis.Redis:
    """Pela a conexão com o redis a partir de um pool de conexões.

    Args:
        settings (Settings): Objeto de configurações

    Returns:
        redis.Redis: cliente Redis
    """
    pool = ioredis.ConnectionPool(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
    )
    return ioredis.Redis.from_pool(pool)


settings = get_settings()
redis_client = get_redis_client(settings)
