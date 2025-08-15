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
    REDIS_PENDING_QUEUE: str
    REDIS_FALLBACK_QUEUE: str
    MAX_WORKERS: int
    MIN_WORKERS: int
    FALLBACK_WORKER_DELAY: int
    PAYMENT_PROCESSOR_URL: str
    PAYMENT_PROCESSOR_TIMEOUT: int
    PAYMENT_KEY: str
    PAYMENT_ZKEY: str


@lru_cache()
def get_settings() -> Settings:
    """Gera uma instância do arquivo de configurações .env

    Returns:
        Settings: configurações
    """
    return Settings()


def get_redis_client(settings: Settings) -> ioredis.Redis:
    """Pega a conexão com o redis

    Args:
        settings (Settings): Objeto de configurações

    Returns:
        redis.Redis: cliente Redis
    """
    return ioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
    )


settings = get_settings()
redis_client = get_redis_client(settings)
