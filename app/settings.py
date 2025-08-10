from functools import lru_cache
from typing import Final

from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio.connection import ConnectionPool


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8'
    )
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int
    REDIS_QUEUE: str
    MAX_WORKERS: int


@lru_cache()
def get_settings() -> Settings:
    """Gera uma instância do arquivo de configurações .env

    Returns:
        Settings: configurações
    """
    return Settings()


def get_redis_client(settings: Settings) -> ConnectionPool:
    """Pega a conexão com o redis

    Args:
        settings (Settings): Objeto de configurações

    Returns:
        ConnectionPool: conexão
    """
    return ConnectionPool(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
    )


settings = Final[get_settings()]
redis_client = Final[get_redis_client(settings)]
