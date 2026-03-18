"""Environment-based configuration via *pydantic-settings*."""

from functools import lru_cache
from pathlib import Path

from pydantic import AmqpDsn, HttpUrl, SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["Settings", "get_settings"]


class Settings(BaseSettings):
    """All tunables loaded from env vars / ``.env`` file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # Control Plane
    CLOUD_API_URL: HttpUrl = Field(..., alias="CLOUD_API_URL")
    X_API_KEY: SecretStr = Field(..., alias="X_API_KEY")

    # Data Plane
    AMQP_URL: AmqpDsn = Field(..., alias="AMQP_URL")
    AMQP_EXCHANGE: str = Field("iiot_telemetry", alias="AMQP_EXCHANGE")

    # OPC-UA certificates (optional; required if security policy is not "None")
    OPCUA_CERT_PATH: Path | None = Field(None, alias="OPCUA_CERT_PATH")
    OPCUA_KEY_PATH: Path | None = Field(None, alias="OPCUA_KEY_PATH")

    # Health endpoint
    HEALTH_HOST: str = Field("0.0.0.0", alias="HEALTH_HOST")
    HEALTH_PORT: int = Field(8080, alias="HEALTH_PORT")

    # Tuning
    EDGE_BUFFER_DB_PATH: Path = Field(
        Path("edge_buffer.db"), alias="EDGE_BUFFER_DB_PATH"
    )
    QUEUE_MAX_SIZE: int = Field(10_000, alias="QUEUE_MAX_SIZE")
    BATCH_SIZE: int = Field(100, alias="BATCH_SIZE")
    BATCH_TIMEOUT_S: float = Field(1.0, alias="BATCH_TIMEOUT_S")
    BACKOFF_BASE_S: float = Field(1.0, alias="BACKOFF_BASE_S")
    BACKOFF_MAX_S: float = Field(60.0, alias="BACKOFF_MAX_S")
    BACKOFF_MAX_RETRIES: int = Field(5, alias="BACKOFF_MAX_RETRIES")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application-wide :class:`Settings` singleton.

    The instance is created lazily on first call so that importing this
    module never triggers env-var validation (safe for tests and CLI
    tools).
    """
    return Settings()
