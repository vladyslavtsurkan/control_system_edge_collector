"""Environment-based configuration via *pydantic-settings*."""

from functools import lru_cache
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.x509.oid import NameOID
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
    X_API_KEY_ID: str = Field(..., alias="X_API_KEY_ID")
    X_API_KEY_SECRET: SecretStr = Field(..., alias="X_API_KEY_SECRET")

    # Data Plane
    AMQP_URL: AmqpDsn = Field(..., alias="AMQP_URL")
    AMQP_EXCHANGE: str = Field("iiot_telemetry", alias="AMQP_EXCHANGE")
    AMQP_CONTROL_EXCHANGE: str = Field(
        "iiot_control_command", alias="AMQP_CONTROL_EXCHANGE"
    )
    AMQP_USE_TLS: bool = Field(True, alias="AMQP_USE_TLS")
    AMQP_HEARTBEAT_S: int = Field(15, alias="AMQP_HEARTBEAT_S")
    TLS_CA_CERT_PATH: str = Field(
        "/app/certs/ca_certificate.pem", alias="TLS_CA_CERT_PATH"
    )
    TLS_CLIENT_CERT_PATH: str = Field(
        "/app/certs/collector_certificate.pem", alias="TLS_CLIENT_CERT_PATH"
    )
    TLS_CLIENT_KEY_PATH: str = Field(
        "/app/certs/collector_key.pem", alias="TLS_CLIENT_KEY_PATH"
    )
    TLS_CHECK_HOSTNAME: bool = Field(True, alias="TLS_CHECK_HOSTNAME")

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

    CONFIG_REFRESH_INTERVAL_S: float = Field(300.0, alias="CONFIG_REFRESH_INTERVAL_S")
    CONFIG_REFRESH_JITTER_S: float = Field(10.0, alias="CONFIG_REFRESH_JITTER_S")

    BACKOFF_BASE_S: float = Field(1.0, alias="BACKOFF_BASE_S")
    BACKOFF_MAX_S: float = Field(60.0, alias="BACKOFF_MAX_S")
    BACKOFF_MAX_RETRIES: int = Field(5, alias="BACKOFF_MAX_RETRIES")

    _organization_id_cache: str | None = None

    @property
    def organization_id(self) -> str:
        if not self.AMQP_USE_TLS:
            if self._organization_id_cache is None:
                self._organization_id_cache = "00000000-0000-0000-0000-000000000000"
            return self._organization_id_cache

        if self._organization_id_cache is None:
            cert_bytes = Path(self.TLS_CLIENT_CERT_PATH).read_bytes()
            certificate = x509.load_pem_x509_certificate(cert_bytes, default_backend())
            common_names = certificate.subject.get_attributes_for_oid(
                NameOID.COMMON_NAME
            )
            if not common_names or not common_names[0].value:
                raise ValueError("COMMON_NAME not found in client certificate subject")
            self._organization_id_cache = common_names[0].value
        return self._organization_id_cache


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application-wide :class:`Settings` singleton.

    The instance is created lazily on first call so that importing this
    module never triggers env-var validation (safe for tests and CLI
    tools).
    """
    return Settings()
