"""Shared pytest fixtures."""

import asyncio
from uuid import uuid4

import pytest

from app.models.config import CollectorConfig, SensorConfig
from app.settings import Settings, get_settings


@pytest.fixture()
def env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set minimal required environment variables."""
    monkeypatch.setenv("CLOUD_API_URL", "https://api.test.com/")
    monkeypatch.setenv("X_API_KEY", "test-key-123")
    monkeypatch.setenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    monkeypatch.setenv("AMQP_EXCHANGE", "test_exchange")


@pytest.fixture()
def settings(env_vars: None, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Create a test Settings and patch ``get_settings()`` everywhere."""
    get_settings.cache_clear()
    s = Settings()
    _getter = lambda: s  # noqa: E731
    monkeypatch.setattr("app.settings.get_settings", _getter)
    monkeypatch.setattr("app.control_plane.api_client.get_settings", _getter)
    monkeypatch.setattr("app.data_plane.amqp_publisher.get_settings", _getter)
    monkeypatch.setattr("app.data_plane.opcua_subscriber.get_settings", _getter)
    monkeypatch.setattr("app.data_plane.persistent_buffer.get_settings", _getter)
    return s


@pytest.fixture()
def sample_sensor() -> SensorConfig:
    return SensorConfig(
        id=uuid4(),
        name="Temperature Sensor 1",
        node_id="ns=2;i=1001",
        units="°C",
    )


@pytest.fixture()
def sample_config(sample_sensor: SensorConfig) -> CollectorConfig:
    return CollectorConfig(
        id=uuid4(),
        name="Test Collector",
        url="opc.tcp://localhost:4840",
        security_policy="None",
        authentication_method="anonymous",
        sensors=[sample_sensor],
    )


@pytest.fixture()
def telemetry_queue() -> asyncio.Queue:
    return asyncio.Queue(maxsize=100)
