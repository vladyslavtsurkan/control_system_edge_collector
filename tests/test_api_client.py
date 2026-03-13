"""Tests for the Control-Plane API client."""

from uuid import uuid4

import httpx
import pytest
import respx

from app.control_plane.api_client import ApiClientError, fetch_collector_config
from app.settings import Settings


@pytest.fixture()
def _cloud_api_config_payload() -> dict:
    return {
        "id": str(uuid4()),
        "name": "Test Collector",
        "url": "opc.tcp://localhost:4840",
        "security_policy": "None",
        "authentication_method": "anonymous",
        "username": None,
        "password": None,
        "sensors": [
            {
                "id": str(uuid4()),
                "name": "Temp",
                "node_id": "ns=2;i=1001",
                "units": "°C",
            }
        ],
    }


class TestFetchCollectorConfig:
    @respx.mock
    async def test_success(
        self,
        settings: Settings,
        _cloud_api_config_payload: dict,
    ) -> None:
        respx.get(f"{settings.CLOUD_API_URL}api/v1/collector/config").respond(
            200, json=_cloud_api_config_payload
        )

        config = await fetch_collector_config()
        assert config.name == "Test Collector"
        assert len(config.sensors) == 1

    @respx.mock
    async def test_4xx_raises_immediately(self, settings: Settings) -> None:
        respx.get(f"{settings.CLOUD_API_URL}api/v1/collector/config").respond(
            403, text="Forbidden"
        )

        with pytest.raises(ApiClientError, match="403"):
            await fetch_collector_config()

    @respx.mock
    async def test_5xx_retries_then_succeeds(
        self,
        settings: Settings,
        _cloud_api_config_payload: dict,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Override settings for fast test
        monkeypatch.setattr(settings, "BACKOFF_BASE_S", 0.01)
        monkeypatch.setattr(settings, "BACKOFF_MAX_S", 0.05)
        monkeypatch.setattr(settings, "BACKOFF_MAX_RETRIES", 3)

        route = respx.get(f"{settings.CLOUD_API_URL}api/v1/collector/config")
        route.side_effect = [
            httpx.Response(500, text="Internal Server Error"),
            httpx.Response(200, json=_cloud_api_config_payload),
        ]

        config = await fetch_collector_config()
        assert config.name == "Test Collector"
        assert route.call_count == 2
