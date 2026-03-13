"""Tests for Pydantic models."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.models.config import CollectorConfig, SensorConfig
from app.models.telemetry import TelemetryPayload, TelemetryValue


class TestSensorConfig:
    def test_valid(self) -> None:
        s = SensorConfig(id=uuid4(), name="Temp", node_id="ns=2;i=1001", units="°C")
        assert s.name == "Temp"

    def test_missing_field_raises(self) -> None:
        with pytest.raises(Exception):
            SensorConfig(id=uuid4(), name="Temp", node_id="ns=2;i=1001")  # type: ignore[call-arg]


class TestCollectorConfig:
    def test_anonymous_auth(self, sample_sensor: SensorConfig) -> None:
        cfg = CollectorConfig(
            id=uuid4(),
            name="c",
            url="opc.tcp://localhost:4840",
            security_policy="None",
            authentication_method="anonymous",
            sensors=[sample_sensor],
        )
        assert cfg.authentication_method == "anonymous"

    def test_username_password_valid(self, sample_sensor: SensorConfig) -> None:
        cfg = CollectorConfig(
            id=uuid4(),
            name="c",
            url="opc.tcp://localhost:4840",
            security_policy="None",
            authentication_method="username_password",
            username="admin",
            password="secret",
            sensors=[sample_sensor],
        )
        assert cfg.username == "admin"

    def test_username_password_missing_creds_raises(
        self, sample_sensor: SensorConfig
    ) -> None:
        with pytest.raises(ValueError, match="username and password are required"):
            CollectorConfig(
                id=uuid4(),
                name="c",
                url="opc.tcp://localhost:4840",
                security_policy="None",
                authentication_method="username_password",
                sensors=[sample_sensor],
            )

    def test_invalid_auth_method_raises(self, sample_sensor: SensorConfig) -> None:
        with pytest.raises(Exception):
            CollectorConfig(
                id=uuid4(),
                name="c",
                url="opc.tcp://localhost:4840",
                security_policy="None",
                authentication_method="certificate",  # type: ignore[arg-type]
                sensors=[sample_sensor],
            )


class TestTelemetryPayload:
    def test_serialisation_roundtrip(self) -> None:
        tp = TelemetryPayload(
            sensor_id=uuid4(),
            time=datetime.now(UTC),
            payload=TelemetryValue(value=23.5, status="Good"),
        )
        d = tp.model_dump(mode="json")
        assert "sensor_id" in d
        assert d["payload"]["value"] == 23.5
        roundtripped = TelemetryPayload.model_validate(d)
        assert roundtripped.sensor_id == tp.sensor_id

    def test_various_value_types(self) -> None:
        for val in [42, 3.14, True, "hello"]:
            tv = TelemetryValue(value=val, status="Good")
            assert tv.value == val
