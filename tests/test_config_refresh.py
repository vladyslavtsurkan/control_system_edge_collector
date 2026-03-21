"""Tests for config refresh helpers."""

from uuid import uuid4

from app.control_plane.config_refresh import (
    _opcua_runtime_snapshot,
    _snapshot_diff,
)
from app.models.config import CollectorConfig, SensorConfig


def _make_config(*, sensors: list[SensorConfig]) -> CollectorConfig:
    return CollectorConfig(
        id=uuid4(),
        name="collector",
        url="opc.tcp://localhost:4840",
        security_policy="None",
        authentication_method="anonymous",
        sensors=sensors,
    )


class TestConfigRefreshHelpers:
    def test_runtime_snapshot_normalizes_sensor_order(self) -> None:
        sensor_a = SensorConfig(
            id=uuid4(),
            name="A",
            node_id="ns=2;i=1001",
            units="C",
        )
        sensor_b = SensorConfig(
            id=uuid4(),
            name="B",
            node_id="ns=2;i=1002",
            units="C",
        )

        cfg1 = _make_config(sensors=[sensor_a, sensor_b])
        cfg2 = _make_config(sensors=[sensor_b, sensor_a])

        assert _opcua_runtime_snapshot(cfg1) == _opcua_runtime_snapshot(cfg2)

    def test_snapshot_diff_masks_password(self) -> None:
        before = {
            "url": "opc.tcp://localhost:4840",
            "password": "old-secret",
        }
        after = {
            "url": "opc.tcp://localhost:4840",
            "password": "new-secret",
        }

        diff = _snapshot_diff(before, after)

        assert diff["changed_fields"] == ["password"]
        assert diff["details"]["password"] == {"before": "***", "after": "***"}
