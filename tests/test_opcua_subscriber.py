"""Tests for the OPC UA subscriber (unit-level, no real OPC UA server)."""

import asyncio
from unittest.mock import MagicMock
from uuid import uuid4

from asyncua.crypto.security_policies import (
    SecurityPolicyAes128Sha256RsaOaep,
    SecurityPolicyAes256Sha256RsaPss,
    SecurityPolicyBasic256Sha256,
    SecurityPolicyNone,
)

from app.data_plane.opcua_subscriber import (
    SECURITY_POLICY_MAP,
    _DataChangeHandler,
)
from app.models.telemetry import TelemetryPayload


class TestSecurityPolicyMap:
    def test_known_policies(self) -> None:
        assert "None" in SECURITY_POLICY_MAP
        assert "Basic256Sha256" in SECURITY_POLICY_MAP
        assert "Aes128_Sha256_RsaOaep" in SECURITY_POLICY_MAP
        assert "Aes256_Sha256_RsaPss" in SECURITY_POLICY_MAP

    def test_maps_to_correct_classes(self) -> None:
        assert SECURITY_POLICY_MAP["None"] is SecurityPolicyNone
        assert SECURITY_POLICY_MAP["Basic256Sha256"] is SecurityPolicyBasic256Sha256
        assert (
            SECURITY_POLICY_MAP["Aes128_Sha256_RsaOaep"]
            is SecurityPolicyAes128Sha256RsaOaep
        )
        assert (
            SECURITY_POLICY_MAP["Aes256_Sha256_RsaPss"]
            is SecurityPolicyAes256Sha256RsaPss
        )

    def test_all_values_have_uri(self) -> None:
        for key, cls in SECURITY_POLICY_MAP.items():
            assert hasattr(cls, "URI"), f"{key} class has no URI attribute"
            assert cls.URI.startswith("http://opcfoundation.org/UA/SecurityPolicy#"), (
                f"{key} has invalid URI: {cls.URI}"
            )


class TestDataChangeHandler:
    def test_enqueues_payload(self) -> None:
        sensor_id = uuid4()
        node_id = "ns=2;i=1001"
        q: asyncio.Queue[TelemetryPayload] = asyncio.Queue(maxsize=10)

        handler = _DataChangeHandler({node_id: sensor_id}, q)

        # Build a mock node
        mock_node = MagicMock()
        mock_node.nodeid.to_string.return_value = node_id

        # Build a mock DataChangeNotif
        mock_data = MagicMock()
        mock_data.monitored_item.Value.StatusCode.name = "Good"

        handler.datachange_notification(mock_node, 42.0, mock_data)

        assert q.qsize() == 1
        item = q.get_nowait()
        assert item.sensor_id == sensor_id
        assert item.payload.value == 42.0
        assert item.payload.status == "Good"

    def test_unmapped_node_is_ignored(self) -> None:
        q: asyncio.Queue[TelemetryPayload] = asyncio.Queue(maxsize=10)
        handler = _DataChangeHandler({}, q)

        mock_node = MagicMock()
        mock_node.nodeid.to_string.return_value = "ns=2;i=9999"

        mock_data = MagicMock()
        handler.datachange_notification(mock_node, 1, mock_data)

        assert q.qsize() == 0

    def test_full_queue_drops_message(self) -> None:
        sensor_id = uuid4()
        node_id = "ns=2;i=1001"
        q: asyncio.Queue[TelemetryPayload] = asyncio.Queue(maxsize=1)

        handler = _DataChangeHandler({node_id: sensor_id}, q)

        mock_node = MagicMock()
        mock_node.nodeid.to_string.return_value = node_id
        mock_data = MagicMock()
        mock_data.monitored_item.Value.StatusCode.name = "Good"

        # Fill the queue
        handler.datachange_notification(mock_node, 1.0, mock_data)
        assert q.qsize() == 1

        # This should drop (queue full) without raising
        handler.datachange_notification(mock_node, 2.0, mock_data)
        assert q.qsize() == 1
