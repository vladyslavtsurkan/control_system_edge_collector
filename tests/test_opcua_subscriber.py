"""Tests for the OPC UA subscriber (unit-level, no real OPC UA server)."""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from asyncua.crypto.security_policies import (
    SecurityPolicyAes128Sha256RsaOaep,
    SecurityPolicyAes256Sha256RsaPss,
    SecurityPolicyBasic256Sha256,
    SecurityPolicyNone,
)

from app.data_plane.opcua_subscriber import (
    OpcuaSubscriber,
    SECURITY_POLICY_MAP,
    _DataChangeHandler,
)
from app.data_plane.persistent_buffer import PersistentEdgeBuffer
from app.models.config import SensorConfig


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
    async def test_persists_payload(self, tmp_path) -> None:
        sensor_id = uuid4()
        node_id = "ns=2;i=1001"
        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()

        handler = _DataChangeHandler({node_id: sensor_id}, buffer)

        # Build a mock node
        mock_node = MagicMock()
        mock_node.nodeid.to_string.return_value = node_id

        # Build a mock DataChangeNotif
        mock_data = MagicMock()
        mock_data.monitored_item.Value.StatusCode.name = "Good"

        handler.datachange_notification(mock_node, 42.0, mock_data)
        await handler.flush_pending()

        rows = await buffer.get_batch(10)
        await buffer.close()

        assert len(rows) == 1
        item = rows[0]["payload"]
        assert item["sensor_id"] == str(sensor_id)
        assert item["payload"]["value"] == 42.0
        assert item["payload"]["status"] == "Good"

    async def test_unmapped_node_is_ignored(self, tmp_path) -> None:
        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()
        handler = _DataChangeHandler({}, buffer)

        mock_node = MagicMock()
        mock_node.nodeid.to_string.return_value = "ns=2;i=9999"

        mock_data = MagicMock()
        handler.datachange_notification(mock_node, 1, mock_data)
        await handler.flush_pending()

        rows = await buffer.get_batch(10)
        await buffer.close()
        assert rows == []

    async def test_multiple_notifications_are_buffered_in_order(self, tmp_path) -> None:
        sensor_id = uuid4()
        node_id = "ns=2;i=1001"
        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()

        handler = _DataChangeHandler({node_id: sensor_id}, buffer)

        mock_node = MagicMock()
        mock_node.nodeid.to_string.return_value = node_id
        mock_data = MagicMock()
        mock_data.monitored_item.Value.StatusCode.name = "Good"

        handler.datachange_notification(mock_node, 1.0, mock_data)
        handler.datachange_notification(mock_node, 2.0, mock_data)
        await handler.flush_pending()

        rows = await buffer.get_batch(10)
        await buffer.close()
        assert [row["payload"]["payload"]["value"] for row in rows] == [1.0, 2.0]


class TestOpcuaSubscriberStartupRetry:
    async def test_start_retries_after_transient_connect_error(
        self,
        sample_config,
        settings,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        object.__setattr__(settings, "BACKOFF_MAX_RETRIES", 3)
        object.__setattr__(settings, "BACKOFF_BASE_S", 0.0)
        object.__setattr__(settings, "BACKOFF_MAX_S", 0.0)

        connect_attempts = 0

        class _FakeSubscription:
            async def subscribe_data_change(self, nodes):
                return list(range(len(nodes)))

            async def unsubscribe(self, _handles):
                return None

            async def delete(self):
                return None

        class _FakeClient:
            def __init__(self, url: str) -> None:
                self.url = url

            async def set_security(self, **_kwargs):
                return None

            def set_user(self, _user: str) -> None:
                return None

            def set_password(self, _password: str) -> None:
                return None

            async def connect(self) -> None:
                nonlocal connect_attempts
                connect_attempts += 1
                if connect_attempts == 1:
                    raise ConnectionRefusedError("simulated startup race")

            async def create_subscription(self, period: int, handler):
                _ = (period, handler)
                return _FakeSubscription()

            def get_node(self, node_id: str):
                return node_id

            async def disconnect(self) -> None:
                return None

        monkeypatch.setattr("app.data_plane.opcua_subscriber.Client", _FakeClient)

        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()

        subscriber = OpcuaSubscriber(sample_config, buffer)
        await subscriber.start()

        assert subscriber.is_connected is True
        assert connect_attempts == 2

        await subscriber.stop()
        await buffer.close()

    async def test_start_raises_after_retry_exhausted(
        self,
        sample_config,
        settings,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        object.__setattr__(settings, "BACKOFF_MAX_RETRIES", 2)
        object.__setattr__(settings, "BACKOFF_BASE_S", 0.0)
        object.__setattr__(settings, "BACKOFF_MAX_S", 0.0)

        class _FakeClient:
            def __init__(self, url: str) -> None:
                self.url = url

            async def set_security(self, **_kwargs):
                return None

            def set_user(self, _user: str) -> None:
                return None

            def set_password(self, _password: str) -> None:
                return None

            async def connect(self) -> None:
                raise ConnectionRefusedError("still down")

            async def disconnect(self) -> None:
                return None

        monkeypatch.setattr("app.data_plane.opcua_subscriber.Client", _FakeClient)

        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()

        subscriber = OpcuaSubscriber(sample_config, buffer)
        with pytest.raises(ConnectionRefusedError):
            await subscriber.start()

        await buffer.close()


class TestOpcuaSubscriberReconfigure:
    async def test_sensor_only_change_updates_subscriptions_without_reconnect(
        self,
        sample_config,
        settings,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        object.__setattr__(settings, "BACKOFF_MAX_RETRIES", 1)
        object.__setattr__(settings, "BACKOFF_BASE_S", 0.0)
        object.__setattr__(settings, "BACKOFF_MAX_S", 0.0)

        class _FakeSubscription:
            def __init__(self) -> None:
                self._next = 1
                self.unsubscribed: list[list[int]] = []

            async def subscribe_data_change(self, nodes):
                handles = list(range(self._next, self._next + len(nodes)))
                self._next += len(nodes)
                return handles

            async def unsubscribe(self, handles):
                self.unsubscribed.append(list(handles))

            async def delete(self):
                return None

        class _FakeClient:
            connect_calls = 0
            disconnect_calls = 0

            def __init__(self, url: str) -> None:
                self.url = url
                self.subscription = _FakeSubscription()

            async def set_security(self, **_kwargs):
                return None

            def set_user(self, _user: str) -> None:
                return None

            def set_password(self, _password: str) -> None:
                return None

            async def connect(self) -> None:
                type(self).connect_calls += 1

            async def create_subscription(self, period: int, handler):
                _ = (period, handler)
                return self.subscription

            def get_node(self, node_id: str):
                return node_id

            async def disconnect(self) -> None:
                type(self).disconnect_calls += 1

        monkeypatch.setattr("app.data_plane.opcua_subscriber.Client", _FakeClient)

        sensor_b = SensorConfig(
            id=uuid4(),
            name="Pressure Sensor",
            node_id="ns=2;i=2002",
            units="bar",
        )
        updated_config = sample_config.model_copy(update={"sensors": [sensor_b]})

        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()

        subscriber = OpcuaSubscriber(sample_config, buffer)
        await subscriber.start()
        first_client = subscriber._client
        first_subscription = subscriber._subscription
        assert _FakeClient.connect_calls == 1

        await subscriber.reconfigure(updated_config)

        assert _FakeClient.connect_calls == 1
        assert subscriber._client is first_client
        assert subscriber._subscription is first_subscription
        assert first_subscription.unsubscribed == [[1]]
        assert set(subscriber._node_to_sensor.keys()) == {"ns=2;i=2002"}

        await subscriber.stop()
        await buffer.close()

    async def test_connection_change_triggers_full_reconnect(
        self,
        sample_config,
        settings,
        tmp_path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        object.__setattr__(settings, "BACKOFF_MAX_RETRIES", 1)
        object.__setattr__(settings, "BACKOFF_BASE_S", 0.0)
        object.__setattr__(settings, "BACKOFF_MAX_S", 0.0)

        class _FakeSubscription:
            async def subscribe_data_change(self, nodes):
                return list(range(1, len(nodes) + 1))

            async def unsubscribe(self, _handles):
                return None

            async def delete(self):
                return None

        class _FakeClient:
            connect_calls = 0
            disconnect_calls = 0

            def __init__(self, url: str) -> None:
                self.url = url

            async def set_security(self, **_kwargs):
                return None

            def set_user(self, _user: str) -> None:
                return None

            def set_password(self, _password: str) -> None:
                return None

            async def connect(self) -> None:
                type(self).connect_calls += 1

            async def create_subscription(self, period: int, handler):
                _ = (period, handler)
                return _FakeSubscription()

            def get_node(self, node_id: str):
                return node_id

            async def disconnect(self) -> None:
                type(self).disconnect_calls += 1

        monkeypatch.setattr("app.data_plane.opcua_subscriber.Client", _FakeClient)

        updated_config = sample_config.model_copy(
            update={"url": "opc.tcp://other-host:4840"}
        )

        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()

        subscriber = OpcuaSubscriber(sample_config, buffer)
        await subscriber.start()
        assert _FakeClient.connect_calls == 1

        await subscriber.reconfigure(updated_config)

        assert _FakeClient.connect_calls == 2
        assert _FakeClient.disconnect_calls >= 1

        await subscriber.stop()
        await buffer.close()


class TestOpcuaSubscriberWriteNodeValue:
    async def test_write_node_value_writes_when_connected(
        self,
        sample_config,
        tmp_path,
    ) -> None:
        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()
        subscriber = OpcuaSubscriber(sample_config, buffer)

        class _FakeNode:
            def __init__(self) -> None:
                self.value = None

            async def write_value(self, value):
                self.value = value

        fake_node = _FakeNode()

        class _FakeClient:
            def get_node(self, _node_id: str):
                return fake_node

        subscriber._client = _FakeClient()  # type: ignore[assignment]
        subscriber._connected = True

        await subscriber.write_node_value("ns=2;s=Target_Temperature", 55.5)

        assert fake_node.value == 55.5
        await buffer.close()

    async def test_write_node_value_raises_when_not_connected(
        self,
        sample_config,
        tmp_path,
    ) -> None:
        buffer = PersistentEdgeBuffer(tmp_path / "edge_buffer.db")
        await buffer.initialize()
        subscriber = OpcuaSubscriber(sample_config, buffer)

        with pytest.raises(RuntimeError, match="not connected"):
            await subscriber.write_node_value("ns=2;s=Target_Temperature", 42.0)

        await buffer.close()
