"""Tests for AMQP control subscriber (unit-level, no real RabbitMQ)."""

import asyncio
import ssl
from types import SimpleNamespace
from typing import Any, cast
from collections.abc import Callable
from unittest.mock import AsyncMock, Mock

import aio_pika
import pytest

from app.control_plane.amqp_subscriber import AmqpControlSubscriber
from app.data_plane.opcua_subscriber import OpcuaSubscriber
from app.generated import telemetry_pb2
from app.settings import Settings


class _FakeOpcuaClient:
    def __init__(self) -> None:
        self.write_node_value = AsyncMock()
        self._mapping: dict[str, str] = {}

    def resolve_control_node_id(self, sensor_ref: str) -> str | None:
        if sensor_ref.startswith("ns=") and ";" in sensor_ref:
            return sensor_ref
        return self._mapping.get(sensor_ref)


class TestConnect:
    async def test_connect_configures_url_ssl_and_topology(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        object.__setattr__(settings, "AMQP_USE_TLS", False)
        object.__setattr__(settings, "TLS_CHECK_HOSTNAME", False)
        object.__setattr__(settings, "AMQP_HEARTBEAT_S", 20)

        opcua = cast(OpcuaSubscriber, _FakeOpcuaClient())
        sub = AmqpControlSubscriber(opcua, asyncio.Event())

        ssl_context = Mock()
        create_context_mock = Mock(return_value=ssl_context)
        monkeypatch.setattr(
            "app.control_plane.amqp_subscriber.ssl.create_default_context",
            create_context_mock,
        )

        queue = AsyncMock()
        exchange = AsyncMock()
        channel = AsyncMock()
        channel.declare_exchange = AsyncMock(return_value=exchange)
        channel.declare_queue = AsyncMock(return_value=queue)

        connection = AsyncMock()
        connection.channel = AsyncMock(return_value=channel)

        robust_connection_ctor = Mock(return_value=connection)
        monkeypatch.setattr(
            "app.control_plane.amqp_subscriber.aio_pika.RobustConnection",
            robust_connection_ctor,
        )

        await sub._connect()

        create_context_mock.assert_called_once_with(
            purpose=ssl.Purpose.SERVER_AUTH,
            cafile=settings.TLS_CA_CERT_PATH,
        )
        ssl_context.load_cert_chain.assert_called_once_with(
            certfile=settings.TLS_CLIENT_CERT_PATH,
            keyfile=settings.TLS_CLIENT_KEY_PATH,
        )
        assert ssl_context.check_hostname is False

        url_arg = robust_connection_ctor.call_args.args[0]
        assert "auth_mechanism=EXTERNAL" in str(url_arg)
        assert "auth=EXTERNAL" in str(url_arg)
        assert "heartbeat=20" in str(url_arg)

        channel.declare_exchange.assert_awaited_once_with(
            settings.AMQP_CONTROL_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        channel.declare_queue.assert_awaited_once_with(
            f"control_queue_{settings.organization_id}",
            durable=True,
        )
        queue.bind.assert_awaited_once_with(
            exchange,
            routing_key=f"control.{settings.organization_id}",
        )


class TestCommandHandling:
    async def test_handle_message_writes_oneof_target_value(
        self, settings: Settings
    ) -> None:
        object.__setattr__(settings, "AMQP_USE_TLS", False)
        opcua = cast(OpcuaSubscriber, _FakeOpcuaClient())
        sub = AmqpControlSubscriber(opcua, asyncio.Event())

        control_command_ctor: Callable[..., Any] = getattr(
            telemetry_pb2, "ControlCommand"
        )
        cmd = control_command_ctor(
            command_id="cmd-1",
            sensor_id="ns=2;s=Target_Temperature",
            timestamp=1,
            float_val=61.5,
        )
        msg = cast(
            aio_pika.abc.AbstractIncomingMessage,
            SimpleNamespace(body=cmd.SerializeToString()),
        )

        await sub._handle_message(msg)

        cast(_FakeOpcuaClient, opcua).write_node_value.assert_awaited_once_with(
            "ns=2;s=Target_Temperature",
            61.5,
        )

    async def test_handle_message_resolves_uuid_sensor_id(
        self, settings: Settings
    ) -> None:
        object.__setattr__(settings, "AMQP_USE_TLS", False)
        fake_client = _FakeOpcuaClient()
        sensor_uuid = "019d4ada-f02b-71b3-85e6-13f30a9836d5"
        fake_client._mapping[sensor_uuid] = "ns=2;s=Target_Temperature"
        opcua = cast(OpcuaSubscriber, fake_client)
        sub = AmqpControlSubscriber(opcua, asyncio.Event())

        control_command_ctor: Callable[..., Any] = getattr(
            telemetry_pb2, "ControlCommand"
        )
        cmd = control_command_ctor(
            command_id="cmd-uuid",
            sensor_id=sensor_uuid,
            timestamp=3,
            float_val=44.0,
        )
        msg = cast(
            aio_pika.abc.AbstractIncomingMessage,
            SimpleNamespace(body=cmd.SerializeToString()),
        )

        await sub._handle_message(msg)

        fake_client.write_node_value.assert_awaited_once_with(
            "ns=2;s=Target_Temperature",
            44.0,
        )

    async def test_handle_message_skips_when_sensor_id_unresolved(
        self, settings: Settings
    ) -> None:
        object.__setattr__(settings, "AMQP_USE_TLS", False)
        fake_client = _FakeOpcuaClient()
        opcua = cast(OpcuaSubscriber, fake_client)
        sub = AmqpControlSubscriber(opcua, asyncio.Event())

        control_command_ctor: Callable[..., Any] = getattr(
            telemetry_pb2, "ControlCommand"
        )
        cmd = control_command_ctor(
            command_id="cmd-unresolved",
            sensor_id="019d4ada-f02b-71b3-85e6-13f30a9836d5",
            timestamp=4,
            float_val=44.0,
        )
        msg = cast(
            aio_pika.abc.AbstractIncomingMessage,
            SimpleNamespace(body=cmd.SerializeToString()),
        )

        await sub._handle_message(msg)

        fake_client.write_node_value.assert_not_awaited()

    async def test_handle_message_skips_when_oneof_missing(
        self, settings: Settings
    ) -> None:
        object.__setattr__(settings, "AMQP_USE_TLS", False)
        opcua = cast(OpcuaSubscriber, _FakeOpcuaClient())
        sub = AmqpControlSubscriber(opcua, asyncio.Event())

        control_command_ctor: Callable[..., Any] = getattr(
            telemetry_pb2, "ControlCommand"
        )
        cmd = control_command_ctor(
            command_id="cmd-2",
            sensor_id="ns=2;s=Target_Temperature",
            timestamp=2,
        )
        msg = cast(
            aio_pika.abc.AbstractIncomingMessage,
            SimpleNamespace(body=cmd.SerializeToString()),
        )

        await sub._handle_message(msg)

        cast(_FakeOpcuaClient, opcua).write_node_value.assert_not_awaited()

    async def test_handle_message_drops_command_when_write_fails(
        self, settings: Settings
    ) -> None:
        object.__setattr__(settings, "AMQP_USE_TLS", False)
        fake_client = _FakeOpcuaClient()
        fake_client.write_node_value = AsyncMock(side_effect=ValueError("bad type"))
        opcua = cast(OpcuaSubscriber, fake_client)
        sub = AmqpControlSubscriber(opcua, asyncio.Event())

        control_command_ctor: Callable[..., Any] = getattr(
            telemetry_pb2, "ControlCommand"
        )
        cmd = control_command_ctor(
            command_id="cmd-type-mismatch",
            sensor_id="ns=2;s=Target_Temperature",
            timestamp=5,
            str_val="1500",
        )
        msg = cast(
            aio_pika.abc.AbstractIncomingMessage,
            SimpleNamespace(body=cmd.SerializeToString()),
        )

        await sub._handle_message(msg)

        fake_client.write_node_value.assert_awaited_once()
