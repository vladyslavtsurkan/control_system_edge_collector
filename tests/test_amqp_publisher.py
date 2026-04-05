"""Tests for the AMQP batch publisher (unit-level, no real RabbitMQ)."""

import asyncio
import ssl
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import aio_pika
import pytest

from app.data_plane.amqp_publisher import AmqpPublisher
from app.data_plane.persistent_buffer import PersistentEdgeBuffer
from app.generated import telemetry_pb2
from app.settings import Settings


def _make_record(record_id: int) -> dict:
    return {
        "id": record_id,
        "payload": {
            "sensor_id": "11111111-1111-1111-1111-111111111111",
            "time": int(datetime(2026, 1, 1, 0, 0, 0).timestamp() * 1000),
            "payload": {"value": 42.0, "status": "Good"},
        },
        "created_at": "2026-01-01 00:00:00",
    }


class TestPublishBatch:
    async def test_publish_serialises_protobuf_batch(self, settings: Settings) -> None:
        buffer = AsyncMock(spec=PersistentEdgeBuffer)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(buffer, shutdown)
        object.__setattr__(settings, "AMQP_USE_TLS", False)

        mock_exchange = AsyncMock()
        pub._exchange = mock_exchange

        batch = [_make_record(1)["payload"], _make_record(2)["payload"]]
        await pub._publish_batch(batch)

        mock_exchange.publish.assert_awaited_once()
        call_args = mock_exchange.publish.call_args
        message = call_args[0][0]

        parsed = telemetry_pb2.TelemetryBatch()
        parsed.ParseFromString(message.body)

        assert len(parsed.readings) == 2
        assert parsed.readings[0].payload.float_val == 42.0
        assert message.content_type == "application/x-protobuf"
        assert message.user_id == settings.organization_id
        assert call_args.kwargs["routing_key"] == "telemetry"


class TestRunLoop:
    async def test_commits_after_successful_publish(self, settings: Settings) -> None:
        buffer = AsyncMock(spec=PersistentEdgeBuffer)
        shutdown = asyncio.Event()
        shutdown.set()
        pub = AmqpPublisher(buffer, shutdown)

        object.__setattr__(settings, "BATCH_SIZE", 10)

        first_batch = [_make_record(1), _make_record(2)]
        buffer.get_batch = AsyncMock(side_effect=[first_batch, []])
        buffer.commit = AsyncMock()
        pub._ensure_connected = AsyncMock()
        pub._publish_batch = AsyncMock()

        await pub.run()

        pub._publish_batch.assert_awaited_once_with(
            [first_batch[0]["payload"], first_batch[1]["payload"]]
        )
        buffer.commit.assert_awaited_once_with([1, 2])

    async def test_connection_error_does_not_commit(self, settings: Settings) -> None:
        buffer = AsyncMock(spec=PersistentEdgeBuffer)
        shutdown = asyncio.Event()
        shutdown.set()
        pub = AmqpPublisher(buffer, shutdown)

        object.__setattr__(settings, "BATCH_SIZE", 10)
        object.__setattr__(settings, "BACKOFF_BASE_S", 0.0)

        first_batch = [_make_record(1)]
        buffer.get_batch = AsyncMock(side_effect=[first_batch, []])
        buffer.commit = AsyncMock()
        pub._ensure_connected = AsyncMock()
        pub._publish_batch = AsyncMock(
            side_effect=aio_pika.exceptions.AMQPConnectionError("down")
        )

        await pub.run()

        buffer.commit.assert_not_awaited()

    async def test_waits_when_buffer_is_empty(self, settings: Settings) -> None:
        buffer = AsyncMock(spec=PersistentEdgeBuffer)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(buffer, shutdown)

        object.__setattr__(settings, "BATCH_TIMEOUT_S", 0.0)

        async def _get_batch(_size: int) -> list[dict]:
            shutdown.set()
            return []

        buffer.get_batch = AsyncMock(side_effect=_get_batch)
        pub._ensure_connected = AsyncMock()
        pub._publish_batch = AsyncMock()

        await pub.run()

        pub._publish_batch.assert_not_awaited()


class TestConnect:
    async def test_connect_builds_ssl_context_and_connects(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        buffer = AsyncMock(spec=PersistentEdgeBuffer)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(buffer, shutdown)

        object.__setattr__(settings, "TLS_CA_CERT_PATH", "/tmp/ca.pem")
        object.__setattr__(settings, "TLS_CLIENT_CERT_PATH", "/tmp/client.pem")
        object.__setattr__(settings, "TLS_CLIENT_KEY_PATH", "/tmp/client.key")
        object.__setattr__(settings, "TLS_CHECK_HOSTNAME", False)

        ssl_context = Mock()
        create_context_mock = Mock(return_value=ssl_context)
        monkeypatch.setattr(
            "app.data_plane.amqp_publisher.ssl.create_default_context",
            create_context_mock,
        )

        connection = AsyncMock()
        channel = AsyncMock()
        connection.channel = AsyncMock(return_value=channel)
        robust_connection_ctor = Mock(return_value=connection)
        monkeypatch.setattr(
            "app.data_plane.amqp_publisher.aio_pika.RobustConnection",
            robust_connection_ctor,
        )

        await pub._connect()

        create_context_mock.assert_called_once_with(
            purpose=ssl.Purpose.SERVER_AUTH,
            cafile=settings.TLS_CA_CERT_PATH,
        )
        ssl_context.load_cert_chain.assert_called_once_with(
            certfile=settings.TLS_CLIENT_CERT_PATH,
            keyfile=settings.TLS_CLIENT_KEY_PATH,
        )
        assert ssl_context.check_hostname is False

        robust_connection_ctor.assert_called_once()
        assert robust_connection_ctor.call_args.kwargs["ssl_context"] is ssl_context
        connection.connect.assert_awaited_once()

    async def test_connect_builds_ssl_context_when_tls_enabled(
        self,
        settings: Settings,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        buffer = AsyncMock(spec=PersistentEdgeBuffer)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(buffer, shutdown)

        object.__setattr__(settings, "TLS_CHECK_HOSTNAME", False)
        object.__setattr__(settings, "TLS_CA_CERT_PATH", "/tmp/ca.pem")
        object.__setattr__(settings, "TLS_CLIENT_CERT_PATH", "/tmp/client.pem")
        object.__setattr__(settings, "TLS_CLIENT_KEY_PATH", "/tmp/client.key")
        object.__setattr__(settings, "AMQP_HEARTBEAT_S", 12)

        ssl_context = Mock()
        create_context_mock = Mock(return_value=ssl_context)
        monkeypatch.setattr(
            "app.data_plane.amqp_publisher.ssl.create_default_context",
            create_context_mock,
        )

        connection = AsyncMock()
        channel = AsyncMock()
        connection.channel = AsyncMock(return_value=channel)
        robust_connection_ctor = Mock(return_value=connection)
        monkeypatch.setattr(
            "app.data_plane.amqp_publisher.aio_pika.RobustConnection",
            robust_connection_ctor,
        )

        await pub._connect()

        create_context_mock.assert_called_once_with(
            purpose=ssl.Purpose.SERVER_AUTH,
            cafile=settings.TLS_CA_CERT_PATH,
        )
        ssl_context.load_cert_chain.assert_called_once_with(
            certfile=settings.TLS_CLIENT_CERT_PATH,
            keyfile=settings.TLS_CLIENT_KEY_PATH,
        )
        assert ssl_context.check_hostname is False
        assert robust_connection_ctor.call_args.kwargs["ssl_context"] is ssl_context

        url_arg = robust_connection_ctor.call_args.args[0]
        assert "auth_mechanism=EXTERNAL" in str(url_arg)
        assert "auth=EXTERNAL" in str(url_arg)
        assert "heartbeat=12" in str(url_arg)
