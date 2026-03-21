"""Tests for the AMQP batch publisher (unit-level, no real RabbitMQ)."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import aio_pika

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
