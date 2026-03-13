"""Tests for the AMQP batch publisher (unit-level, no real RabbitMQ)."""

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from app.data_plane.amqp_publisher import AmqpPublisher
from app.models.telemetry import TelemetryPayload, TelemetryValue
from app.settings import Settings


def _make_payload() -> TelemetryPayload:
    return TelemetryPayload(
        sensor_id=uuid4(),
        time=datetime.now(UTC),
        payload=TelemetryValue(value=42.0, status="Good"),
    )


class TestCollectBatch:
    async def test_collects_up_to_batch_size(self, settings: Settings) -> None:
        q: asyncio.Queue[TelemetryPayload] = asyncio.Queue(maxsize=200)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(q, shutdown)

        # Override for fast test
        object.__setattr__(settings, "BATCH_SIZE", 5)
        object.__setattr__(settings, "BATCH_TIMEOUT_S", 5.0)

        for _ in range(10):
            await q.put(_make_payload())

        batch = await pub._collect_batch()
        assert len(batch) == 5

    async def test_collects_partial_on_timeout(self, settings: Settings) -> None:
        q: asyncio.Queue[TelemetryPayload] = asyncio.Queue(maxsize=200)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(q, shutdown)

        object.__setattr__(settings, "BATCH_SIZE", 100)
        object.__setattr__(settings, "BATCH_TIMEOUT_S", 0.05)

        await q.put(_make_payload())
        await q.put(_make_payload())

        batch = await pub._collect_batch()
        assert len(batch) == 2

    async def test_empty_queue_returns_empty(self, settings: Settings) -> None:
        q: asyncio.Queue[TelemetryPayload] = asyncio.Queue(maxsize=200)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(q, shutdown)

        object.__setattr__(settings, "BATCH_SIZE", 100)
        object.__setattr__(settings, "BATCH_TIMEOUT_S", 0.05)

        batch = await pub._collect_batch()
        assert len(batch) == 0


class TestPublishBatch:
    async def test_publish_serialises_json_array(self, settings: Settings) -> None:
        q: asyncio.Queue[TelemetryPayload] = asyncio.Queue(maxsize=200)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(q, shutdown)

        mock_exchange = AsyncMock()
        pub._exchange = mock_exchange

        batch = [_make_payload(), _make_payload()]
        await pub._publish_batch(batch)

        mock_exchange.publish.assert_awaited_once()
        call_args = mock_exchange.publish.call_args
        message = call_args[0][0]
        parsed = json.loads(message.body)
        assert isinstance(parsed, list)
        assert len(parsed) == 2
        assert parsed[0]["payload"]["value"] == 42.0


class TestFlush:
    async def test_flush_drains_queue(self, settings: Settings) -> None:
        q: asyncio.Queue[TelemetryPayload] = asyncio.Queue(maxsize=200)
        shutdown = asyncio.Event()
        pub = AmqpPublisher(q, shutdown)

        for _ in range(3):
            await q.put(_make_payload())

        mock_exchange = AsyncMock()
        pub._exchange = mock_exchange
        pub._connected = True
        pub._connection = MagicMock()
        pub._connection.is_closed = False

        await pub._flush()

        assert q.empty()
        mock_exchange.publish.assert_awaited_once()
