"""AMQP batch publisher using *aio_pika* with store-and-forward."""

import asyncio
from typing import Any

import aio_pika
import structlog

from app.data_plane.persistent_buffer import PersistentEdgeBuffer
from app.generated import telemetry_pb2
from app.settings import get_settings
from app.utils.backoff import retry_with_backoff

__all__ = ["AmqpPublisher"]

log = structlog.get_logger()


class AmqpPublisher:
    """Reads telemetry payloads from :class:`PersistentEdgeBuffer`, batches
    them, and publishes JSON arrays to a RabbitMQ exchange.

    **Store-and-forward**: if the AMQP connection drops, the OPC UA
    subscriber keeps filling the SQLite buffer. This
    publisher reconnects with exponential back-off and drains the buffer
    once the connection is restored.
    """

    def __init__(
        self,
        buffer: PersistentEdgeBuffer,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._settings = get_settings()
        self._buffer = buffer
        self._shutdown = shutdown_event
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    # Connection management

    async def _connect(self) -> None:
        """Establish (or re-establish) the AMQP connection, channel,
        and exchange with exponential back-off.
        """

        async def _do_connect() -> None:
            amqp_url = str(self._settings.AMQP_URL)
            log.info("connecting to AMQP", url=amqp_url)
            self._connection = await aio_pika.connect_robust(amqp_url)
            self._channel = await self._connection.channel()
            self._exchange = await self._channel.declare_exchange(
                self._settings.AMQP_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
            self._connected = True
            log.info("AMQP connected", exchange=self._settings.AMQP_EXCHANGE)

        await retry_with_backoff(
            _do_connect,
            max_retries=self._settings.BACKOFF_MAX_RETRIES,
            base_delay=self._settings.BACKOFF_BASE_S,
            max_delay=self._settings.BACKOFF_MAX_S,
            operation_name="amqp_connect",
        )

    async def _ensure_connected(self) -> None:
        if (
            not self._connected
            or self._connection is None
            or self._connection.is_closed
        ):
            self._connected = False
            await self._connect()

    @staticmethod
    def build_protobuf_batch(payloads: list[dict[str, Any]]) -> bytes:
        """Convert list of dicts (from SQLite) into compressed Protobuf bytes"""
        batch_pb = telemetry_pb2.TelemetryBatch()

        for data in payloads:
            reading_pb = batch_pb.readings.add()
            reading_pb.sensor_id = str(data["sensor_id"])
            reading_pb.time = int(data["time"])
            reading_pb.payload.status = data["payload"]["status"]

            # Distribution of values using oneof
            val = data["payload"]["value"]
            if isinstance(val, bool):
                reading_pb.payload.bool_val = val
            elif isinstance(val, int):
                reading_pb.payload.int_val = val
            elif isinstance(val, float):
                reading_pb.payload.float_val = val
            elif isinstance(val, str):
                reading_pb.payload.str_val = val

        return batch_pb.SerializeToString()

    async def _publish_batch(self, batch: list[dict[str, Any]]) -> None:
        """Serialize *batch* as a JSON array and publish to the exchange."""
        protobuf_bytes = self.build_protobuf_batch(batch)

        message = aio_pika.Message(
            body=protobuf_bytes,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/x-protobuf",
        )

        assert self._exchange is not None  # guaranteed after _ensure_connected
        await self._exchange.publish(message, routing_key="telemetry")

        log.debug("batch published", size=len(batch), bytes=len(batch))

    async def run(self) -> None:
        """Get the oldest batches from the buffer, publish, then commit on success."""
        while True:
            records = await self._buffer.get_batch(self._settings.BATCH_SIZE)
            if not records:
                if self._shutdown.is_set():
                    break
                await asyncio.sleep(self._settings.BATCH_TIMEOUT_S)
                continue

            ids = [int(record["id"]) for record in records]
            payloads = [record["payload"] for record in records]

            try:
                await self._ensure_connected()
                await self._publish_batch(payloads)
                await self._buffer.commit(ids)
            except aio_pika.exceptions.CONNECTION_EXCEPTIONS:
                log.exception(
                    "AMQP connection error - retaining batch for retry",
                    batch_size=len(payloads),
                )
                self._connected = False
                await asyncio.sleep(self._settings.BACKOFF_BASE_S)
            except Exception:
                log.exception(
                    "AMQP publish failed - retaining batch for retry",
                    batch_size=len(payloads),
                )
                await asyncio.sleep(self._settings.BACKOFF_BASE_S)

        log.info("publisher loop stopped - persistent buffer drained")

    async def close(self) -> None:
        """Close channel and connection."""
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._connected = False
        log.info("AMQP connection closed")
