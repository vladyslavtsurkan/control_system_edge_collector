"""AMQP batch publisher using *aio_pika* with store-and-forward."""

import asyncio
import json

import aio_pika
import structlog

from app.models.telemetry import TelemetryPayload
from app.settings import get_settings
from app.utils.backoff import retry_with_backoff

__all__ = ["AmqpPublisher"]

log = structlog.get_logger()


class AmqpPublisher:
    """Reads :class:`TelemetryPayload` items from an :class:`asyncio.Queue`,
    batches them, and publishes JSON arrays to a RabbitMQ exchange.

    **Store-and-forward**: if the AMQP connection drops, the OPC UA
    subscriber keeps filling the queue (up to its *maxsize*).  This
    publisher reconnects with exponential back-off and drains the queue
    once the connection is restored.
    """

    def __init__(
        self,
        queue: asyncio.Queue[TelemetryPayload],
        shutdown_event: asyncio.Event,
    ) -> None:
        self._settings = get_settings()
        self._queue = queue
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

    # Batch collection

    async def _collect_batch(self) -> list[TelemetryPayload]:
        """Collect up to ``BATCH_SIZE`` items **or** wait up to
        ``BATCH_TIMEOUT_S`` – whichever comes first.
        """
        batch: list[TelemetryPayload] = []
        deadline = asyncio.get_event_loop().time() + self._settings.BATCH_TIMEOUT_S

        while len(batch) < self._settings.BATCH_SIZE:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                item = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=remaining,
                )
                batch.append(item)
            except TimeoutError:
                break

        return batch

    # Publishing

    async def _publish_batch(self, batch: list[TelemetryPayload]) -> None:
        """Serialise *batch* as a JSON array and publish to the exchange."""
        body = json.dumps(
            [p.model_dump(mode="json") for p in batch],
        ).encode()

        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )

        assert self._exchange is not None  # guaranteed after _ensure_connected
        await self._exchange.publish(message, routing_key="telemetry")

        log.debug("batch published", size=len(batch), bytes=len(body))

    # ── Main loop ───────────────────────────────────────────────────

    async def run(self) -> None:
        """Run the publisher loop until the shutdown event is set and
        the queue is drained.
        """
        await self._ensure_connected()

        while not self._shutdown.is_set():
            batch = await self._collect_batch()
            if not batch:
                continue
            try:
                await self._publish_batch(batch)
            except Exception:
                log.exception(
                    "AMQP publish failed – will reconnect",
                    batch_size=len(batch),
                )
                # Put items back so they aren't lost.
                for item in batch:
                    try:
                        self._queue.put_nowait(item)
                    except asyncio.QueueFull:
                        log.warning("queue full while re-enqueuing after AMQP failure")
                        break
                self._connected = False
                await self._ensure_connected()

        # Flush remaining items on shutdown
        await self._flush()

    async def _flush(self) -> None:
        """Drain whatever is left in the queue and publish a final batch."""
        remaining: list[TelemetryPayload] = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if remaining:
            try:
                await self._ensure_connected()
                # Publish in chunks of BATCH_SIZE
                for i in range(0, len(remaining), self._settings.BATCH_SIZE):
                    chunk = remaining[i : i + self._settings.BATCH_SIZE]
                    await self._publish_batch(chunk)
                log.info("flushed remaining telemetry", count=len(remaining))
            except Exception:
                log.exception(
                    "failed to flush remaining telemetry on shutdown",
                    lost_count=len(remaining),
                )

    async def close(self) -> None:
        """Close channel and connection."""
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._connected = False
        log.info("AMQP connection closed")
