"""AMQP control-command subscriber that writes commands to OPC UA."""

import asyncio
import ssl
from typing import Any
from collections.abc import Callable

import aio_pika
import structlog
from yarl import URL

from app.data_plane.opcua_subscriber import OpcuaSubscriber
from app.generated import telemetry_pb2
from app.settings import get_settings
from app.utils.backoff import retry_with_backoff

__all__ = ["AmqpControlSubscriber"]

log = structlog.get_logger()


class AmqpControlSubscriber:
    """Consumes ControlCommand messages and writes target values to OPC UA."""

    def __init__(
        self,
        opcua_client: OpcuaSubscriber,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._settings = get_settings()
        self._opcua_client = opcua_client
        self._shutdown = shutdown_event
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None
        self._queue: aio_pika.abc.AbstractQueue | None = None
        self._connected = False

    async def _connect(self) -> None:
        """Establish (or re-establish) AMQP connection, topology, and queue."""

        async def _do_connect() -> None:
            ssl_context = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=self._settings.TLS_CA_CERT_PATH,
            )
            ssl_context.load_cert_chain(
                certfile=self._settings.TLS_CLIENT_CERT_PATH,
                keyfile=self._settings.TLS_CLIENT_KEY_PATH,
            )
            ssl_context.check_hostname = self._settings.TLS_CHECK_HOSTNAME

            base_url = str(self._settings.AMQP_URL).replace("guest:guest@", "")
            url = (
                URL(base_url)
                .with_user("")
                .with_password("")
                .with_query(
                    {
                        "auth_mechanism": "EXTERNAL",
                        "auth": "EXTERNAL",
                        "heartbeat": str(self._settings.AMQP_HEARTBEAT_S),
                    }
                )
            )

            log.info("connecting control subscriber to AMQP via mTLS", url=str(url))
            self._connection = aio_pika.RobustConnection(url, ssl_context=ssl_context)
            await self._connection.connect()

            self._channel = await self._connection.channel()
            self._exchange = await self._channel.declare_exchange(
                self._settings.AMQP_CONTROL_EXCHANGE,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )

            queue_name = f"control_queue_{self._settings.organization_id}"
            self._queue = await self._channel.declare_queue(queue_name, durable=True)
            await self._queue.bind(
                self._exchange,
                routing_key=f"control.{self._settings.organization_id}",
            )
            self._connected = True
            log.info(
                "control AMQP connected",
                exchange=self._settings.AMQP_CONTROL_EXCHANGE,
                queue=queue_name,
            )

        await retry_with_backoff(
            _do_connect,
            max_retries=self._settings.BACKOFF_MAX_RETRIES,
            base_delay=self._settings.BACKOFF_BASE_S,
            max_delay=self._settings.BACKOFF_MAX_S,
            operation_name="amqp_control_connect",
        )

    async def _ensure_connected(self) -> None:
        if (
            not self._connected
            or self._connection is None
            or self._connection.is_closed
            or self._queue is None
        ):
            self._connected = False
            await self._connect()

    @staticmethod
    def _extract_target_value(cmd: Any) -> Any | None:
        field_name = cmd.WhichOneof("target_value")
        if field_name is None:
            return None
        return getattr(cmd, field_name)

    async def _handle_message(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        control_command_ctor: Callable[[], Any] = getattr(
            telemetry_pb2,
            "ControlCommand",
        )
        cmd = control_command_ctor()
        cmd.ParseFromString(message.body)

        target_value = self._extract_target_value(cmd)
        if target_value is None:
            log.warning(
                "control command without target value",
                command_id=cmd.command_id,
                sensor_id=cmd.sensor_id,
            )
            return

        resolved_node_id = self._opcua_client.resolve_control_node_id(cmd.sensor_id)
        if resolved_node_id is None:
            log.warning(
                "control command sensor_id could not be resolved to node_id",
                command_id=cmd.command_id,
                sensor_id=cmd.sensor_id,
            )
            return

        try:
            await self._opcua_client.write_node_value(resolved_node_id, target_value)
        except Exception:
            # Drop malformed/incompatible command payloads after logging.
            log.exception(
                "control command apply failed",
                command_id=cmd.command_id,
                sensor_id=cmd.sensor_id,
                node_id=resolved_node_id,
                target_value=target_value,
            )
            return

        log.info(
            "control command applied",
            command_id=cmd.command_id,
            sensor_id=cmd.sensor_id,
            node_id=resolved_node_id,
            target_value=target_value,
        )

    async def run(self) -> None:
        """Run consumer loop until cancelled by application shutdown."""
        try:
            while not self._shutdown.is_set():
                try:
                    await self._ensure_connected()
                    assert self._queue is not None

                    async with self._queue.iterator() as q_iter:
                        async for message in q_iter:
                            if self._shutdown.is_set():
                                break
                            async with message.process():
                                await self._handle_message(message)
                except aio_pika.exceptions.CONNECTION_EXCEPTIONS:
                    log.exception("control AMQP connection error")
                    self._connected = False
                    await asyncio.sleep(self._settings.BACKOFF_BASE_S)
                except Exception:
                    log.exception("control subscriber loop error")
                    await asyncio.sleep(self._settings.BACKOFF_BASE_S)
        except asyncio.CancelledError:
            log.debug("control subscriber cancelled")
            raise
        finally:
            await self.close()

    async def close(self) -> None:
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        self._queue = None
        self._exchange = None
        self._connected = False
        log.info("control AMQP connection closed")
