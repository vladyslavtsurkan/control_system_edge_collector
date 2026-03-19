"""Edge Collector Service – main orchestrator.

Lifecycle
---------
1. Load settings from environment / ``.env``.
2. Fetch OPC UA collector configuration from the Cloud API.
3. Refresh OPC UA collector configuration periodically and apply changes.
4. Start OPC UA subscriptions  →  persistent SQLite edge buffer.
5. Start AMQP batch publisher  ←  persistent SQLite edge buffer.
6. Start ``/healthz`` HTTP endpoint.
7. Wait for ``SIGINT`` / ``SIGTERM``.
8. Graceful shutdown: stop OPC UA, drain buffer, close AMQP.
"""

import asyncio
import signal
import sys

import structlog

from app.control_plane.api_client import fetch_collector_config
from app.control_plane.config_refresh import config_refresh_loop
from app.data_plane.amqp_publisher import AmqpPublisher
from app.data_plane.opcua_subscriber import OpcuaSubscriber
from app.data_plane.persistent_buffer import PersistentEdgeBuffer
from app.health import HealthServer
from app.logging import setup_logging
from app.settings import get_settings

log = structlog.get_logger()


async def main() -> None:
    # Setup logging first so we get logs from the start.
    setup_logging()

    settings = get_settings()
    log.info(
        "edge collector starting",
        cloud_api=str(settings.CLOUD_API_URL),
        amqp_exchange=settings.AMQP_EXCHANGE,
    )

    # Fetch OPC UA config (Control Plane)
    config = await fetch_collector_config()

    # Shared resources
    buffer = PersistentEdgeBuffer(settings.EDGE_BUFFER_DB_PATH)
    await buffer.initialize()
    shutdown_event = asyncio.Event()

    # Instantiate components
    subscriber = OpcuaSubscriber(config, buffer)
    publisher = AmqpPublisher(buffer, shutdown_event)
    health = HealthServer(
        settings.HEALTH_HOST,
        settings.HEALTH_PORT,
        subscriber,
        publisher,
    )

    # Signal handling
    def _request_shutdown(sign: signal.Signals) -> None:
        log.info("shutdown signal received", signal=sign.name)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    # Start components
    try:
        await subscriber.start()
    except Exception:
        log.exception("failed to start OPC UA subscriber")
        sys.exit(1)

    await health.start()

    config_refresh_task = asyncio.create_task(
        config_refresh_loop(subscriber, shutdown_event, config),
        name="config-refresh",
    )
    publisher_task = asyncio.create_task(publisher.run(), name="amqp-publisher")
    log.info("edge collector running – press Ctrl+C to stop")

    # Wait for shutdown
    await shutdown_event.wait()

    # Graceful teardown
    log.info("shutting down …")

    config_refresh_task.cancel()
    try:
        await config_refresh_task
    except asyncio.CancelledError:
        pass

    await subscriber.stop()

    # Give the publisher a moment to flush, then cancel if stuck.
    try:
        await asyncio.wait_for(publisher_task, timeout=10.0)
    except TimeoutError:
        log.warning("publisher flush timed out – cancelling")
        publisher_task.cancel()
        try:
            await publisher_task
        except asyncio.CancelledError:
            pass

    await publisher.close()
    await buffer.close()
    await health.stop()
    log.info("edge collector stopped")


if __name__ == "__main__":
    asyncio.run(main())
