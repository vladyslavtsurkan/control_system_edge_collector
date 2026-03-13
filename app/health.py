"""Lightweight ``/healthz`` HTTP endpoint for container orchestrators."""

import json
from typing import TYPE_CHECKING

from aiohttp import web

import structlog

if TYPE_CHECKING:
    from app.data_plane.amqp_publisher import AmqpPublisher
    from app.data_plane.opcua_subscriber import OpcuaSubscriber

__all__ = ["HealthServer"]

log = structlog.get_logger()


class HealthServer:
    """Tiny aiohttp server exposing ``GET /healthz``."""

    def __init__(
        self,
        host: str,
        port: int,
        subscriber: "OpcuaSubscriber",
        publisher: "AmqpPublisher",
    ) -> None:
        self._host = host
        self._port = port
        self._subscriber = subscriber
        self._publisher = publisher
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/healthz", self._handle_healthz)

        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        log.info(
            "health endpoint started",
            host=self._host,
            port=self._port,
        )

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            log.info("health endpoint stopped")

    async def _handle_healthz(self, _request: web.Request) -> web.Response:
        opcua_ok = self._subscriber.is_connected
        amqp_ok = self._publisher.is_connected
        healthy = opcua_ok and amqp_ok
        body = {
            "status": "healthy" if healthy else "degraded",
            "opcua_connected": opcua_ok,
            "amqp_connected": amqp_ok,
        }
        return web.Response(
            status=200 if healthy else 503,
            body=json.dumps(body),
            content_type="application/json",
        )
