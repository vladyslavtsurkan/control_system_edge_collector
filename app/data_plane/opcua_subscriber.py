"""OPC UA subscription handler using *asyncua*."""

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import structlog
from asyncua import Client, ua
from asyncua.common.subscription import DataChangeNotif
from asyncua.crypto.security_policies import (
    SecurityPolicy,
    SecurityPolicyAes128Sha256RsaOaep,
    SecurityPolicyAes256Sha256RsaPss,
    SecurityPolicyBasic256Sha256,
    SecurityPolicyNone,
)

from app.models.config import CollectorConfig
from app.models.telemetry import TelemetryPayload, TelemetryValue
from app.settings import get_settings

__all__ = ["OpcuaSubscriber"]

log = structlog.get_logger()

# ecurity-policy lookup
# Maps the human-readable string returned by the Cloud API to the
# asyncua security-policy *class* expected by ``Client.set_security()``.
SECURITY_POLICY_MAP: dict[str, type[SecurityPolicy]] = {
    "None": SecurityPolicyNone,
    "Basic256Sha256": SecurityPolicyBasic256Sha256,
    "Aes128_Sha256_RsaOaep": SecurityPolicyAes128Sha256RsaOaep,
    "Aes256_Sha256_RsaPss": SecurityPolicyAes256Sha256RsaPss,
}

# Maps policy names to asyncua MessageSecurityMode.
SECURITY_MODE_MAP: dict[str, ua.MessageSecurityMode] = {
    "None": ua.MessageSecurityMode.None_,
    "Basic256Sha256": ua.MessageSecurityMode.SignAndEncrypt,
    "Aes128_Sha256_RsaOaep": ua.MessageSecurityMode.SignAndEncrypt,
    "Aes256_Sha256_RsaPss": ua.MessageSecurityMode.SignAndEncrypt,
}


class _DataChangeHandler:
    """asyncua subscription callback handler.

    Receives ``datachange_notification`` events and pushes
    :class:`TelemetryPayload` instances onto the internal queue.
    """

    def __init__(
        self,
        node_to_sensor: dict[str, UUID],
        queue: asyncio.Queue[TelemetryPayload],
    ) -> None:
        self._node_to_sensor = node_to_sensor
        self._queue = queue

    def datachange_notification(
        self,
        node: Any,
        val: Any,
        data: DataChangeNotif,
    ) -> None:
        node_id_str = node.nodeid.to_string()
        sensor_id = self._node_to_sensor.get(node_id_str)
        if sensor_id is None:
            log.warning("unmapped node change", node_id=node_id_str)
            return

        # Extract OPC UA StatusCode as a human-readable string.
        status_code = data.monitored_item.Value.StatusCode
        status_str = (
            status_code.name if hasattr(status_code, "name") else str(status_code)
        )

        payload = TelemetryPayload(
            sensor_id=sensor_id,
            time=datetime.now(UTC),
            payload=TelemetryValue(value=val, status=status_str),
        )

        try:
            self._queue.put_nowait(payload)
        except asyncio.QueueFull:
            log.warning(
                "internal queue full – dropping telemetry point",
                sensor_id=str(sensor_id),
                node_id=node_id_str,
            )


class OpcuaSubscriber:
    """Connects to an OPC UA server and creates data-change subscriptions
    for every sensor in the collector configuration.
    """

    def __init__(
        self,
        config: CollectorConfig,
        queue: asyncio.Queue[TelemetryPayload],
    ) -> None:
        self._config = config
        self._settings = get_settings()
        self._queue = queue
        self._client: Client | None = None
        self._subscription: Any | None = None
        self._handles: list[Any] = []
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        """Connect to OPC UA, configure security and create subscriptions."""
        cfg = self._config

        self._client = Client(url=cfg.url)

        # ── Security ────────────────────────────────────
        policy_name = cfg.security_policy
        if policy_name != "None":
            policy_cls = SECURITY_POLICY_MAP.get(policy_name)
            if policy_cls is None:
                raise ValueError(f"Unsupported OPC UA security policy: {policy_name!r}")

            cert_path = self._settings.OPCUA_CERT_PATH
            key_path = self._settings.OPCUA_KEY_PATH
            if not cert_path or not key_path:
                raise ValueError(
                    f"OPCUA_CERT_PATH and OPCUA_KEY_PATH are required for "
                    f"security policy {policy_name!r}"
                )

            mode = SECURITY_MODE_MAP[policy_name]
            await self._client.set_security(
                policy=policy_cls,
                certificate=str(cert_path),
                private_key=str(key_path),
                mode=mode,
            )
            log.info(
                "OPC UA security configured",
                policy=policy_name,
                mode=mode.name,
            )

        # Authentication
        if cfg.authentication_method == "username_password":
            self._client.set_user(cfg.username)
            self._client.set_password(cfg.password)
            log.info("OPC UA auth: username/password")
        else:
            log.info("OPC UA auth: anonymous")

        # Connect
        await self._client.connect()
        self._connected = True
        log.info("OPC UA connected", url=cfg.url)

        # Build node→sensor lookup
        node_to_sensor: dict[str, UUID] = {s.node_id: s.id for s in cfg.sensors}

        # Subscribe
        handler = _DataChangeHandler(node_to_sensor, self._queue)
        self._subscription = await self._client.create_subscription(
            period=100,  # ms – server may negotiate a different value
            handler=handler,
        )

        nodes = [self._client.get_node(nid) for nid in node_to_sensor]
        self._handles = await self._subscription.subscribe_data_change(nodes)
        log.info(
            "OPC UA subscriptions created",
            node_count=len(self._handles),
        )

    async def stop(self) -> None:
        """Unsubscribe and disconnect cleanly."""
        if self._subscription and self._handles:
            try:
                await self._subscription.unsubscribe(self._handles)
                await self._subscription.delete()
            except Exception:
                log.exception("error unsubscribing from OPC UA")
            self._handles.clear()
            self._subscription = None

        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                log.exception("error disconnecting OPC UA client")
            self._connected = False
            log.info("OPC UA disconnected")
