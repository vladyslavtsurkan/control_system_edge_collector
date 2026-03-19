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

from app.data_plane.persistent_buffer import PersistentEdgeBuffer
from app.models.config import CollectorConfig
from app.models.telemetry import TelemetryPayload, TelemetryValue
from app.settings import get_settings
from app.utils.backoff import retry_with_backoff

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

    Receives ``datachange_notification`` events and persists
    :class:`TelemetryPayload` instances into SQLite.
    """

    def __init__(
        self,
        node_to_sensor: dict[str, UUID],
        buffer: PersistentEdgeBuffer,
    ) -> None:
        self._node_to_sensor = node_to_sensor
        self._buffer = buffer
        self._pending_tasks: set[asyncio.Task[None]] = set()

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

        task = asyncio.create_task(
            self._buffer.put(payload.model_dump(mode="json")),
            name="persist-telemetry",
        )
        self._pending_tasks.add(task)
        task.add_done_callback(self._on_persist_done)

    async def flush_pending(self) -> None:
        """Wait for scheduled inserts to finish before shutdown."""
        if not self._pending_tasks:
            return
        await asyncio.gather(*tuple(self._pending_tasks), return_exceptions=True)

    def _on_persist_done(self, task: asyncio.Task[None]) -> None:
        self._pending_tasks.discard(task)
        try:
            task.result()
        except Exception:
            log.exception("failed to persist telemetry payload")

    def update_node_map(self, node_to_sensor: dict[str, UUID]) -> None:
        """Replace node->sensor mapping used by data-change callbacks."""
        self._node_to_sensor = node_to_sensor


class OpcuaSubscriber:
    """Connects to an OPC UA server and creates data-change subscriptions
    for every sensor in the collector configuration.
    """

    def __init__(
        self,
        config: CollectorConfig,
        buffer: PersistentEdgeBuffer,
    ) -> None:
        self._config = config
        self._settings = get_settings()
        self._buffer = buffer
        self._client: Client | None = None
        self._subscription: Any | None = None
        self._handler: _DataChangeHandler | None = None
        self._node_to_sensor: dict[str, UUID] = {}
        self._node_handles: dict[str, Any] = {}
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    @staticmethod
    def _build_node_to_sensor(config: CollectorConfig) -> dict[str, UUID]:
        return {sensor.node_id: sensor.id for sensor in config.sensors}

    @staticmethod
    def _requires_reconnect(previous: CollectorConfig, updated: CollectorConfig) -> bool:
        return (
            previous.url != updated.url
            or previous.security_policy != updated.security_policy
            or previous.authentication_method != updated.authentication_method
            or previous.username != updated.username
            or previous.password != updated.password
        )

    async def start(self) -> None:
        """Connect to OPC UA, configure security and create subscriptions."""
        cfg = self._config

        # ── Security validation (non-retryable configuration errors) ──
        policy_name = cfg.security_policy
        policy_cls: type[SecurityPolicy] | None = None
        mode: ua.MessageSecurityMode | None = None
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

        async def _do_start() -> None:
            # Create a fresh client for each attempt to avoid stale socket/session state.
            client = Client(url=cfg.url)
            try:
                if policy_name != "None":
                    assert policy_cls is not None
                    assert mode is not None
                    await client.set_security(
                        policy=policy_cls,
                        certificate=str(self._settings.OPCUA_CERT_PATH),
                        private_key=str(self._settings.OPCUA_KEY_PATH),
                        mode=mode,
                    )
                    log.info(
                        "OPC UA security configured",
                        policy=policy_name,
                        mode=mode.name,
                    )

                if cfg.authentication_method == "username_password":
                    client.set_user(cfg.username)
                    client.set_password(cfg.password)
                    log.info("OPC UA auth: username/password")
                else:
                    log.info("OPC UA auth: anonymous")

                await client.connect()

                node_to_sensor = self._build_node_to_sensor(cfg)
                handler = _DataChangeHandler(node_to_sensor, self._buffer)
                subscription = await client.create_subscription(
                    period=100,  # ms - server may negotiate a different value
                    handler=handler,
                )
                node_ids = list(node_to_sensor)
                handles: list[Any] = []
                if node_ids:
                    nodes = [client.get_node(node_id) for node_id in node_ids]
                    handles = await subscription.subscribe_data_change(nodes)

                self._client = client
                self._handler = handler
                self._subscription = subscription
                self._node_to_sensor = node_to_sensor
                self._node_handles = {
                    node_id: handle
                    for node_id, handle in zip(node_ids, handles, strict=False)
                }
                self._connected = True
                log.info("OPC UA connected", url=cfg.url)
                log.info(
                    "OPC UA subscriptions created",
                    node_count=len(self._node_handles),
                )
            except Exception:
                try:
                    await client.disconnect()
                except Exception:
                    log.debug("failed to cleanup OPC UA client after startup error")
                raise

        await retry_with_backoff(
            _do_start,
            max_retries=self._settings.BACKOFF_MAX_RETRIES,
            base_delay=self._settings.BACKOFF_BASE_S,
            max_delay=self._settings.BACKOFF_MAX_S,
            operation_name="opcua_start",
        )

    async def stop(self) -> None:
        """Unsubscribe and disconnect cleanly."""
        if self._subscription:
            try:
                all_handles = list(self._node_handles.values())
                if all_handles:
                    await self._subscription.unsubscribe(all_handles)
                await self._subscription.delete()
            except Exception:
                log.exception("error unsubscribing from OPC UA")
            self._subscription = None
            self._node_handles.clear()
            self._node_to_sensor.clear()

        if self._handler is not None:
            await self._handler.flush_pending()
            self._handler = None

        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                log.exception("error disconnecting OPC UA client")
            log.info("OPC UA disconnected")
            self._client = None

        self._connected = False

    async def reconfigure(self, config: CollectorConfig) -> None:
        """Apply a new collector config with minimal disruption.

        Performs full reconnect only when connection/auth/security fields
        changed. Sensor-only changes are applied in-place.
        """
        previous_config = self._config
        previous_node_to_sensor = dict(self._node_to_sensor)

        if self._requires_reconnect(previous_config, config):
            log.info("reconfiguring OPC UA subscriber with reconnect")
            await self.stop()
            self._config = config

            try:
                await self.start()
                return
            except Exception:
                log.exception(
                    "failed to start OPC UA with refreshed config; attempting rollback"
                )

            self._config = previous_config
            await self.start()
            raise RuntimeError(
                "failed to apply refreshed OPC UA config; rolled back to previous"
            )

        # Sensor-only reconfiguration path.
        log.info("reconfiguring OPC UA subscriptions without reconnect")
        self._config = config

        if (
            not self._connected
            or self._client is None
            or self._subscription is None
            or self._handler is None
        ):
            # Fallback to restart if in-memory subscription state is incomplete.
            await self.stop()
            await self.start()
            return

        new_node_to_sensor = self._build_node_to_sensor(config)
        removed_node_ids = sorted(set(previous_node_to_sensor) - set(new_node_to_sensor))
        added_node_ids = sorted(set(new_node_to_sensor) - set(previous_node_to_sensor))

        try:
            removed_handles = [
                self._node_handles[node_id]
                for node_id in removed_node_ids
                if node_id in self._node_handles
            ]
            if removed_handles:
                await self._subscription.unsubscribe(removed_handles)
                for node_id in removed_node_ids:
                    self._node_handles.pop(node_id, None)

            if added_node_ids:
                nodes = [self._client.get_node(node_id) for node_id in added_node_ids]
                new_handles = await self._subscription.subscribe_data_change(nodes)
                for node_id, handle in zip(added_node_ids, new_handles, strict=False):
                    self._node_handles[node_id] = handle

            self._node_to_sensor = new_node_to_sensor
            self._handler.update_node_map(new_node_to_sensor)
            log.info(
                "OPC UA sensor subscriptions updated",
                removed_count=len(removed_node_ids),
                added_count=len(added_node_ids),
                active_count=len(self._node_handles),
            )
        except Exception:
            # Recover from any partial subscription mutations by reconnecting.
            log.exception("in-place sensor reconfiguration failed; reconnecting")
            await self.stop()
            try:
                await self.start()
            except Exception:
                log.exception(
                    "failed to start OPC UA with refreshed config; attempting rollback"
                )
                self._config = previous_config
                await self.start()
                raise RuntimeError(
                    "failed to apply refreshed OPC UA config; rolled back to previous"
                )
