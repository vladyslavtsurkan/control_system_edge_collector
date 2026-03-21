"""Runtime collector-config refresh helpers."""

import asyncio
import random
from typing import Any

import structlog

from app.control_plane.api_client import fetch_collector_config
from app.data_plane.opcua_subscriber import OpcuaSubscriber
from app.models.config import CollectorConfig
from app.settings import get_settings

__all__ = ["config_refresh_loop"]

log = structlog.get_logger()


def _normalize_sensors(config: CollectorConfig) -> list[dict[str, str]]:
    sensors = [
        {
            "id": str(sensor.id),
            "name": sensor.name,
            "node_id": sensor.node_id,
            "units": sensor.units,
        }
        for sensor in config.sensors
    ]
    return sorted(sensors, key=lambda item: (item["id"], item["node_id"]))


def _opcua_runtime_snapshot(config: CollectorConfig) -> dict[str, Any]:
    return {
        "url": config.url,
        "security_policy": config.security_policy,
        "authentication_method": config.authentication_method,
        "username": config.username,
        "password": config.password,
        "sensors": _normalize_sensors(config),
    }


def _mask_sensitive(field: str, value: Any) -> Any:
    if field == "password" and value is not None:
        return "***"
    return value


def _snapshot_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    changed_fields: list[str] = []
    details: dict[str, Any] = {}
    for field in sorted(set(before) | set(after)):
        if before.get(field) == after.get(field):
            continue
        changed_fields.append(field)
        details[field] = {
            "before": _mask_sensitive(field, before.get(field)),
            "after": _mask_sensitive(field, after.get(field)),
        }
    return {"changed_fields": changed_fields, "details": details}


async def config_refresh_loop(
    subscriber: OpcuaSubscriber,
    shutdown_event: asyncio.Event,
    initial_config: CollectorConfig,
) -> None:
    settings = get_settings()
    interval_s = max(settings.CONFIG_REFRESH_INTERVAL_S, 1.0)
    jitter_s = max(settings.CONFIG_REFRESH_JITTER_S, 0.0)

    current_snapshot = _opcua_runtime_snapshot(initial_config)
    log.info(
        "config refresh loop started",
        interval_s=interval_s,
        jitter_s=jitter_s,
    )

    try:
        while not shutdown_event.is_set():
            wait_s = interval_s + random.uniform(0.0, jitter_s)
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=wait_s)
                break
            except TimeoutError:
                pass

            try:
                refreshed_config = await fetch_collector_config()
            except Exception:
                log.exception("failed to refresh collector config; keeping current")
                continue

            refreshed_snapshot = _opcua_runtime_snapshot(refreshed_config)
            if refreshed_snapshot == current_snapshot:
                log.debug("collector config unchanged")
                continue

            diff = _snapshot_diff(current_snapshot, refreshed_snapshot)
            log.info(
                "collector config changed; applying",
                changed_fields=diff["changed_fields"],
                changes=diff["details"],
            )

            try:
                await subscriber.reconfigure(refreshed_config)
            except Exception:
                log.exception("failed to apply refreshed config; keeping previous")
                continue

            current_snapshot = refreshed_snapshot
            log.info("collector reconfiguration applied")
    except asyncio.CancelledError:
        log.debug("config refresh loop cancelled")
        raise
    finally:
        log.info("config refresh loop stopped")
