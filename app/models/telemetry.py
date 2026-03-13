"""Pydantic models for the standardised telemetry payload."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

__all__ = ["TelemetryValue", "TelemetryPayload"]


class TelemetryValue(BaseModel):
    """Inner payload carrying the sensor reading."""

    value: float | int | str | bool
    status: str


class TelemetryPayload(BaseModel):
    """One telemetry data-point ready for AMQP publishing."""

    sensor_id: UUID
    time: datetime
    payload: TelemetryValue
