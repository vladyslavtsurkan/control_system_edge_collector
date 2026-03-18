"""Pydantic models for the Cloud API ``/api/v1/collector/config`` response."""

from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, model_validator

__all__ = ["CollectorConfig", "SensorConfig"]


class SensorConfig(BaseModel):
    """A single OPC UA sensor / tag."""

    id: UUID
    name: str
    node_id: str
    units: str


class CollectorConfig(BaseModel):
    """Full OPC UA collector configuration returned by the Cloud API."""

    id: UUID
    name: str
    url: str
    security_policy: str
    authentication_method: Literal["anonymous", "username_password"]
    username: str | None = None
    password: str | None = None
    sensors: list[SensorConfig]

    @model_validator(mode="after")
    def _validate_credentials(self) -> Self:
        if self.authentication_method == "username_password":
            if not self.username or not self.password:
                raise ValueError(
                    "username and password are required when "
                    "authentication_method is 'username_password'"
                )
        return self
