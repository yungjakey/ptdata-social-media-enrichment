"""AWS type definitions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

from src.common.types import BaseConfig, Directions
from src.connectors.types import TableConfig

logger = logging.getLogger(__name__)


class Region(str, Enum):
    """AWS regions."""

    EU_CENTRAL_1 = "eu-central-1"


@dataclass
class State:
    """Query execution states."""

    # Success states
    _ok = ("SUCCEEDED", "COMPLETED")
    _nok = ("FAILED", "CANCELLED")
    _pending = ("QUEUED", "RUNNING")

    # Class-level constants
    SUCCEEDED = "SUCCEEDED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"

    status: str

    def __post_init__(self):
        # Normalize to uppercase
        self.status = str(self.status or "").upper()

    @property
    def ok(self) -> bool:
        """Check if state is a success state."""
        return self.status in [s.upper() for s in self._ok]

    @property
    def nok(self) -> bool:
        """Check if state is a failure state."""
        return self.status in [s.upper() for s in self._nok]

    @property
    def pending(self) -> bool:
        """Check if state is a pending state."""
        return self.status in [s.upper() for s in self._pending]


@dataclass(frozen=True)
class AWSConfig(BaseConfig):
    """Configuration for AWS services."""

    output: str
    connections: list[dict[Directions, TableConfig]]
    region: Region | str = Region.EU_CENTRAL_1
    wait_time: int = 1
    max_wait_time: int = 10
    max_retries: int = 3

    @classmethod
    def from_config(
        cls, output: str, connections: list[dict[Directions, dict[str, str]]], region: str, **kwargs
    ) -> AWSConfig:
        """Create instance from OmegaConf config."""

        if not connections:
            raise ValueError("No connections defined")

        connections = []
        for connection in connections:
            try:
                connections.append(
                    {
                        Directions.Input: TableConfig.from_config(**connection[Directions.Input]),
                        Directions.Output: TableConfig.from_config(**connection[Directions.Output]),
                    }
                )
            except Exception as e:
                raise ValueError(f"Invalid connection  {connection}: {e}") from e

        try:
            region = Region(region)
        except ValueError as e:
            raise ValueError(f"Invalid region: {region}") from e

        config = {
            "output": output,
            "connections": connections,
            "region": region,
            **kwargs,
        }

        return cls(**config)
