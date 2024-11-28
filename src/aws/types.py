"""AWS type definitions."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum

from omegaconf import DictConfig

from src.common.types import BaseConfig

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
class QueryConfig:
    """Configuration for a single query."""

    template: str
    params: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_config(cls, template: str, params: dict[str, str]):
        """Create instance from OmegaConf config."""
        return cls(template=template, params=params)


@dataclass(frozen=True)
class TableConfig:
    """Configuration for a single table."""

    database: str
    table: str
    query: QueryConfig

    @classmethod
    def from_config(cls, database: str, table: str, query: DictConfig, **_):
        """Create instance from OmegaConf config."""
        try:
            query = QueryConfig.from_config(**query)
        except ValueError as e:
            raise ValueError(f"Invalid query configuration: {e}") from e

        return cls(database=database, table=table, query=query)


@dataclass
class DatabaseConfig:
    """Database configuration with validation."""

    source: Sequence[TableConfig]
    target: Sequence[TableConfig]
    primary: list[str]

    @classmethod
    def from_config(
        cls,
        source: Sequence[DictConfig],
        target: Sequence[DictConfig],
        primary: list[str],
    ):
        """Create instance from OmegaConf config."""
        try:
            sources = [TableConfig.from_config(**s) for s in source]
            targets = [TableConfig.from_config(**t) for t in target]
        except ValueError as e:
            raise ValueError(f"Invalid database configuration: {e}") from e

        return cls(
            source=sources,
            target=targets,
            primary=primary,
        )


@dataclass(frozen=True)
class AWSConfig(BaseConfig):
    """Configuration for AWS services."""

    output: str
    db: DatabaseConfig
    region: Region = Region.EU_CENTRAL_1
    wait_time: int = 1
    max_wait_time: int = 10
    max_retries: int = 3

    @classmethod
    def from_config(cls, region: str, output: str, db: DictConfig, **kwargs) -> AWSConfig:
        """Create instance from OmegaConf config."""

        config = {"output": output, **kwargs}

        try:
            config["region"] = Region(region)
        except ValueError as e:
            raise ValueError(f"Invalid region: {config['region']}") from e

        try:
            config["db"] = DatabaseConfig.from_config(**db)
        except ValueError as e:
            raise ValueError(f"Invalid database configuration: {db}") from e

        return cls(**config)
