"""AWS type definitions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

from pydantic import Field

from src.connectors.types import ConnectorParams

logger = logging.getLogger(__name__)


class Region(str, Enum):
    """AWS regions."""

    EU_CENTRAL_1 = "eu-central-1"


@dataclass
class TableConfig:
    """Athena table configuration."""

    name: str
    filter: str | None = None


@dataclass
class AthenaParams(ConnectorParams):
    """Athena-specific connection parameters."""

    database: str
    region: Region
    output_location: str
    workgroup: str
    tables: list[TableConfig]
    query: str
    wait_time: int = Field(default=10, gt=0, description="Wait time in seconds")
    max_retries: int = Field(default=3, gt=0, description="Max retries")
    max_wait_time: int = Field(default=60, gt=0, description="Max wait time in seconds")

    @classmethod
    def from_dict(cls, params: dict) -> AthenaParams:
        """Create instance from dict config."""
        tables = [TableConfig(**t) for t in params.get("tables", [])]
        return cls(
            database=params["database"],
            region=Region(params["region"]),
            output_location=params["output_location"],
            workgroup=params["workgroup"],
            tables=tables,
            query=params["query"],
            wait_time=params.get("wait_time", 10),
            max_retries=params.get("max_retries", 3),
            max_wait_time=params.get("max_wait_time", 60),
        )
