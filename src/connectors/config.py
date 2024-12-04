from __future__ import annotations

from enum import Enum, auto

from pydantic import Field

from src.common.config import BaseConfig


class IOType(str, Enum):
    """Input/Output type."""

    INPUT = auto()
    OUTPUT = auto()


class Region(str, Enum):
    DEFAULT = "europe-central-1"


class Workgroup(str, Enum):
    DEFAULT = "default"


class ConnectorConfig(BaseConfig):
    """Base configuration for connectors."""

    provider: str = Field(
        ...,
        description="Provider name",
    )

    direction: IOType = Field(
        ...,
        description="Connector direction",
    )

    region: Region = Field(
        default=Region.DEFAULT,
        description="AWS region",
    )

    workgroup: Workgroup = Field(
        default=Workgroup.DEFAULT,
        description="AWS workgroup",
    )
