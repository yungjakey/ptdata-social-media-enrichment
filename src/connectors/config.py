from __future__ import annotations

from pydantic import Field

from src.common.config import BaseConfig, TConf


class ConnectorConfig(BaseConfig):
    """Base configuration for connectors."""

    provider: str = Field(
        ...,
        description="Provider name",
    )
    params: TConf = Field(
        ...,
        description="Connector parameters",
    )
