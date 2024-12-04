"""AWS connector configuration."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from src.connectors.config import ConnectorConfig


class AWSConnectorConfig(ConnectorConfig):
    """Unified configuration for AWS connector."""

    provider = Literal["aws"]
    database: str = Field(
        ...,
        description="Database name",
    )
    output_location: str = Field(
        ...,
        description="S3 output location",
        pattern=r"^s3://[a-z0-9.-]+(?:/[a-z0-9._-]+)*/?$",
    )
    source_query: str = Field(
        ...,
        description="Source query for Athena",
    )
    target_params: dict = Field(
        ...,
        description="Target parameters for Athena",
    )
