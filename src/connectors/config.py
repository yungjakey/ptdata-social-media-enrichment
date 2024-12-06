"""AWS connector configuration."""

from __future__ import annotations

from pydantic import Field

from src.common.config import BaseConfig

_VALID_IMPLEMENTATIONS = ["sql", "file"]


class AWSConnectorConfig(BaseConfig):
    """Unified configuration for AWS connector."""

    output_location: str = Field(
        ...,
        description="S3 output location",
        pattern=r"^s3://[a-z0-9.-]+(?:/[a-z0-9._-]+)*/?$",
    )
    source_query: str = Field(
        ...,
        description="Source query for Athena",
    )

    region: str = "eu-central-1"
    workgroup: str = "primary"
    target_format: str = "parquet"
    timeout: int = Field(
        default=60,
        description="Query timeout in seconds",
        ge=1,
        le=60,
    )
    poll_interval: int = Field(
        default=5,
        description="Poll interval in seconds",
        ge=1,
        le=10,
    )
