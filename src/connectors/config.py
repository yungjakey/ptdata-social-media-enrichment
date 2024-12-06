"""AWS connector configuration."""

from __future__ import annotations

from pydantic import Field

from src.common.config import BaseConfig

_VALID_IMPLEMENTATIONS = ["sql", "file"]


class AWSConnectorConfig(BaseConfig):
    """Unified configuration for AWS connector."""

    region: str = "eu-central-1"
    workgroup: str = "primary"
    target_format: str = "parquet"

    bucket_name: str = Field(
        ...,
        description="S3 output location",
    )

    source_query: str = Field(
        ...,
        description="Source query for Athena",
    )

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

    @property
    def output_location(self) -> str:
        return f"s3://{self.bucket_name}"
