"""AWS connector configuration."""

from __future__ import annotations

from enum import Enum

from pydantic import Field

from src.common.config import BaseConfig


class AllowedTypes(str, Enum):
    INT = "int"
    STRING = "string"


class AllowedNames(str, Enum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"


class Partition(BaseConfig):
    Name: AllowedNames = Field(
        ...,
        description="Name of the partition",
    )

    Type: AllowedTypes = Field(
        ...,
        description="Type of the partition",
    )


class TableConfig(BaseConfig):
    """Table configuration for AWS Glue."""

    database: str = Field(
        ...,
        description="Name of the Athena database",
    )

    table_name: str = Field(
        ...,
        description="Name of the Athena table",
    )

    partitions: list[Partition] | None = Field(
        default=None,
        description="Partition configuration as list of Partition objects",
    )

    @property
    def names(self) -> list[str]:
        return [col.Name.value for col in self.partitions] if self.partitions else []

    @property
    def types(self) -> list[str]:
        return [col.Type.value for col in self.partitions] if self.partitions else []

    @property
    def partition_keys(self) -> list[dict[str, str]]:
        """Convert partition objects to Glue API format."""
        if not self.partitions:
            return []
        return [{"Name": p.Name.value, "Type": p.Type.value} for p in self.partitions]


class AWSConnectorConfig(BaseConfig):
    """Unified configuration for AWS connector."""

    region: str = Field(
        default="eu-central-1",
        description="Region to connect to",
    )
    workgroup: str = Field(
        default="primary",
        description="Athena workgroup",
    )
    target_format: str = Field(
        default="parquet",
        description="Target format for output files",
    )

    bucket_name: str = Field(
        ...,
        description="S3 bucket name",
    )
    table_config: TableConfig = Field(
        ...,
        description="Table configuration",
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
        default=1,
        description="Poll interval in seconds",
        ge=1,
        le=10,
    )

    @property
    def output_location(self) -> str:
        """Get the S3 output location."""
        return f"s3://{self.bucket_name}/"
