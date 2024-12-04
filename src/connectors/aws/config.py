"""AWS connector configuration."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, validates

from src.common.config import BaseConfig
from src.connectors.config import ConnectorConfig


class AWSConnectorConfig(ConnectorConfig):
    """Unified configuration for AWS connector."""

    provider = Literal["aws"]
    params: type[SQLParams] | type[FileParams] = Field(..., description="Data format")

    # check data_format and direction
    @validates("params", pre=True)
    @staticmethod
    def validate_direction(v):
        if "$type" not in v:
            raise ValueError(f"Not parameter $type specified: {v}")

        if v["$type"] not in ["sql", "file"]:
            raise ValueError(f"Invalid parameter $type: {v}")

        if v["$type"] == "sql":
            c = SQLParams
        elif v["$type"] == "file":
            c = FileParams

        try:
            return c(**v)
        except TypeError as e:
            raise ValueError(f"Invalid {c.__name__}: {v}") from e


class SQLParams(BaseConfig):
    region: str = "europe-central-1"
    workgroup: str = "default"
    query: str = Field(
        ...,
        description="Source query for Athena",
        min_length=1,
    )
    database: str = Field(
        ...,
        description="Database name",
        pattern=r"^[a-z0-9._-]+$",
    )
    output_location: str = Field(
        ...,
        description="S3 output location",
        pattern=r"^s3://[a-z0-9.-]+(?:/[a-z0-9._-]+)*/?$",
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


class FileParams(BaseConfig):
    file_path: str = Field(
        ...,
        description="Local file path",
        pattern=r"^s3://[a-z0-9.-]+(?:/[a-z0-9._-]+)*/?$",
    )
