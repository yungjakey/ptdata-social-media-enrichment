"""AWS connector types."""

from __future__ import annotations

import logging
from enum import Enum
from urllib.parse import urlparse

from pydantic import Field, field_validator

from src.common.types import BaseConfig, BaseParams, ConfigDict

logger = logging.getLogger(__name__)


class Workggroup(str, Enum):
    """AWS Athena workgroup."""

    DEFAULT = "default"
    PRIMARY = "primary"

    @classmethod
    def default(cls):
        return cls.DEFAULT


class Region(str, Enum):
    """AWS regions."""

    EU_CENTRAL_1 = "eu-central-1"

    @classmethod
    def default(cls):
        return cls.EU_CENTRAL_1


class AWSParams(BaseParams):
    """AWS connection parameters."""

    database: str = Field(..., min_length=1, description="Database name")
    query: str = Field(..., min_length=1, description="Query string")
    output_location: str = Field(..., description="S3 location for output")
    workgroup: str = Field(default_factory=Workggroup.default, min_length=1)
    region: Region = Field(default_factory=Region.default, description="AWS region")
    wait_time: int = Field(default=10, gt=0, description="Wait time in seconds")
    max_retries: int = Field(default=3, gt=0, description="Max retries")
    max_wait_time: int = Field(default=60, gt=0, description="Max wait time in seconds")

    @field_validator("output_location")
    @classmethod
    def validate_s3_location(cls, v: str) -> str:
        """Validate S3 location."""
        try:
            result = urlparse(v)
            if result.scheme != "s3":
                raise ValueError("Must be an S3 URL (s3://...)")
            if not result.netloc:
                raise ValueError("Must specify bucket name")
            return v
        except Exception as e:
            raise ValueError(f"Invalid S3 location: {e}") from e


class AWSConfig(BaseConfig[AWSParams]):
    """AWS connector configuration."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)
