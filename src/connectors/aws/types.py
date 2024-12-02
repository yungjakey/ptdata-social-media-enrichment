"""AWS connector types."""

from __future__ import annotations

import logging
from enum import Enum, auto
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator

from src.connectors.types import ConnectorConfig

logger = logging.getLogger(__name__)


class QueryExecutionError(Exception):
    """Raised when a query execution fails."""

    def __init__(self, query_id: str, state: QueryState, reason: str | None = None):
        self.query_id = query_id
        self.state = state
        self.reason = reason
        message = f"Query {query_id} failed with state {state.name}"
        if reason:
            message += f": {reason}"
        super().__init__(message)


class NotConnectedError(Exception):
    """Raised when trying to use client before connecting."""


class S3LocationError(Exception):
    """Raised when S3 location is invalid."""


class MalformedResponseError(Exception):
    """Raised when AWS response is missing required fields."""

    def __init__(self, response: dict[str, Any], missing_field: str):
        self.response = response
        self.missing_field = missing_field
        super().__init__(f"AWS response missing required field: {missing_field}")


class QueryState(Enum):
    """Enumeration of possible Athena query states."""

    QUEUED = auto()
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    CANCELLED = auto()

    @classmethod
    def terminal_states(cls):
        """Return terminal states."""
        return {cls.SUCCEEDED, cls.FAILED, cls.CANCELLED}

    @classmethod
    def running_states(cls):
        """Return running states."""
        return {cls.QUEUED, cls.RUNNING}


class Region(str, Enum):
    """AWS regions."""

    EU_CENTRAL_1 = "eu-central-1"


class AWSParams(BaseModel):
    """AWS connection parameters."""

    database: str = Field(..., min_length=1)
    query: str = Field(..., min_length=1)
    output_location: str = Field(default="s3://", description="S3 location for output")
    workgroup: str = Field(default="default", min_length=1)
    region: Region = Field(default=Region.EU_CENTRAL_1, description="AWS region")
    wait_time: int = Field(default=10, gt=0, description="Wait time in seconds")
    max_retries: int = Field(default=3, gt=0, description="Max retries")
    max_wait_time: int = Field(default=60, gt=0, description="Max wait time in seconds")

    @field_validator("output_location")
    @classmethod
    def validate_s3_location(cls, v: str) -> str:
        """Validate S3 location."""
        try:
            location = urlparse(v)
            if location.scheme != "s3":
                raise ValueError("Location must use s3:// scheme")
            if not location.netloc:
                raise ValueError("Location must include a bucket name")
            if not location.path or location.path == "/":
                raise ValueError("Location must include a path")
            return v
        except Exception as e:
            raise ValueError(f"Invalid S3 location '{v}'") from e

    @classmethod
    def from_dict(cls, database: str, query: str, **kwargs) -> AWSParams:
        """Create instance from dict config."""
        return cls(database=database, query=query, **kwargs)


class AWSClientConfig(ConnectorConfig):
    """AWS client configuration."""

    name: str
    params: AWSParams
    type: str = "aws"

    @classmethod
    def from_dict(cls, type: str, name: str, params: dict[str, Any]) -> AWSClientConfig:
        """Create an instance from explicit configuration parameters."""
        try:
            aws_params = AWSParams.from_dict(**params)
        except Exception as e:
            raise ValueError(f"Invalid AWS parameters: {e}") from e

        return cls(type=type, name=name, params=aws_params)
