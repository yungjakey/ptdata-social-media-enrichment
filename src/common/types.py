"""Common type definitions."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BaseConfig(BaseModel):
    """Base configuration class."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class RootConfig(BaseModel):
    """Root configuration for job execution."""

    input: BaseConfig = Field(..., description="Input data source configuration")
    output: BaseConfig = Field(..., description="Output data destination configuration")
    provider: BaseConfig = Field(..., description="Provider configuration")
    model: BaseConfig = Field(..., description="Model configuration")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @classmethod
    def from_dict(cls, **config: dict[str, Any]) -> RootConfig:
        """Create RootConfig from dictionary."""
        try:
            return cls(**config)
        except Exception as e:
            raise ValueError(f"Invalid root configuration: {e}") from e
