"""Inference types."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import ConfigDict, Field


class APIVersion(str, Enum):
    """Azure OpenAI API versions."""

    DEFAULT = "2024-02-15-preview"


class ModelName(str, Enum):
    """Available OpenAI models."""

    DEFAULT = "gpt-4o-mini"


class InferenceConfig:
    """Azure OpenAI parameters."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True, from_attributes=True)

    engine: ModelName = Field(
        default=ModelName.DEFAULT,
        description="Model engine name",
    )

    version: APIVersion = Field(
        default=APIVersion.DEFAULT,
        description="API version",
    )

    api_base: str = Field(
        ..., description="API base URL", pattern=r"^https?://[a-zA-Z0-9.-]+(?::\d+)?(?:/\S*)?$"
    )

    api_key: str = Field(
        ...,
        description="API key",
    )

    max_workers: int = Field(
        default=5,
        description="Maximum number of concurrent workers",
        ge=1,
        le=10,
    )

    temperature: float = Field(
        default=0.7,
        description="Sampling temperature",
        ge=0.0,
        le=1.0,
    )

    max_tokens: int | None = Field(
        default=None,
        description="Maximum number of tokens to generate",
        gt=0 if not None else None,
    )

    top_p: float = Field(default=1.0, description="Nucleus sampling parameter", ge=0.0, le=1.0)

    presence_penalty: float = Field(
        default=0.0,
        description="Presence penalty parameter",
        ge=-2.0,
        le=2.0,
    )

    frequency_penalty: float = Field(
        default=0.0,
        description="Frequency penalty parameter",
        ge=-2.0,
        le=2.0,
    )

    stop: list[str] | None = Field(
        default=None,
        description="Stop sequences",
    )

    response_format: Any | None = Field(
        default=None,
        description="Response format configuration",
    )

    timeout: int = Field(default=10, description="Request timeout in seconds", ge=1, le=60)
