"""Inference types."""

from __future__ import annotations

from enum import Enum
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


class APIVersion(str, Enum):
    """Azure OpenAI API versions."""

    FEB_PREVIEW = "2024-02-15-preview"
    DEFAULT = FEB_PREVIEW


class ModelName(str, Enum):
    """Available OpenAI models."""

    GPT4O_MINI = "gpt-4o-mini"
    GPT4 = "gpt-4"
    DEFAULT = GPT4O_MINI

    @property
    def max_tokens(self) -> int:
        """Get max tokens for model."""
        return {
            self.GPT4: 8192,
            self.GPT4O_MINI: 8192,
        }[self]


class InferenceParams(BaseModel):
    """Azure OpenAI parameters."""

    engine: ModelName = Field(
        default=ModelName.DEFAULT,
        description="Model engine name",
    )
    version: APIVersion = Field(
        default=APIVersion.DEFAULT,
        description="API version",
    )
    api_base: str = Field(
        ...,
        min_length=1,
        description="API base URL",
    )
    api_key: str = Field(
        ...,
        min_length=1,
        description="API key",
    )
    max_workers: int = Field(
        default=5,
        gt=0,
        description="Maximum number of concurrent workers",
    )
    temperature: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Sampling temperature",
    )
    max_tokens: int | None = Field(
        default=None,
        description="Maximum number of tokens to generate",
    )
    top_p: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Nucleus sampling parameter",
    )
    presence_penalty: float = Field(
        default=0.0,
        ge=-2.0,
        le=2.0,
        description="Presence penalty parameter",
    )
    frequency_penalty: float = Field(
        default=0.0,
        ge=-2.0,
        le=2.0,
        description="Frequency penalty parameter",
    )
    stop: list[str] | None = Field(
        default=None,
        description="Stop sequences",
    )
    response_format: BaseModel | None = Field(
        default=None,
        description="Response format configuration",
    )
    timeout: int = Field(
        default=10,
        gt=0,
        description="Request timeout in seconds",
    )

    @field_validator("api_key")
    def validate_api_key(self, v: str) -> str:
        """Validate API key."""
        if not v or len(v.strip()) == 0:
            raise ValueError("API key cannot be empty")
        return v.strip()

    @field_validator("api_base")
    def validate_api_base(self, v: str) -> str:
        """Validate API base URL."""
        if not v or len(v.strip()) == 0:
            raise ValueError("API base URL cannot be empty")

        try:
            parsed_url = urlparse(v.strip())

            # Check for valid scheme
            if parsed_url.scheme not in ("http", "https"):
                raise ValueError("URL must use http or https scheme")

            # Ensure host is not empty
            if not parsed_url.netloc:
                raise ValueError("URL must have a valid host")

            return v.strip()

        except Exception as e:
            raise ValueError(f"Invalid API base URL: {v}") from e

    @field_validator("max_tokens")
    def validate_max_tokens(self, v: int | None, info: ValidationInfo) -> int | None:
        """Validate max_tokens against model's maximum."""
        if v is not None:
            # Retrieve the engine from the context
            engine = info.data.get("engine")
            if engine is None:
                raise ValueError("Engine must be specified to validate max_tokens")

            model_max = engine.max_tokens
            if v > model_max:
                raise ValueError(f"max_tokens cannot exceed {model_max} for {engine}")
        return v


class InferenceConfig(BaseConfig[InferenceParams]):
    """Azure OpenAI configuration."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)
