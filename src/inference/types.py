from __future__ import annotations

import logging
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)


class APIVersion(str, Enum):
    """Azure OpenAI API versions."""

    V2024_10_01 = "2024-10-01-preview"


class ModelName(str, Enum):
    """Available OpenAI models."""

    GPT4 = "gpt-4"
    GPT4O_MINI = "gpt-4o-mini"

    @property
    def max_tokens(self) -> int:
        """Get max tokens for model."""
        return {
            self.GPT4: 8192,
            self.GPT4O_MINI: 8192,
        }[self]


class OpenAIConfig(BaseModel):
    """Configuration for Azure OpenAI services."""

    type: str = "azure"
    api_key: str = Field(..., description="Azure OpenAI API key")
    api_base: str = Field(..., description="Azure OpenAI API base URL")
    api_version: APIVersion = Field(
        default=APIVersion.V2024_10_01, description="Azure OpenAI API version"
    )
    engine: ModelName = Field(..., description="Azure OpenAI model name")
    max_workers: int = Field(default=5, gt=0, description="Maximum number of concurrent workers")
    temperature: float = Field(default=0.7, ge=0.0, le=1.0, description="Sampling temperature")
    max_tokens: int | None = Field(default=None, description="Maximum number of tokens to generate")
    top_p: float = Field(default=1.0, ge=0.0, le=1.0, description="Nucleus sampling parameter")
    presence_penalty: float = Field(
        default=0.0, ge=-2.0, le=2.0, description="Presence penalty parameter"
    )
    frequency_penalty: float = Field(
        default=0.0, ge=-2.0, le=2.0, description="Frequency penalty parameter"
    )
    stop: list[str] | None = Field(default=None, description="Stop sequences")
    response_format: BaseModel | None = Field(
        default=None, description="Response format configuration"
    )
    timeout: int = Field(default=10, gt=0, description="Request timeout in seconds")

    @model_validator(mode="after")
    def validate_required_fields(self) -> OpenAIConfig:
        """Validate required fields are not empty."""
        for field_name in ["api_key", "api_base"]:
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} cannot be empty")
        return self

    @classmethod
    def from_dict(
        cls, api_key: str, api_base: str, api_version: str, engine: str, **kwargs: Any
    ) -> OpenAIConfig:
        """Create config from dictionary."""
        try:
            return cls(
                api_key=api_key,
                api_base=api_base,
                api_version=APIVersion(api_version),
                engine=ModelName(engine),
                **kwargs,
            )
        except ValueError as e:
            raise ValueError(f"Invalid configuration: {e}") from e
