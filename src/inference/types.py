from __future__ import annotations

import logging
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from src.common import BaseConfig

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


class OpenAIParams(BaseConfig):
    api_key: str = Field(..., min_length=1, description="Azure OpenAI base URL")
    api_base: str = Field(..., min_length=1, description="Azure OpenAI base URL")
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

    # validate api key and base
    @model_validator(after=True)
    def validate_api_key_and_base(cls, values):
        api_key = values.get("api_key")
        api_base = values.get("api_base")
        if not api_key.startswith("sk-") and not api_key.startswith("azopenai://"):
            raise ValueError("Invalid OpenAI API key")
        if not api_base.startswith("https://") and not api_base.startswith("azopenai://"):
            raise ValueError("Invalid OpenAI API base")
        return values

    @classmethod
    def from_dict(cls, api_key: str, api_base: str, **kwargs):
        return cls(api_key=api_key, api_base=api_base, **kwargs)


class OpenAIConfig(BaseConfig):
    """Configuration for Azure OpenAI services."""

    params: OpenAIParams = Field(default_factory=OpenAIParams)
    type: str = "azure"

    @classmethod
    def from_dict(
        cls, api_key: str, api_base: str, api_version: str, engine: str, **kwargs: Any
    ) -> OpenAIConfig:
        """Create config from dictionary."""
        try:
            azure_params = OpenAIParams.from_dict(**kwargs)
        except Exception as e:
            raise ValueError(f"Invalid OpenAI parameters: {e}") from e

        return cls(api_key=api_key, api_base=api_base, params=azure_params)
