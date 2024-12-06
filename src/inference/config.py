"""Inference types."""

from __future__ import annotations

import importlib
import os

from pydantic import BaseModel, Field, validator

from src.common.config import BaseConfig


class InferenceConfig(BaseConfig):
    """Inference configuration."""

    provider: str = "azure"
    version: str = "2024-08-01-preview"
    deployment: str = "gpt-4o"
    engine: str = "gpt-4o"

    api_key: str | None = Field(
        None,
        description="API key",
        min_length=1,
    )
    api_base: str | None = Field(
        None,
        description="API base URL",
        min_length=1,
    )

    @validator("api_key")
    @classmethod
    def validate_api_key(cls, v: str | None = None) -> str:
        if not v:
            v = os.getenv("OPENAI_API_KEY")
        if not v:
            raise ValueError("API key is required")
        return v

    @validator("api_base")
    @classmethod
    def validate_api_base(cls, v: str | None = None) -> str:
        if not v:
            v = os.getenv("OPENAI_API_BASE")
        if not v:
            raise ValueError("API base URL is required")
        return v

    workers: int = Field(
        description="Number of workers",
        ge=1,
        le=10,
        default=1,
    )
    response_format: type[BaseModel] | str = Field(
        ...,
        description="Response format",
    )

    @validator("response_format", pre=True)
    @classmethod
    def validate_response_format(cls, v: str) -> type[BaseModel]:
        try:
            mod = importlib.import_module(f"src.inference.models.{v.lower()}")
            return getattr(mod, v.capitalize())
        except (ImportError, AttributeError) as e:
            raise ValueError(f"Invalid response format: {v}") from e

    temperature: float = Field(
        default=0.7,
        description="Sampling temperature",
        ge=0.0,
        le=1.0,
    )
    max_tokens: int | None = Field(
        default=None,
        description="Maximum number of tokens to generate",
        ge=0,
        le=4096,
    )
    timeout: int | None = Field(
        default=None,
        description="Request timeout in seconds",
        ge=1,
        le=60,
    )
