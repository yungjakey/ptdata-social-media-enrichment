"""Inference types."""

from __future__ import annotations

import importlib

from pydantic import BaseModel, Field, validator

from src.common.config import BaseConfig


class InferenceConfig(BaseConfig):
    """Inference configuration."""

    # creds
    api_key: str | None = Field(
        default=...,
        description="API key",
    )
    api_base: str | None = Field(
        default=...,
        description="API base URL",
    )

    # basic
    provider: str = Field(
        default="azure",
        description="Inference provider",
    )
    version: str = Field(
        default="2024-08-01-preview",
        description="API version",
    )
    deployment: str = Field(
        default="gpt-4o",
        description="API deployment",
    )
    engine: str = Field(
        default="gpt-4o",
        description="API engine",
    )

    # orchestration
    workers: int = Field(
        description="Number of concurrent workers",
        ge=1,
        le=50,
        default=1,
    )

    # data integration
    exclude_fields: list[str] = Field(
        default_factory=list,
        description="Fields to exclude from LLM processing",
    )

    # inference
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
