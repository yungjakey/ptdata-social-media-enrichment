"""Inference types."""

from __future__ import annotations

import importlib

from pydantic import BaseModel, Field, validates

from src.common.config import BaseConfig


class InferenceConfig(BaseConfig):
    """Inference configuration."""

    provider: str = "azure"
    engine: str = "gpt-4o-mini"
    version: str = "2024-02-15-preview"

    response_format: type[BaseModel] = Field(
        ...,
        description="Response format",
        min_length=1,
    )

    @validates("response_format", pre=True)
    @classmethod
    def validate_response_format(cls, v: str) -> type[BaseModel]:
        """validate specified response format exists locally"""
        try:
            mod = importlib.import_module(f"src.inference.models.{v.lower()}")
        except ImportError as e:
            raise ValueError(f"Invalid response format: {v}") from e

        try:
            return getattr(mod, v.capitalize())
        except AttributeError as e:
            raise ValueError(f"Invalid response format: {v}") from e

    api_base: str = Field(
        ...,
        description="API base URL",
        pattern=r"^https?://[a-zA-Z0-9.-]+(?::\d+)?(?:/\S*)?$",
    )

    api_key: str = Field(
        ...,
        description="API key",
        min_length=1,
    )

    workers: int = Field(
        default=5,
        description="Number of workers",
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

    timeout: int = Field(default=10, description="Request timeout in seconds", ge=1, le=60)
