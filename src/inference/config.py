"""Inference types."""

from __future__ import annotations

import importlib
import os

from pydantic import Field, field_validator, model_validator

from src.common.config import BaseConfig
from src.inference.models.base import BaseInferenceModel


class InferenceConfig(BaseConfig):
    """Inference configuration."""

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

    # creds
    api_key: str = Field(
        default=...,
        description="API key",
    )
    api_base: str = Field(
        default=...,
        description="API base URL",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_credentials(cls, values: dict) -> dict:
        api_key = values.get("api_key") or os.getenv("OPENAI_API_KEY")
        api_base = values.get("api_base") or os.getenv("OPENAI_API_BASE")

        if not api_key:
            raise ValueError("OPENAI_API_KEY must be set")
        if not api_base:
            raise ValueError("OPENAI_API_BASE must be set")

        values["api_key"] = api_key
        values["api_base"] = api_base
        return values

    workers: int = Field(
        default=1,
        description="Number of workers",
        ge=1,
        le=50,
    )

    # data integration
    include_fields: list[str] = Field(
        default_factory=list,
        description="Fields to include in the LLM processing",
    )

    # inference
    response_format: type[BaseInferenceModel] = Field(
        ...,
        description="Response format",
    )

    @field_validator("response_format", mode="before")
    @classmethod
    def validate_response_format(cls, v: str) -> type[BaseInferenceModel]:
        try:
            mod = importlib.import_module(f"src.inference.models.{v.lower()}")
            return getattr(mod, "".join([p.capitalize() for p in v.split("_")]))
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
        le=16384,
    )
    timeout: int | None = Field(
        default=None,
        description="Request timeout in seconds",
        ge=1,
        le=60,
    )
    profanity_filter: bool = Field(
        default=False,
        description="Filter profanity",
    )
