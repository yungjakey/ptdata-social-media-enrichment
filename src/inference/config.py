"""Inference types."""

from __future__ import annotations

import importlib
import os

from pydantic import Field, field_validator

from inference.models.base import BaseInferenceModel
from src.common.config import BaseConfig


class InferenceConfig(BaseConfig):
    """Inference configuration."""

    # creds
    api_key: str = Field(
        default=...,
        description="API key",
    )
    api_base: str = Field(
        default=...,
        description="API base URL",
    )

    @field_validator("api_key", mode="before")
    @classmethod
    def check_api_key(cls, v: str) -> str:
        e = None
        if not v:
            e = os.getenv("OPENAI_API_KEY")
            if not v:
                raise ValueError("OPENAI_API_KEY must be set in env")
        if not e:
            raise ValueError("OPENAI_API_KEY must be set")
        return e

    @field_validator("api_base", mode="before")
    @classmethod
    def check_api_base(cls, v: str) -> str:
        e = None
        if not v:
            e = os.getenv("OPENAI_API_BASE")
            if not v:
                raise ValueError("OPENAI_API_BASE must be set in env")
        if not e:
            raise ValueError("OPENAI_API_BASE must be set")
        return e

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
        le=4096,
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
