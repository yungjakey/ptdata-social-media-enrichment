"""Type definitions for Azure OpenAI configuration."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel

from src.common.types import BaseConfig

logger = logging.getLogger(__name__)


class Role(str, Enum):
    """OpenAI chat completion roles."""

    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"


class Message(BaseModel):
    """OpenAI chat completion message."""

    role: Role
    content: str
    name: str | None = None


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


class APIVersion(str, Enum):
    """Azure OpenAI API versions."""

    V2024_10_01 = "2024-10-01-preview"


class ResponseFormat(str, Enum):
    """OpenAI response format."""

    JSON = "json"
    TEXT = "text"

    def __str__(self) -> str:
        return {
            self.JSON: "{type: 'json_object'}",
            self.TEXT: None,
        }


@dataclass(frozen=True)
class OpenAIConfig(BaseConfig):
    """Configuration for Azure OpenAI services."""

    api_key: str
    api_base: str
    api_version: APIVersion
    engine: ModelName
    max_workers: int
    temperature: float = 0.7
    max_tokens: int | None = None
    top_p: float = 1.0
    presence_penalty: float = 0.0
    frequency_penalty: float = 0.0
    stop: list[str] | None = None
    response_format: ResponseFormat | None = None
    timeout: int = 10
    # messages: list[Message] = field(default_factory=list)

    def validate(self) -> None:
        """Validate configuration."""
        if self.max_workers <= 0:
            raise ValueError("max_workers must be positive")

    @classmethod
    def from_config(cls, api_version: str, engine: str, **kwargs) -> OpenAIConfig:
        """Create instance from OmegaConf config."""
        creds = {"api_key": os.getenv("OPENAI_API_KEY"), "api_base": os.getenv("OPENAI_API_BASE")}

        if not all(creds.values()):
            missing = ",".join([f for f in creds if not creds[f]])
            raise ValueError(f"Missing OpenAI API creds: {missing}")

        config = kwargs

        try:
            config["api_version"] = APIVersion(api_version)
        except ValueError as e:
            raise ValueError(f"Invalid API version: {api_version}") from e

        try:
            config["engine"] = ModelName(engine)
        except ValueError as e:
            raise ValueError(f"Invalid model: {engine}") from e

        return cls(**config)
