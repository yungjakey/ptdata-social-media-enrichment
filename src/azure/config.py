"""Azure configuration classes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from omegaconf import DictConfig

from src.common import DataConfig, SingletonConfig


@dataclass
class Payload(DataConfig):
    """Payload for Azure OpenAI API."""

    user: str
    system: str
    config: ModelConfig
    messages = field(default_factory=list)

    def __post_init__(self):
        self.messages = [
            {"role": "user", "content": self.user},
            {"role": "system", "content": self.system},
        ]

    def __dict__(self):
        return {"messages": self.messages, **self.config.dict()}


@dataclass(frozen=True)
class ClientConfig(DataConfig):
    """Azure OpenAI client configuration."""

    api_key: str | None = None
    api_base: str | None = None
    api_version: str | None = None
    engine: str | None = None
    organization: str | None = None
    timeout: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ClientConfig:
        """Create instance from dictionary."""
        try:
            return cls(
                api_key=data.get("api_key"),
                api_base=data.get("api_base"),
                api_version=data.get("api_version"),
                engine=data.get("engine"),
                organization=data.get("organization"),
                timeout=data.get("timeout"),
            )
        except Exception as e:
            raise ValueError("Failed to create client config") from e


@dataclass(frozen=True)
class ModelConfig(DataConfig):
    """Model configuration with parameters."""

    client: ClientConfig
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    frequency_penalty: float | None = None
    presence_penalty: float | None = None
    stop: list[str] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ModelConfig:
        """Create instance from dictionary."""
        try:
            client_data = data.get("client", {})
            if not client_data:
                raise ValueError("Client configuration is required")

            return cls(
                client=ClientConfig.from_dict(client_data),
                temperature=data.get("temperature"),
                max_tokens=data.get("max_tokens"),
                top_p=data.get("top_p"),
                frequency_penalty=data.get("frequency_penalty"),
                presence_penalty=data.get("presence_penalty"),
                stop=data.get("stop"),
            )
        except Exception as e:
            raise ValueError("Failed to create model config") from e

    def validate(self) -> None:
        """Validate model configuration."""
        self._validate_required_fields({"client"})
        if self.temperature is not None and not 0 <= self.temperature <= 2:
            raise ValueError("Temperature must be between 0 and 2")
        if self.max_tokens is not None and self.max_tokens < 1:
            raise ValueError("Max tokens must be positive")
        if self.top_p is not None and not 0 <= self.top_p <= 1:
            raise ValueError("Top p must be between 0 and 1")
        if self.frequency_penalty is not None and not -2 <= self.frequency_penalty <= 2:
            raise ValueError("Frequency penalty must be between -2 and 2")
        if self.presence_penalty is not None and not -2 <= self.presence_penalty <= 2:
            raise ValueError("Presence penalty must be between -2 and 2")


@dataclass(frozen=True)
class OpenAIConfig(SingletonConfig):
    """Configuration for Azure OpenAI services.

    Manages settings for Azure OpenAI including:
    - API authentication
    - Model configurations
    - Batch processing parameters
    - Prompts for input and output models

    This is a singleton configuration to ensure consistent settings across the application.

    Attributes:
        client (ClientConfig): Client configuration
        model (ModelConfig): Model-specific settings
        max_workers (int): Maximum number of concurrent workers
        prompts (dict): Templates for source and target prompts
    """

    client: ClientConfig
    model: ModelConfig
    max_workers: int = 10
    prompts: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_config(cls, config: DictConfig) -> OpenAIConfig:
        """Create instance from OmegaConf config."""
        try:
            openai_config = config.get("openai", {})
            if not openai_config:
                raise ValueError("OpenAI configuration section is missing")

            client_config = ClientConfig.from_dict(openai_config.get("client", {}))
            if not client_config.api_key or not client_config.api_base:
                raise ValueError(
                    "OpenAI API credentials (api_key, base_url) must be specified in client config"
                )

            model_config = ModelConfig.from_dict(openai_config.get("model", {}))

            return cls.get_instance(
                client=client_config,
                model=model_config,
                max_workers=openai_config.get("max_workers", 10),
                prompts=openai_config.get("prompts", {}),
            )
        except Exception as e:
            raise ValueError("Invalid OpenAI configuration") from e

    def validate(self) -> None:
        """Validate OpenAI configuration."""
        if self.max_workers <= 0:
            raise ValueError("Number of workers must be positive")
        self.client.validate()
        self.model.validate()
