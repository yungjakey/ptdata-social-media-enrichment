"""Base classes."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar

from omegaconf import DictConfig
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Base(BaseModel):
    """Base model with prompt generation and key validation."""

    _BASE_PROMPT: ClassVar[str] = "{0}"
    primary_keys: ClassVar[list[str]] = []

    def __init__(self, **kwargs: type) -> None:
        if "primary_keys" in kwargs:
            self.__class__.primary_keys = kwargs.pop("primary_keys")

        missing = set(self.primary_keys) - set(kwargs.keys())
        if missing:
            raise ValueError(f"Missing primary keys: {missing}") from None

        super().__init__(**kwargs)

    @classmethod
    def _prompt(cls, data: dict[str, type] | str) -> str:
        """Generate prompt string from data."""
        return cls._BASE_PROMPT.format(data)

    class Config:
        """Pydantic config settings."""

        arbitrary_types_allowed = True


@dataclass(frozen=True)
class DataConfig:
    """Base class for data structure configurations."""

    def validate(self) -> None:
        """Override to implement data validation."""
        pass

    def __post_init__(self) -> None:
        self.validate()


@dataclass(frozen=True)
class SingletonConfig:
    """Base class for singleton configurations."""

    _instance: ClassVar[SingletonConfig | None] = None
    _created_at: datetime = field(default_factory=datetime.now, init=False)

    @classmethod
    def get_instance(cls, **kwargs) -> SingletonConfig:
        """Get or create singleton instance."""
        if cls._instance is None:
            cls._instance = cls(**kwargs)
            logger.debug("Created new %s instance", cls.__name__)
        return cls._instance

    @classmethod
    def from_config(cls, config: DictConfig) -> SingletonConfig:
        """Create instance from OmegaConf config."""
        raise NotImplementedError("Subclasses must implement from_config")

    def __post_init__(self) -> None:
        """Validate configuration on initialization."""
        self.validate()

    def validate(self) -> None:
        """Override to implement config validation."""
        pass


@dataclass(frozen=True)
class LoggerConfig(SingletonConfig):
    """Configuration for application logging."""

    version: int
    disable_existing_loggers: bool
    formatters: dict
    handlers: dict
    root: dict

    @classmethod
    def from_config(cls, config: DictConfig) -> LoggerConfig:
        """Create instance from OmegaConf config."""
        return cls.get_instance(**config.logging)

    def validate(self) -> None:
        """Validate logger configuration."""
        required_fields = {"version", "formatters", "handlers", "root"}
        config_fields = set(self.__dict__.keys()) - {"_created_at"}

        missing = required_fields - config_fields
        if missing:
            raise ValueError(f"Missing required logger config fields: {missing}")

    def configure(self) -> None:
        """Apply logging configuration."""
        import logging.config

        logging.config.dictConfig(self.__dict__)
        logger.debug("Configured logging with %s", self.__class__.__name__)
