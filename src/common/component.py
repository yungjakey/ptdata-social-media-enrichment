from __future__ import annotations

from typing import Any, TypeVar

from src.common.config import BaseConfig

TConfig = TypeVar("TConfig", bound=BaseConfig)


class ComponentFactory:
    """Base class for configurable components."""

    _config: type[BaseConfig]

    def __init__(self, config: TConfig) -> None:
        """Initialize with configuration."""
        self._config = config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ComponentFactory:
        """Create a component from a configuration dictionary."""
        return cls(cls._config(**config))

    @property
    def config(self) -> TConfig:
        """Access the configuration."""
        return self._config
