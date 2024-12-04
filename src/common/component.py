from __future__ import annotations

from typing import Any

from src.common.config import BaseConfig, TConf


class ComponentFactory:
    """Base class for configurable components."""

    _config: type[BaseConfig]

    def __init__(self, config: TConf) -> None:
        """Initialize with configuration."""
        self._config = config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ComponentFactory:
        """Create a component from a configuration dictionary."""
        return cls(cls._config(**config))

    @property
    def config(self) -> TConf:
        """Access the configuration."""
        return self._config
