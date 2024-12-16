from __future__ import annotations

from typing import Generic

from src.common.config import TConf


class ComponentFactory(Generic[TConf]):
    """Base class for configurable components."""

    # This is a class variable that will be set by subclasses
    _config_type: type[TConf]
    _instance_config: TConf

    def __init__(self, config: TConf) -> None:
        """Initialize with configuration."""
        self._instance_config = config

    @classmethod
    def from_config(cls, config: dict[str, type]) -> ComponentFactory:
        """Create a component from a configuration dictionary."""
        return cls(cls._config_type(**config))

    @property
    def config(self) -> TConf:
        """Access the configuration."""
        return self._instance_config
