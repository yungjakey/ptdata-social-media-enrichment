"""Common package for social media analytics."""

import logging

from .types import BaseConfig, Directions


class LoggerConfig:
    """Logger configuration."""

    _default_level = "INFO"
    _default_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def __init__(self, level: str | None = None, format: str | None = None):
        """Initialize logger config."""
        self.level = level or self._default_level
        self.format = format or self._default_format

    def configure(self) -> None:
        """Configure logging with the specified settings."""
        logging.basicConfig(level=self.level, format=self.format)

    @classmethod
    def from_config(cls, **config) -> "LoggerConfig":
        """Create instance from config dictionary."""
        return cls(level=config.get("level", None), format=config.get("format", None))


__all__ = ["BaseConfig", "Directions", "LoggerConfig"]
