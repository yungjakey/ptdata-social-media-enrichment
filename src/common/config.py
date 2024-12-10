"""Common configuration classes."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field


class BaseConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        from_attributes=True,
    )


class RootConfig(BaseConfig):
    """Root configuration."""

    connector: dict[str, Any] = Field(
        ...,
        description="AWS connector configuration",
    )
    inference: dict[str, Any] = Field(
        ...,
        description="Inference configuration",
    )
    logging: dict[str, Any] | None = Field(
        default=None,
        description="Logging configuration",
    )

    def __init__(self, **data):
        """Initialize root config."""
        super().__init__(**data)
        if self.logging:
            self.setup_logging(self.logging)

    @classmethod
    def setup_logging(cls, config: dict[str, Any]) -> None:
        """Setup logging based on configuration."""
        level = config.get("level", "INFO")
        format = config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        filename = config.get("filename")

        # Configure root logger
        logging.basicConfig(level=level, format=format)

        # Add file handler if filename specified
        if filename:
            handler = RotatingFileHandler(
                filename=filename,
                maxBytes=config.get("max_bytes", 10 * 1024 * 1024),  # Default 10MB
                backupCount=config.get("backup_count", 5),
            )
            handler.setFormatter(logging.Formatter(format))
            logging.getLogger().addHandler(handler)


TConf = TypeVar("TConf", bound=BaseConfig)
