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
    connector: dict[str, Any] = Field(..., description="Connector configuration")
    inference: dict[str, Any] = Field(..., description="Inference configuration")
    logging: dict[str, Any] | None = Field(default=None, description="Logging configuration")

    def __init__(self, **data):
        super().__init__(**data)
        if self.logging:
            self.setup_logging(self.logging)

    @classmethod
    def setup_logging(cls, config: dict[str, Any]) -> None:
        """Setup logging based on configuration."""
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)  # Allow all logs through at root

        formatter = logging.Formatter(
            fmt=config.get("format", "%(asctime)s %(levelname)s %(name)s %(message)s"),
            datefmt=config.get("datefmt", "%Y-%m-%d %H:%M:%S"),
        )

        # Configure handlers
        handlers = config.get("handlers", {})

        # Console handler
        if console_cfg := handlers.get("console"):
            console = logging.StreamHandler()
            console.setFormatter(formatter)
            console.setLevel(console_cfg.get("level", "INFO"))
            root_logger.addHandler(console)

        # File handler
        if file_cfg := handlers.get("file"):
            file_handler = RotatingFileHandler(
                filename=file_cfg.get("filename", "app.log"),
                maxBytes=file_cfg.get("maxBytes", 10_000_000),  # 10MB
                backupCount=file_cfg.get("backupCount", 5),
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(file_cfg.get("level", "DEBUG"))
            root_logger.addHandler(file_handler)


TConf = TypeVar("TConf", bound=BaseConfig)
