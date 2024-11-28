"""Base configuration classes and type definitions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

from omegaconf import DictConfig

logger = logging.getLogger(__name__)


# Core enums and type definitions
class Directions(Enum):
    """Source and sink enum."""

    Input = "source"
    Output = "target"


@dataclass(frozen=True)
class BaseConfig:
    """Base configuration with validation."""

    @classmethod
    def from_config(cls, **config: DictConfig) -> BaseConfig:
        """Create instance from OmegaConf config."""
        return cls(**config)

    def __post_init__(self) -> None:
        """Validate configuration on initialization."""
        self.validate()

    def validate(self) -> None:
        """Override to implement config validation."""
        pass
