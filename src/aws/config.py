"""AWS configuration classes."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from omegaconf import DictConfig

from src.common import DataConfig, SingletonConfig

logger = logging.getLogger(__name__)


class CategoryEnum(str, Enum):
    """Base class for categorized enums."""

    @property
    def category(self) -> type:
        """Get category of enum value."""
        return self.value


class State(CategoryEnum):
    """Enum for operation states with categories."""

    class Category(str, Enum):
        OK = "ok"
        NOK = "nok"
        PENDING = "pending"

    # ok states
    SUCCEEDED = Category.OK
    COMPLETED = Category.OK

    # nok states
    CANCELLED = Category.NOK
    FAILED = Category.NOK

    # pending states
    STARTING = Category.PENDING
    RUNNING = Category.PENDING


@dataclass(frozen=True)
class TableConfig(DataConfig):
    """Configuration for a single table."""

    database: str
    table: str
    primary: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TableConfig:
        """Create instance from dictionary."""
        logger.debug("Creating TableConfig from data: %s", data)
        return cls(**data)

    def validate(self) -> None:
        """Validate table configuration."""
        if not all([self.database, self.table]):
            logger.error("Invalid table config: missing database or table name")
            raise ValueError("Database and table name are required")
        logger.debug("Validated TableConfig: %s.%s", self.database, self.table)


@dataclass(frozen=True)
class DBConfig(DataConfig):
    """Database configuration with source and target tables."""

    source: list[TableConfig]
    target: list[TableConfig]
    primary: list[str] = field(default_factory=list)
    max_retries: int | None = None
    wait_time: int | None = None
    max_wait_time: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DBConfig:
        """Create instance from dictionary."""
        logger.debug("Creating DBConfig from data")
        config_data = data.copy()
        config_data["source"] = [TableConfig.from_dict(s) for s in data.get("source", [])]
        config_data["target"] = [TableConfig.from_dict(t) for t in data.get("target", [])]
        return cls(**config_data)

    def validate(self) -> None:
        """Validate database configuration."""
        logger.debug(
            "Validating DBConfig with %d sources and %d targets",
            len(self.source),
            len(self.target),
        )
        if not self.source or not self.target:
            logger.error("Invalid DB config: missing sources or targets")
            raise ValueError("At least one source and target table required")

        for table in self.source + self.target:
            table.validate()
        logger.info("Successfully validated DBConfig")


@dataclass(frozen=True)
class AWSConfig(SingletonConfig):
    """AWS service configuration."""

    output: str
    region: str = "eu-central-1"
    db: DBConfig | None = None

    @classmethod
    def from_config(cls, config: DictConfig) -> AWSConfig:
        """Create instance from OmegaConf config."""
        logger.debug("Creating AWSConfig from config")
        config_data = {
            "output": config.get("output"),
            "region": config.get("region", "eu-central-1"),
        }

        if db_config := config.get("db"):
            config_data["db"] = DBConfig.from_dict(db_config)

        return cls.get_instance(**config_data)

    def validate(self) -> None:
        """Validate AWS configuration."""
        logger.debug("Validating AWSConfig")
        if not self.output:
            logger.error("Invalid AWS config: missing output path")
            raise ValueError("Output path is required")

        if self.db:
            self.db.validate()

        logger.info("Successfully validated AWSConfig")
