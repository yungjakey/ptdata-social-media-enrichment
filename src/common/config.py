"""Configuration classes."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar

from omegaconf import DictConfig

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
class DataConfig:
    """Base class for data structure configurations."""

    def validate(self) -> None:
        """Override to implement data validation."""
        pass

    def __post_init__(self) -> None:
        self.validate()


@dataclass(frozen=True)
class TableConfig(DataConfig):
    """Configuration for a single table."""

    database: str
    table: str
    primary: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TableConfig:
        """Create instance from dictionary."""
        return cls(database=data["database"], table=data["table"], primary=data.get("primary", []))

    def validate(self) -> None:
        """Validate table configuration."""
        if not self.database or not self.table:
            raise ValueError("Database and table names are required")
        if self.primary and not all(isinstance(key, str) for key in self.primary):
            raise ValueError("Primary keys must be strings")


@dataclass(frozen=True)
class DBConfig(DataConfig):
    """Database configuration with source and target tables."""

    sources: list[TableConfig]
    targets: list[TableConfig]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DBConfig:
        """Create instance from dictionary."""
        return cls(
            sources=[TableConfig.from_dict(src) for src in data.get("sources", [])],
            targets=[TableConfig.from_dict(tgt) for tgt in data.get("targets", [])],
        )

    def validate(self) -> None:
        """Validate database configuration."""
        if not self.sources or not self.targets:
            raise ValueError("Source and target tables must be specified")


@dataclass(frozen=True)
class ModelConfig:
    """OpenAI model configuration."""

    temperature: float
    max_tokens: int
    api_version: str = "2024-03-01"
    engine: str = "gpt-4-turbo"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ModelConfig:
        """Create instance from dictionary."""
        return cls(
            temperature=data.get("temperature", 0.7),
            max_tokens=data.get("max_tokens", 2048),
            api_version=data.get("api_version", "2024-03-01"),
            engine=data.get("engine", "gpt-4-turbo"),
        )

    def validate(self) -> None:
        """Validate model configuration."""
        if not 0 <= self.temperature <= 2:
            raise ValueError("Temperature must be between 0 and 2")
        if self.max_tokens <= 0:
            raise ValueError("Max tokens must be positive")

    def __post_init__(self) -> None:
        self.validate()

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary format."""
        return {
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "api_version": self.api_version,
            "engine": self.engine,
        }


@dataclass(frozen=True)
class AWSConfig(SingletonConfig):
    """AWS service configuration."""

    output: str
    region: str = "eu-central-1"
    max_retries: int = 10
    wait_time: int = 1
    max_wait_time: int = 60
    primary: list[str] = field(default_factory=list)
    db: DBConfig | None = None

    @classmethod
    def from_config(cls, config: DictConfig) -> AWSConfig:
        """Create instance from OmegaConf config."""
        aws_config = config.get("aws", {})
        return cls.get_instance(
            output=aws_config.get("output", ""),
            region=aws_config.get("region", "eu-central-1"),
            max_retries=aws_config.get("max_retries", 10),
            wait_time=aws_config.get("wait_time", 1),
            max_wait_time=aws_config.get("max_wait_time", 60),
            primary=aws_config.get("primary", []),
            db=DBConfig.from_dict(aws_config.get("db", {})) if "db" in aws_config else None,
        )

    def validate(self) -> None:
        """Validate AWS configuration."""
        if not self.output:
            raise ValueError("Output location must be specified")
        if self.max_retries < 1:
            raise ValueError("Max retries must be positive")
        if self.wait_time < 0:
            raise ValueError("Wait time cannot be negative")
        if self.max_wait_time < self.wait_time:
            raise ValueError("Max wait time must be greater than wait time")
        if self.db:
            self.db.validate()


@dataclass(frozen=True)
class OpenAIConfig(SingletonConfig):
    """OpenAI service configuration."""

    api_key: str
    base_url: str
    max_workers: int = 10
    model: ModelConfig = field(
        default_factory=lambda: ModelConfig(temperature=0.7, max_tokens=2048)
    )
    prompts: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_config(cls, config: DictConfig) -> OpenAIConfig:
        """Create instance from OmegaConf config."""
        openai_config = config.get("openai", {})
        client_config = openai_config.get("client", {})

        return cls.get_instance(
            api_key=client_config.get("api_key", ""),
            base_url=client_config.get("base_url", ""),
            max_workers=openai_config.get("max_workers", 10),
            model=ModelConfig.from_dict(
                {
                    **openai_config.get("model", {}),
                    "api_version": client_config.get("api_version", "2024-03-01"),
                    "engine": client_config.get("engine", "gpt-4-turbo"),
                }
            ),
            prompts=openai_config.get("prompts", {}),
        )

    def validate(self) -> None:
        """Validate OpenAI configuration."""
        if not self.api_key or not self.base_url:
            raise ValueError("API credentials must be specified")
        if self.max_workers <= 0:
            raise ValueError("Number of workers must be positive")
        self.model.validate()


@dataclass(frozen=True)
class RootConfig(SingletonConfig):
    """Root configuration that encapsulates all other configs."""

    aws: AWSConfig
    openai: OpenAIConfig

    @classmethod
    def from_config(cls, config: DictConfig) -> RootConfig:
        """Create instance from OmegaConf config."""

        return cls.get_instance(
            aws=AWSConfig.from_config(config), openai=OpenAIConfig.from_config(config)
        )

    def validate(self) -> None:
        """Validate all configurations."""
        self.aws.validate()
        self.openai.validate()
