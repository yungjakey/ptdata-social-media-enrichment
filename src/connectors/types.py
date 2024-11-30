from __future__ import annotations

import abc
from dataclasses import dataclass
from enum import Enum
from typing import Any

from src.common.types import BaseConfig


class Directions(Enum):
    """Source and sink enum."""

    Input = "source"
    Output = "target"


@dataclass(frozen=True)
class TableConfig(BaseConfig):
    """Configuration for a single table connection."""

    database: str
    query: str

    @classmethod
    def from_config(cls, database: str, query: str) -> TableConfig:
        return cls(database=database, query=query)


class ConnectorProtocol(abc.ABC):
    """Protocol for connector configurations."""

    @abc.abstractmethod
    def validate(self) -> bool:
        """Validate connector configuration."""
        pass

    @abc.abstractmethod
    def connect(self) -> Any:
        """Establish connection to the data source."""
        pass

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the data source."""
        pass


@dataclass
class ConnectorConfig(BaseConfig):
    """Configuration for database connections."""

    direction: Directions
    name: str
    database: str
    tables: list[TableConfig]
    query: str | None = None

    def validate(self) -> None:
        """Validate connector configuration."""
        super().validate()
        if not self.name:
            raise ValueError("Name must be specified")
        if not self.database:
            raise ValueError("Database must be specified")
        if not self.tables:
            raise ValueError("At least one table must be specified")
