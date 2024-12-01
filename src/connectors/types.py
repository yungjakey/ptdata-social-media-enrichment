"""Base connector types and configuration."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, TypeVar

from typing_extensions import runtime_checkable


class ConnectorType(str, Enum):
    """Supported connector types."""

    ATHENA = "athena"


@dataclass
class ConnectorConfig:
    """Base connector configuration."""

    type: ConnectorType
    name: str
    params: dict[str, Any]


@runtime_checkable
class ConnectorProtocol(Protocol):
    """Base connector protocol."""

    type: ConnectorType
    name: str
    params: dict[str, Any]

    def validate(self) -> None:
        """Validate connector configuration."""
        ...

    def connect(self) -> None:
        """Connect to the data source."""
        ...

    def disconnect(self) -> None:
        """Disconnect from the data source."""
        ...

    async def read(self) -> Any:
        """Read data from the data source."""
        ...

    async def write(self, records: Any) -> None:
        """Write data to the data source."""
        ...


T = TypeVar("T", bound=ConnectorProtocol)


class Connector:
    """Base connector class."""

    _registry: dict[ConnectorType, type[ConnectorProtocol]] = {}

    @classmethod
    def register(cls, connector_type: ConnectorType) -> Any:
        """Register a connector type."""

        def wrapper(connector_cls: type[T]) -> type[T]:
            cls._registry[connector_type] = connector_cls
            return connector_cls

        return wrapper

    @classmethod
    def from_config(cls, type: str, name: str, params: dict[str, Any]) -> ConnectorProtocol:
        """Create a connector from configuration."""
        try:
            connector_type = ConnectorType(type)
        except ValueError as e:
            raise ValueError(f"Unsupported connector type: {type}") from e

        if connector_type not in cls._registry:
            raise ValueError(f"No connector registered for type: {type}")

        config = ConnectorConfig(type=connector_type, name=name, params=params)

        connector_cls = cls._registry[connector_type]
        return connector_cls(config)
