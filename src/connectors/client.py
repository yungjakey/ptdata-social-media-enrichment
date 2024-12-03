"""Connector type definitions and protocols."""

from __future__ import annotations

from abc import abstractmethod, abstractproperty
from collections.abc import AsyncGenerator
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from src.connectors.types import ConnectorConfig


@runtime_checkable
class BaseConnector(Protocol):
    """Protocol defining the base interface for data source connectors."""

    @abstractproperty
    def config(self) -> ConnectorConfig: ...

    @abstractproperty
    def _session(self) -> type: ...

    @abstractproperty
    def validate(self) -> None: ...

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def read(self, **kwargs) -> AsyncGenerator[BaseModel]: ...

    @abstractmethod
    async def write(self, records: AsyncGenerator[BaseModel], **kwargs) -> None: ...

    async def __aenter__(self) -> BaseConnector:
        """Enter async context manager."""
        try:
            await self.connect()
        except Exception as e:
            raise RuntimeError(f"Error connecting {self.__name__}") from e
        return self

    async def __aexit__(
        self, exc_type: type | None, exc: Exception | None, tb: type | None
    ) -> None:
        """Exit async context manager."""
        try:
            await self.disconnect()
        except Exception as e:
            raise RuntimeError(f"Error disconnecting {self.__name__}") from e
