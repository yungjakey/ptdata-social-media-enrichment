"""Base connector component for data integration."""

from __future__ import annotations

import logging
from typing import Any

from src.common.component import ComponentFactory
from src.connectors.config import ConnectorConfig

logger = logging.getLogger(__name__)


class ConnectorComponent(ComponentFactory):
    """Base class for data connectors."""

    _config = ConnectorConfig

    async def connect(self) -> Any:
        """Establish connection."""
        raise NotImplementedError

    async def disconnect(self) -> None:
        """Close connection resources."""
        raise NotImplementedError

    async def read(self, *args: Any, **kwargs: Any) -> Any:
        """Read data from source."""
        raise NotImplementedError

    async def write(self, *args: Any, **kwargs: Any) -> Any:
        """Write data to destination."""
        raise NotImplementedError

    async def __aenter__(self):
        """Enter context."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        await self.disconnect()
