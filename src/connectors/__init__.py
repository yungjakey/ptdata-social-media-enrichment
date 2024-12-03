"""Connector module initialization."""

import logging
from dataclasses import dataclass
from enum import Enum
from importlib import import_module

from .types import BaseConnector

logger = logging.getLogger(__name__)


@dataclass
class ConnectorImplementation:
    """Connector implementation configuration."""

    name: str
    module_path: str | None = None
    client_class_name: str | None = None

    def __post_init__(self) -> None:
        """Set default module path and client name if not provided."""
        if not self.module_path:
            self.module_path = f"src.connectors.{self.name}"
            logger.debug(f"Using default module path: {self.module_path}")
        if not self.client_class_name:
            self.client_class_name = f"{self.name.upper()}Client"
            logger.debug(f"Using default client class name: {self.client_class_name}")

    def get_client_class(self) -> type[BaseConnector]:
        """Get the client class for this connector type."""
        try:
            module = import_module(self.module_path)
        except ImportError as e:
            raise ValueError(
                f"No module found for connector {self.name}: {self.module_path}"
            ) from e

        try:
            client_class = getattr(module, self.client_class_name)
        except AttributeError as e:
            raise ValueError(
                f"No client class {self.client_class_name} found in {self.module_path}"
            ) from e

        if not issubclass(client_class, BaseConnector):
            raise TypeError(f"Client class {self.client_class_name} must implement BaseConnector")

        return client_class


class Connector(Enum):
    """Available connector types."""

    aws = ConnectorImplementation(name="aws")

    @classmethod
    def registry(cls) -> list[str]:
        """Get list of available connector names."""
        return [member.name for member in cls]

    @classmethod
    def from_config(cls, kind: str, **kwargs) -> BaseConnector:
        """Create a connector instance from configuration."""
        if kind not in cls.__members__:
            raise ValueError(
                f"Unsupported connector type: {kind}. "
                f"Must be one of: {', '.join(cls.registry())}"
            )

        implementation = cls[kind].value
        client_class = implementation.get_client_class()
        return client_class(**kwargs)


__all__ = ["Connector"]
