"""Model builder for processing and transforming data."""

from __future__ import annotations

import logging
from typing import Any, get_args, get_origin

from pydantic import BaseModel, create_model

from src.common.component import ComponentFactory
from src.models.types import ModelConfig

logger = logging.getLogger(__name__)


class ModelBuilder(ComponentFactory):
    """Manages model configuration and building."""

    _config = ModelConfig

    def __init__(self, config: ModelConfig) -> None:
        """Initialize model builder."""
        super().__init__(config)
        self._dynamic_fields: dict[str, Any] = {}

    def with_field(
        self,
        name: str,
        field_type: Any,
        default: Any = ...,
    ) -> ModelBuilder:
        """Add field to model with type resolution."""

        if get_origin(field_type) is list:
            inner_type = get_args(field_type)[0]

            if isinstance(inner_type, dict):
                field_type = list[self.from_schema(f"{name}_List", inner_type)]
            elif isinstance(inner_type, ModelBuilder):
                field_type = list[inner_type.build()]

        if isinstance(field_type, dict):
            field_type = self.from_schema(f"{name}_Model", field_type)

        if isinstance(field_type, ModelBuilder):
            field_type = field_type.build()

        self._dynamic_fields[name] = (field_type, default)
        return self

    @staticmethod
    def from_config(config: dict[str, type]) -> ModelBuilder:
        """Start building dynamic model."""
        builder = super().from_config(config)

        if builder.config.schema:
            for field_name, field_type in builder.config.schema.items():
                builder.with_field(field_name, field_type)

        return builder

    def build(self) -> type[BaseModel]:
        """Build dynamic model."""

        return create_model(
            self.config.name,
            __base__=BaseModel,
            __module__=BaseModel.__module__,
            __doc__=self.config.doc,
            **self._dynamic_fields,
        )
