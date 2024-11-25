# src/models/builder.py
"""Dynamic model builder for input/output schemas."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import create_model

from src.common import Base

logger = logging.getLogger(__name__)


class Builder:
    """Dynamic model builder for input/output schemas."""

    def __init__(self, name: str = "DynamicModel"):
        self.name = name
        self.fields: dict[str, tuple[type, Any]] = {}
        self._base: type[Base] = Base
        self._prompt_template: str | None = None
        self._primary_keys: list[str] | None = None
        logger.debug("Initialized Builder for model: %s", name)

    def with_prompt(self, prompt_template: str) -> Builder:
        """Set prompt template for the model."""
        self._prompt_template = prompt_template
        logger.debug("Set prompt template for %s", self.name)
        return self

    def with_field(self, name: str, field_type: type | Builder, default: Any = ...) -> Builder:
        """Add field to schema."""
        if isinstance(field_type, Builder):
            logger.debug("Building nested model for field '%s' in %s", name, self.name)
            field_type = field_type.build()
        self.fields[name] = (field_type, default)
        logger.debug("Added field '%s' (%s) to %s", name, field_type.__name__, self.name)
        return self

    def from_schema(self, schema: dict[str, Any]) -> Builder:
        """Build schema from dictionary specification."""
        logger.info("Building %s from schema with %d fields", self.name, len(schema))
        for field_name, field_spec in schema.items():
            if isinstance(field_spec, dict):
                # Nested schema
                logger.debug("Creating nested schema for '%s' in %s", field_name, self.name)
                nested = Builder(field_name.title()).from_schema(field_spec)
                self.with_field(field_name, nested)
            else:
                # Simple field
                field_type, default = (
                    field_spec if isinstance(field_spec, tuple) else (field_spec, ...)
                )
                self.with_field(field_name, field_type, default)
        return self

    def build(self) -> type[Base]:
        """Create model from schema definition."""
        logger.info("Building model '%s' with %d fields", self.name, len(self.fields))
        model = create_model(
            self.name, __base__=self._base, __module__=self.__module__, **self.fields
        )

        if self._prompt_template is not None:
            model._BASE_PROMPT = self._prompt_template
            logger.debug("Applied custom prompt template to %s", self.name)
        if self._primary_keys is not None:
            model.primary_keys = self._primary_keys
            logger.debug("Set primary keys for %s: %s", self.name, self._primary_keys)

        logger.info("Successfully built model: %s", self.name)
        return model
