"""Dynamic model builder for input/output schemas."""

from __future__ import annotations

import logging

from pydantic import Field, create_model

from src.azure.types import OpenAIConfig
from src.models.types import Base

logger = logging.getLogger(__name__)


class Builder:
    """Dynamic model builder for input/output schemas."""

    def __init__(self, name: str = "DynamicModel"):
        self.name = name
        self.fields: dict[str, tuple[type, type]] = {}
        self._base: type[Base] = Base
        self._prompt_template: str | None = None
        self._primary_keys: list[str] | None = None
        self._openai_config: OpenAIConfig | None = None
        logger.debug(f"Initialized Builder for model: {self.name}")

    def with_prompt(self, prompt_template: str) -> Builder:
        """Set prompt template for the model."""
        self._prompt_template = prompt_template
        return self

    def with_field(self, name: str, field_type: type | Builder, default: type = ...) -> Builder:
        """Add field to schema."""
        if isinstance(field_type, Builder):
            logger.debug("Building nested model for field '%s' in %s", name, self.name)
            field_type = field_type.build()
        self.fields[name] = (field_type, default)
        return self

    def with_base(self, base: type[Base]) -> Builder:
        """Set base class for the model."""
        self._base = base
        return self

    def with_primary_keys(self, keys: list[str]) -> Builder:
        """Set primary keys for the model."""
        self._primary_keys = keys
        return self

    def with_openai_config(self, config: OpenAIConfig) -> Builder:
        """Set OpenAI configuration for the model."""
        self._openai_config = config
        return self

    def from_schema(self, schema: dict[str, type]) -> Builder:
        """Build schema from dictionary."""
        logger.info("Building %s from schema with %d fields", self.name, len(schema))
        for field_name, field_spec in schema.items():
            if isinstance(field_spec, dict):
                # Nested schema
                logger.debug("Creating nested schema for '%s' in %s", field_name, self.name)
                nested = Builder(field_name.title()).from_schema(field_spec)
                self.with_field(field_name, nested)
            else:
                # Simple field
                field_type = field_spec if isinstance(field_spec, type) else type(field_spec)
                self.with_field(field_name, field_type)

        return self

    def build(self) -> type[Base]:
        """Build and return the Pydantic model."""
        namespace = {}
        if self._prompt_template:
            namespace["_BASE_PROMPT"] = self._prompt_template

        # Prepare fields, ensuring primary_keys is handled correctly
        fields = self.fields.copy()

        # If primary_keys are specified, add them to the fields
        if self._primary_keys:
            fields["primary_keys"] = (list[str], Field(default=self._primary_keys, min_length=1))

        # Create model with base functionality
        model = create_model(
            self.name,
            __base__=self._base,
            **fields,
            __module__=self._base.__module__,
            **namespace,
        )

        # Add OpenAI configuration if provided
        if self._openai_config:
            model._openai_config = self._openai_config

        logger.info("Built model %s with %d fields", self.name, len(fields))
        return model
