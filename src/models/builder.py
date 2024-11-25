# src/models/builder.py
"""Dynamic model builder for input/output schemas."""

from __future__ import annotations

from typing import Any

from pydantic import create_model

from src.models.base import Base


class Builder:
    """Dynamic model builder for input/output schemas."""

    def __init__(self, name: str = "DynamicModel"):
        self.name = name
        self.fields: dict[str, tuple[type, Any]] = {}
        self._base: type[Base] = Base
        self._prompt_template: str | None = None
        self._primary_keys: list[str] | None = None

    def with_prompt(self, prompt_template: str) -> Builder:
        """Set prompt template for the model."""
        self._prompt_template = prompt_template
        return self

    def with_field(self, name: str, field_type: type | Builder, default: Any = ...) -> Builder:
        """Add field to schema."""
        if isinstance(field_type, Builder):
            field_type = field_type.build()
        self.fields[name] = (field_type, default)
        return self

    def from_schema(self, schema: dict[str, Any]) -> Builder:
        """Build schema from dictionary specification."""
        for field_name, field_spec in schema.items():
            if isinstance(field_spec, dict):
                # Nested schema
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
        model = create_model(
            self.name, __base__=self._base, __module__=self.__module__, **self.fields
        )

        if self._prompt_template is not None:
            model._BASE_PROMPT = self._prompt_template
        if self._primary_keys is not None:
            model.primary_keys = self._primary_keys

        return model
