"""Model and prompt builders."""

from __future__ import annotations

import json
import logging

from pydantic import create_model

from src.common.types import Base

logger = logging.getLogger(__name__)


class ModelBuilder:
    """Dynamic model builder."""

    def __init__(self, name: str = "DynamicModel"):
        self.name = name
        self.fields: dict[str, tuple[type, type]] = {}
        self._base: type[Base] = Base
        self._model_doc: str | None = None

        logger.debug(f"Initialized Builder for model: {self.name}")

    def with_field(
        self, name: str, field_type: type | ModelBuilder, default: type = ...
    ) -> ModelBuilder:
        """Add field to schema."""
        logger.debug(f"Adding field {name} with type {field_type}")

        if isinstance(field_type, ModelBuilder):
            field_type = field_type.build()

        self.fields[name] = (field_type, default)
        return self

    def from_schema(self, schema: dict[str, type]) -> ModelBuilder:
        """Build schema from dictionary."""
        logger.info(f"Building model from schema: {json.dumps(schema, indent=4)}")

        for field_name, field_spec in schema.items():
            if isinstance(field_spec, dict):
                nested = ModelBuilder(field_name.title()).from_schema(field_spec)
                self.with_field(field_name, nested)
            else:
                field_type = field_spec if isinstance(field_spec, type) else type(field_spec)
                self.with_field(field_name, field_type)

        return self

    def with_prompt(self, prompt_template: str) -> ModelBuilder:
        """Set prompt template as model's documentation."""
        self._model_doc = prompt_template
        return self

    def build(self) -> type[Base]:
        """Build and return the Pydantic model."""
        model = create_model(
            self.name,
            __base__=self._base,
            __module__=self._base.__module__,
            __doc__=self._model_doc,
            **self.fields,
        )

        logger.info(f"Built model: {model} with fields: {json.dumps(self.fields)}")
        return model
