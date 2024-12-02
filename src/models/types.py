"""Dynamic model builder."""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field, create_model

from src.common import BaseConfig

logger = logging.getLogger(__name__)


class ModelParams(BaseConfig):
    """Model parameters."""

    prompt: str = Field(..., min_length=1, description="Prompt template")
    schema: dict[str, type] = Field(..., description="Model schema")


class ModelConfig(BaseConfig):
    """Model parameters."""

    name: str
    params: ModelParams
    type: str = "inference"

    @classmethod
    def from_dict(cls, name: str, params: dict[str, type]):
        try:
            params = ModelParams.from_dict(**params)
        except Exception as e:
            raise ValueError(f"Invalid model parameters: {e}") from e
        return cls(name=name, params=params)


class ModelBuilder:
    """Dynamic model builder."""

    def __init__(self, name: str = "DynamicModel"):
        self.name = name
        self.fields: dict[str, tuple[type, type]] = {}
        self._base: type[BaseModel] = BaseModel
        self._model_doc: str | None = None

        logger.debug(f"Initialized Builder for model: {self.name}")

    @classmethod
    def with_field(
        cls, name: str, field_type: type | ModelBuilder, default: type = ...
    ) -> ModelBuilder:
        """Add field to schema."""
        logger.debug(
            f"Adding field {name} with type {field_type.__name__ if isinstance(field_type, type) else field_type.name}"
        )

        if isinstance(field_type, ModelBuilder):
            field_type = field_type.build()

        cls.fields[name] = (field_type, default)
        return cls

    @classmethod
    def from_schema(cls, schema: dict[str, type]) -> ModelBuilder:
        """Build schema from dictionary."""
        logger.info(f"Building model from schema with fields: {list(schema.keys())}")

        for field_name, field_spec in schema.items():
            if isinstance(field_spec, dict):
                nested = cls(field_name.title()).from_schema(field_spec)
                cls.with_field(field_name, nested)
            else:
                field_type = field_spec if isinstance(field_spec, type) else type(field_spec)
                cls.with_field(field_name, field_type)

        return cls

    @classmethod
    def with_prompt(cls, prompt_template: str) -> ModelBuilder:
        """Set prompt template as model's documentation."""
        cls._model_doc = prompt_template
        return cls

    @classmethod
    def from_config(cls, dict_config: dict[str, type]) -> ModelBuilder:
        try:
            config = ModelConfig.from_dict(**dict_config)
        except Exception as e:
            raise ValueError(f"Invalid model configuration: {e}") from e

        return cls.from_schema(config.params.schema).with_prompt(config.params.prompt)

    def build(self) -> type[BaseModel]:
        """Build and return the Pydantic model."""
        model = create_model(
            self.name,
            __base__=self._base,
            __module__=self._base.__module__,
            __doc__=self._model_doc,
            **self.fields,
        )

        logger.info(f"Built model: {model.__name__} with fields: {list(self.fields.keys())}")
        return model
