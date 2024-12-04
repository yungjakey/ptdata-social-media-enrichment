"""Dynamic model builder."""

from typing import Literal

from pydantic import Field

from src.common.config import BaseConfig


class ModelConfig(BaseConfig):
    """Model configuration."""

    name: str = Field(
        ...,
        description="Model name",
        min_length=1,
    )

    prompt: str = Field(
        ...,
        description="Prompt template",
        min_length=1,
    )

    schema: dict[str, Literal["str", "int", "float", "bool"]] = Field(
        ...,
        description="Model schema defining output fields and their Python types",
        min_length=1,
    )

    @classmethod
    def _validate_schema_types(cls, v: dict) -> dict:
        """Validate schema field types."""
        valid_types = {"str", "int", "float", "bool"}

        for field, field_type in v.items():
            if not field:
                raise ValueError("Schema field cannot be empty")
            if field_type not in valid_types:
                raise ValueError(
                    f"Invalid schema field type: {field_type}. Must be one of {valid_types}"
                )

        return v
