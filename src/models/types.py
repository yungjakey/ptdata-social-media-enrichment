"""Type definitions for model building and validation."""

from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field


class Base(BaseModel):
    """Base model with prompt generation and key validation."""

    _BASE_PROMPT: ClassVar[str] = "{0}"
    primary_keys: list[str] = Field(default_factory=list, min_length=1)

    def __init__(self, **kwargs: type) -> None:
        if "primary_keys" not in kwargs:
            raise ValueError("primary_keys is required") from None

        missing = set(kwargs["primary_keys"]) - set(kwargs.keys())
        if missing:
            raise ValueError(f"Missing primary keys: {missing}") from None

        super().__init__(**kwargs)

    @classmethod
    def _prompt(cls, data: dict[str, type] | str) -> str:
        """Generate prompt string from data."""
        return str(data)

    @classmethod
    def prompt(cls, **kwargs: type) -> str:
        """Generate prompt from data."""
        return cls._BASE_PROMPT.format(**kwargs)

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
