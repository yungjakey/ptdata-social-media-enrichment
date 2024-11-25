# src/models/base.py
"""Base model for input and output schemas."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel


class Base(BaseModel):
    """Base model with prompt generation and key validation."""

    _BASE_PROMPT: ClassVar[str] = "{0}"
    primary_keys: ClassVar[list[str]] = []

    def __init__(self, **kwargs: Any) -> None:
        if "primary_keys" in kwargs:
            self.__class__.primary_keys = kwargs.pop("primary_keys")

        missing = set(self.primary_keys) - set(kwargs.keys())
        if missing:
            raise ValueError(f"Missing primary keys: {missing}") from None

        super().__init__(**kwargs)

    @classmethod
    def _prompt(cls, data: dict[str, Any] | str) -> str:
        """Generate prompt string from data."""
        return cls._BASE_PROMPT.format(data)

    class Config:
        """Pydantic config settings."""

        arbitrary_types_allowed = True
