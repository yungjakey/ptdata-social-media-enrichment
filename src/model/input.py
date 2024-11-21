"""Input models for social media analytics."""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar


@dataclass
class DynamicInput:
    """Dynamic input model that reflects database schema."""
    _data: dict[str, Any]
    _schema: ClassVar[dict[str, type]] = {}

    @classmethod
    def update_schema(cls, schema: dict[str, type]) -> None:
        """Update the schema definition."""
        cls._schema = schema

    def __getattr__(self, name: str) -> Any:
        """Get attribute from _data dict."""
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")

    def to_dict(self) -> dict[str, Any]:
        """Convert instance to dictionary."""
        result = {}
        for key, value in self._data.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DynamicInput":
        """Create instance from dictionary."""
        processed_data = {}
        for key, value in data.items():
            if key in cls._schema:
                field_type = cls._schema[key]
                if field_type == datetime and isinstance(value, str):
                    processed_data[key] = datetime.fromisoformat(value)
                else:
                    processed_data[key] = value
        return cls(_data=processed_data)


def create_input_model_from_schema(schema: dict[str, type]) -> type[DynamicInput]:
    """Create a new input model class from schema."""
    DynamicInput.update_schema(schema)
    return DynamicInput
