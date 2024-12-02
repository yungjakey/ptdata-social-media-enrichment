from dataclasses import dataclass
from typing import NamedTuple, Union

from dateitime import date, datetime
from pydantic import BaseModel


class Direction(NamedTuple):
    Input: str
    Output: str


class TypeConverter:
    """Converts Pydantic models to AWS Athena table schema definitions."""

    STRING = "string"
    INTEGER = "int"
    BIGINT = "bigint"
    SMALLINT = "smallint"
    TINYINT = "tinyint"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    VARCHAR = "varchar"

    ATHENA2PY = {
        str: (STRING, VARCHAR),
        int: (TINYINT, SMALLINT, INTEGER, BIGINT),
        float: (FLOAT, DOUBLE),
        bool: (BOOLEAN,),
        datetime: (TIMESTAMP,),
        date: (DATE,),
    }

    @classmethod
    def _py2athena(cls, annotation: type) -> str:
        if annotation in cls.ATHENA2PY:
            return cls.ATHENA2PY[annotation][0].value
        return cls.STRING.value

    @classmethod
    def py2athena(cls, model: type[BaseModel]) -> dict[str, str]:
        """Convert Pydantic model fields to Athena type strings."""
        schema = {}
        for name, field in model.model_fields.items():
            # Handle Optional and Union types
            annotation = field.annotation or str
            origin = getattr(annotation, "__origin__", annotation)

            # Extract the actual type for Optional and Union
            if origin in (Union, type(None)):
                type_args = getattr(annotation, "__args__", [str])
                annotation = next((arg for arg in type_args if arg is not type(None)), str)

            # Determine Athena type
            if origin in (list, set):
                # Recursively get the type of list/set elements
                value_type = cls._py2athena(annotation.__args__[0])
                schema[name] = f"array<{value_type}>"
            elif origin is dict:
                # For dictionaries, use string as a default
                key_type = cls._py2athena(annotation.__args__[0] if annotation.__args__ else str)
                value_type = cls._py2athena(annotation.__args__[1] if annotation.__args__ else str)
                schema[name] = f"map<{key_type},{value_type}>"
            else:
                schema[name] = cls._py2athena(annotation)

        return schema


class BaseConfig(BaseModel):
    type: str
    name: str
    params: dict[str, type]


@dataclass(frozen=True)
class RootConfig:
    Model: type[BaseConfig]
    Input: type[BaseConfig]
    Output: type[BaseConfig]
    Provider: type[BaseConfig]

    @classmethod
    def from_dict(cls, kwargs: dict[str, dict[str, type]]):
        return cls(**kwargs)
