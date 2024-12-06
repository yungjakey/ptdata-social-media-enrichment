from __future__ import annotations

import os
from datetime import datetime
from enum import Enum

import pyarrow as pa
from pydantic import BaseModel


def _get_partition_path(now: datetime, partitions: dict[str, type]) -> str:
    missing = {col: not hasattr(now, col) for col in partitions}

    if any(missing.values()):
        raise RuntimeError(f"Unknown partition columns: {', '.join(missing.keys())}")

    return os.path.join(*[f"{col}={getattr(now, col)}" for col in partitions])


class TypeConverter:
    @classmethod
    def _py_to_glue(cls, py_type: type) -> str:
        """Convert Python type to Glue type."""
        if py_type is int:
            return "int"
        elif py_type is float:
            return "double"
        elif py_type is bool:
            return "boolean"
        elif py_type is str:
            return "string"
        elif py_type is datetime:
            return "timestamp"
        else:
            raise ValueError(f"Unsupported type: {py_type}")

    @classmethod
    def _py_to_pa(cls, py_type: type) -> pa.DataType:
        """Convert Python type to PyArrow type."""
        if py_type is int:
            return pa.int64()
        elif py_type is float:
            return pa.float64()
        elif py_type is bool:
            return pa.bool_()
        elif py_type is str:
            return pa.string()
        elif py_type is datetime:
            return pa.timestamp("us")
        else:
            raise ValueError(f"Unsupported type: {py_type}")

    @classmethod
    def pydantic2py(cls, model: type[BaseModel]) -> dict[str, type]:
        """Convert Pydantic model to Python schema."""
        return {name: field.annotation for name, field in model.model_fields.items()}

    @classmethod
    def py2pa(cls, py: dict[str, type]) -> pa.Schema:
        """Convert Python schema to PyArrow schema."""
        return pa.schema([(name, cls._py_to_pa(type_)) for name, type_ in py.items()])

    @classmethod
    def py2glue(cls, py: dict[str, type]) -> list[dict[str, str]]:
        """Convert Python schema to Glue schema."""
        return [{"Name": name, "Type": cls._py_to_glue(type_)} for name, type_ in py.items()]


class QueryState(str, Enum):
    """Athena query states.
    Reference: https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatus.html
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @classmethod
    def terminal_states(cls) -> set[QueryState]:
        """States where no further status changes will occur."""
        return {cls.SUCCEEDED, cls.FAILED, cls.CANCELLED}

    @classmethod
    def running_states(cls) -> set[QueryState]:
        """States where query is still processing."""
        return {cls.QUEUED, cls.RUNNING}

    @classmethod
    def from_response(cls, status: dict[str, type]) -> QueryState:
        """Create QueryState from Athena API response."""
        try:
            state = status["State"]
            return cls(state)
        except (KeyError, ValueError) as e:
            raise Exception(f"Error parsing Athena query state: {status}.") from e
