from __future__ import annotations

import os
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


def _get_partition_path(now: datetime, partitions: dict[str, type]) -> str:
    missing = {col: not hasattr(now, col) for col in partitions}

    if any(missing.values()):
        raise RuntimeError(f"Unknown partition columns: {', '.join(missing.keys())}")

    return os.path.join([f"{col}={getattr(now, col)}" for col in partitions])


class TypeConverter:
    @classmethod
    def _py_to_glue(cls, py_type: type[Any]) -> str:
        """Convert Python type to Glue type."""
        if py_type is int:
            return "int"
        elif py_type is float:
            return "double"
        elif py_type is bool:
            return "boolean"
        elif hasattr(py_type, "__origin__") and py_type.__origin__ is list:
            return f"array<{cls._py_to_glue(py_type.__args__[0])}>"
        elif hasattr(py_type, "__origin__") and py_type.__origin__ is dict:
            return f"map<{cls._py_to_glue(py_type.__args__[0])},{cls._py_to_glue(py_type.__args__[1])}>"
        else:
            return "string"

    @classmethod
    def pydantic2py(cls, model: type[BaseModel]) -> dict[str, type]:
        """Convert Pydantic model to PySpark schema."""
        return {name: field.annotations for name, field in model.model_fields()}

    @classmethod
    def py2glue(cls, py: dict[str, type]) -> list[dict[str, str]]:
        """Convert Pydantic model to Glue table schema."""
        columns = []
        for n, t in py.items():
            columns.append(
                {
                    "Name": n,
                    "Type": cls._py_to_glue(t),
                }
            )
        return columns


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
