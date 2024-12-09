from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, ClassVar

import pyarrow as pa

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class TypeMap:
    """Type mapping between PyArrow, Python, and SQL types."""

    name: str
    sql: str
    pa: pa.DataType
    py: type
    value: Any | None = None  # data value

    # Type mappings
    PY_TO_SQL: ClassVar[dict[type, str]] = {
        # Partition types
        int: "bigint",  # Default to bigint for integers
        str: "string",
        # Non-partition types
        float: "double",
        bool: "boolean",
        datetime: "timestamp",
        date: "date",
        bytes: "binary",
    }
    SQL_TO_PY: ClassVar[dict[str, type]] = {
        # Partition types
        "int": int,
        "string": str,
        # Non-partition types
        "double": float,
        "boolean": bool,
        "timestamp": datetime,
        "date": date,
        "binary": bytes,
    }
    PY_TO_PA: ClassVar[dict[type, pa.DataType]] = {
        # Partition types
        int: pa.int64(),  # Use int16 for partition integers
        str: pa.string(),
        # Non-partition types
        float: pa.float64(),
        bool: pa.bool_(),
        datetime: pa.timestamp("us"),
        date: pa.date32(),
        bytes: pa.binary(),
    }

    @classmethod
    def py2sql(cls, py_type: type) -> str:
        """Convert Python type to SQL type."""
        return cls.PY_TO_SQL.get(py_type, "string")

    @classmethod
    def sql2py(cls, sql_type: str) -> type:
        """Convert SQL type to Python type."""
        return cls.SQL_TO_PY.get(sql_type.lower(), str)

    @classmethod
    def py2pa(cls, py_type: type) -> pa.DataType:
        """Convert Python type to PyArrow type."""
        return cls.PY_TO_PA.get(py_type, pa.string())

    @classmethod
    def from_athena(cls, sql_type: str, value: str | None) -> Any:
        """Convert Athena value to Python type."""
        if not value:
            return None

        py_type = cls.sql2py(sql_type)
        try:
            if py_type == datetime:
                return datetime.fromisoformat(value)
            if py_type == date:
                return date.fromisoformat(value)
            return py_type(value)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to convert {value} to {py_type}: {e}")

    @classmethod
    def model2athena(cls, model: BaseModel) -> list[dict[str, str]]:
        """Convert Pydantic model fields to Athena columns."""
        return [
            {
                "Name": name,
                "Type": cls.py2sql(field.annotation),
            }
            for name, field in model.model_fields.items()
        ]

    @classmethod
    def athena2pyarrow(cls, columns: list[dict[str, str]]) -> pa.Schema:
        """Convert Athena columns to PyArrow schema."""
        fields = []
        for col in columns:
            name = col["Name"]
            sql_type = col["Type"]
            py_type = cls.sql2py(sql_type)
            pa_type = cls.py2pa(py_type)

            fields.append(pa.field(name, pa_type, nullable=True))
            logger.debug(f"Converting {name}: SQL={sql_type}, Python={py_type}, PyArrow={pa_type}")

        return pa.schema(fields)


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
    def terminal_states(cls) -> set[str]:
        """States where no further status changes will occur."""
        return {cls.SUCCEEDED, cls.FAILED, cls.CANCELLED}

    @classmethod
    def running_states(cls) -> set[str]:
        """States where the query is still running."""
        return {cls.QUEUED, cls.RUNNING}

    @classmethod
    def from_response(cls, status: dict[str, str]) -> QueryState:
        """Create QueryState from Athena response."""
        state = status.get("State", "")
        if not state:
            raise ValueError("No state in response")
        return cls(state)
