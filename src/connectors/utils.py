from __future__ import annotations

import logging
from datetime import date, datetime
from enum import Enum
from typing import Any, Optional

import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

QUERY_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {{ table_name }} (
    {%- for column in columns %}
    {{ column.Name }} {{ column.Type }}{% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- if partitions %}
PARTITIONED BY (
    {%- for partition in partitions %}
    {{ partition }}{% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- endif %}
LOCATION '{{ output_location }}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression' = 'snappy',
    'vacuum_min_snapshots_to_keep' = '10',
    'vacuum_max_snapshot_age_seconds' = '604800'
)
"""


class IcebergConverter:
    """Converter for Pydantic models to various data formats including Iceberg."""

    PY_TO_SQL = {
        int: "bigint",
        str: "string",
        float: "double",
        bool: "boolean",
        datetime: "timestamp",
        date: "date",
        bytes: "binary",
    }

    PY_TO_PA = {
        int: pa.int64(),
        str: pa.string(),
        float: pa.float64(),
        bool: pa.bool_(),
        datetime: pa.timestamp("us"),
        date: pa.date32(),
        bytes: pa.binary(),
    }

    PY_TO_ICEBERG = {
        bool: BooleanType(),
        str: StringType(),
        int: LongType(),
        float: DoubleType(),
        datetime: TimestampType(),
        date: DateType(),
        bytes: BinaryType(),
    }

    SQL_TO_PY = {
        "int": int,
        "bigint": int,
        "string": str,
        "double": float,
        "boolean": bool,
        "timestamp": datetime,
        "date": date,
        "binary": bytes,
    }

    def __init__(
        self,
        model: type[BaseModel],
        datetime_field: str,
        partition_transforms: list[str],
    ):
        self.model = model
        self._datetime_field = datetime_field
        self._partition_transforms = partition_transforms

    def _get_field_type(self, field_type: type, type_mapping: dict) -> Any:
        """Get the corresponding type from a mapping, handling Optional types."""
        # Handle Optional types
        origin = getattr(field_type, "__origin__", None)
        if origin is Optional:
            field_type = field_type.__args__[0]
        return type_mapping.get(field_type, type_mapping[str])  # Default to string type

    def to_iceberg_schema(self) -> Schema:
        """Convert to Iceberg schema."""
        fields = []

        # Add model fields
        for idx, (name, field) in enumerate(self.model.model_fields.items(), start=1):
            field_type = field.annotation
            required = field.is_required()

            # Handle Optional types
            if hasattr(field_type, "__origin__") and field_type.__origin__ is Optional:
                field_type = field_type.__args__[0]
                required = False

            iceberg_type = self._get_field_type(field_type, self.PY_TO_ICEBERG)
            fields.append(NestedField(idx, name, iceberg_type, required=required))

        # Add timestamp field if not present
        if self._datetime_field and self._datetime_field not in self.model.model_fields:
            fields.append(
                NestedField(len(fields) + 1, self._datetime_field, TimestampType(), required=True)
            )

        return Schema(*fields)

    def to_partition_spec(self, schema: Schema) -> PartitionSpec:
        """Create partition spec on the datetime field."""
        if not self._datetime_field:
            return PartitionSpec()  # Empty partition spec if no datetime field

        field_id = next(f.field_id for f in schema.fields if f.name == self._datetime_field)
        return PartitionSpec(
            *[
                PartitionField(
                    source_id=field_id,
                    field_id=len(schema.fields) + idx + 1,
                    transform=transform,
                    name=transform,
                )
                for idx, transform in enumerate(self._partition_transforms)
            ]
        )

    def to_arrow_schema(self) -> pa.Schema:
        """Convert to PyArrow schema."""
        fields = []

        # Add model fields
        for name, field in self.model.model_fields.items():
            pa_type = self._get_field_type(field.annotation, self.PY_TO_PA)
            fields.append(pa.field(name, pa_type, nullable=not field.is_required()))

        # Add timestamp field if not present
        if self._datetime_field and self._datetime_field not in self.model.model_fields:
            fields.append(pa.field(self._datetime_field, pa.timestamp("us"), nullable=False))

        return pa.schema(fields)

    def to_athena_columns(self) -> list[dict[str, str]]:
        """Convert to Athena columns."""
        columns = [
            {
                "Name": name,
                "Type": self._get_field_type(field.annotation, self.PY_TO_SQL),
            }
            for name, field in self.model.model_fields.items()
        ]

        # Add timestamp column if not present
        if self._datetime_field and self._datetime_field not in self.model.model_fields:
            columns.append({"Name": self._datetime_field, "Type": "timestamp"})

        return columns

    @classmethod
    def athena2py(cls, athena_type: str) -> type:
        """Convert Athena type to Python type."""
        return cls.SQL_TO_PY.get(athena_type.lower(), str)


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
