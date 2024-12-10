from __future__ import annotations

import logging
from datetime import date, datetime
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


class IcebergConverter:
    """Converter for Pydantic models to various data formats including Iceberg."""

    PY_TO_ICEBERG = {
        bool: BooleanType(),
        str: StringType(),
        int: LongType(),
        float: DoubleType(),
        datetime: TimestampType(),
        date: DateType(),
        bytes: BinaryType(),
    }

    PA_TO_PY = {
        pa.int64(): int,
        pa.string(): str,
        pa.float64(): float,
        pa.bool_(): bool,
        pa.timestamp("us"): datetime,
        pa.date32(): date,
        pa.binary(): bytes,
    }

    def __init__(
        self,
        model: type[BaseModel],
        datetime_field: str,
        partition_transforms: list[str],
        additional_fields: dict[str, type] | None = None,
    ):
        self.model = model
        self._datetime_field = datetime_field
        self._partition_transforms = partition_transforms
        self._additional_fields = (
            {
                f: self._get_field_type(field_type, self.PA_TO_PY)
                for f, field_type in additional_fields.items()
            }
            if additional_fields
            else {}
        )

    def _get_field_type(self, field_type: type, type_mapping: dict) -> Any:
        """Get the corresponding type from a mapping, handling Optional types."""
        # Handle Optional types
        origin = getattr(field_type, "__origin__", None)
        if origin is Optional:
            field_type = field_type.__args__[0]

        # Handle pyarrow types directly
        if isinstance(field_type, pa.DataType):
            return type_mapping.get(field_type, str)

        # For Python types, ensure we're using the type object itself
        if isinstance(field_type, type):
            return type_mapping.get(field_type, str)

        # Default fallback
        return type_mapping.get(str, str)

    def to_iceberg_schema(self) -> Schema:
        """Convert to Iceberg schema."""
        fields = []

        # Add timestamp field first
        fields.append(NestedField(1, self._datetime_field, TimestampType(), required=True))

        # Add model fields
        for idx, (name, field) in enumerate(self.model.model_fields.items(), start=2):
            field_type = field.annotation
            required = False  # Make all fields optional to match PyArrow schema

            iceberg_type = self._get_field_type(field_type, self.PY_TO_ICEBERG)
            fields.append(NestedField(idx, name, iceberg_type, required=required))

        # Add additional fields
        for name, field_type in self._additional_fields.items():
            iceberg_type = self._get_field_type(field_type, self.PY_TO_ICEBERG)
            fields.append(NestedField(len(fields) + 1, name, iceberg_type, required=False))

        return Schema(*fields)

    def to_partition_spec(self, schema: Schema) -> PartitionSpec:
        """Create partition spec for the given transforms."""
        if not self._partition_transforms:
            return PartitionSpec()

        # Create a single partition field for each transform
        partition_fields = []
        next_field_id = len(schema.fields) + 1

        for transform in self._partition_transforms:
            partition_fields.append(
                PartitionField(
                    source_id=1,  # Datetime is always field 1
                    field_id=next_field_id,
                    transform=transform,
                    name=transform,
                )
            )
            next_field_id += 1

        return PartitionSpec(*partition_fields)
