from __future__ import annotations

import json
from datetime import date, datetime

import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.transforms import DayTransform, MonthTransform, YearTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        # Handle datetime
        if isinstance(obj, datetime):
            return obj.isoformat()

        # Handle bytes
        elif isinstance(obj, bytes):
            try:
                return obj.decode("ascii")
            except UnicodeDecodeError:
                return obj.hex()

        # Handle other types by falling back to the parent method
        return super().default(obj)


class ArrowConverter:
    @staticmethod
    def _get_arrow_type(python_type):
        if python_type is int:
            return pa.int64()
        if python_type is str:
            return pa.string()
        if python_type is float:
            return pa.float64()
        if python_type is bool:
            return pa.bool_()
        if python_type is datetime:
            return pa.timestamp("us")
        if python_type is date:
            return pa.date32()
        if python_type is bytes:
            return pa.binary()

        return pa.string()

    @classmethod
    def to_dict(cls, model: type[BaseModel]) -> dict[str, type]:
        """Convert Pydantic model to Arrow schema."""
        return [
            (name, cls._get_arrow_type(field.annotation))
            for name, field in model.model_fields.items()
        ]

    @classmethod
    def to_arrow_schema(cls, model: type[BaseModel]) -> pa.Schema:
        return pa.schema(cls.to_dict(model))


class IcebergConverter:
    """Converter for Pydantic models to various data formats including Iceberg."""

    PARTITION_TRANSFORMATIONS = {
        "year": YearTransform(),
        "month": MonthTransform(),
        "day": DayTransform(),
    }

    @staticmethod
    def _get_iceberg_type(arrow_type):  # noqa: C901
        """Map PyArrow types to Iceberg types."""
        # Integer types
        if arrow_type == pa.int32():
            return IntegerType()
        if arrow_type == pa.int64():
            return LongType()

        # Floating-point types
        if arrow_type == pa.float64():
            return DoubleType()
        if arrow_type == pa.float32():
            return FloatType()

        # Boolean type
        if arrow_type == pa.bool_():
            return BooleanType()

        # String types
        if arrow_type == pa.string():
            return StringType()

        # Binary types
        if arrow_type == pa.binary() or arrow_type == pa.large_binary():
            return BinaryType()

        # Timestamp types
        if (
            arrow_type == pa.timestamp("us")
            or arrow_type == pa.timestamp("us", tz="UTC")
            or arrow_type == pa.timestamp("us", tz=None)
        ):
            return TimestampType()

        # Date type
        if arrow_type == pa.date32():
            return DateType()

        # Default to string if no matching type is found
        return StringType()

    @classmethod
    def to_iceberg_schema(cls, schema: pa.Schema, datetime_field: pa.Field) -> Schema:
        fields = [
            NestedField(
                name=datetime_field.name,
                field_id=0,
                field_type=cls._get_iceberg_type(datetime_field.type),
                required=not datetime_field.nullable,
            )
        ]
        # iterate over schema
        for i, field in enumerate(schema):
            fields.append(
                NestedField(
                    name=field.name,
                    field_id=i + 1,
                    field_type=cls._get_iceberg_type(field.type),
                    required=not field.nullable,
                )
            )
        return Schema(*fields)

    @classmethod
    def to_partition_spec(
        cls,
        transformation: str,
    ) -> PartitionSpec:
        partitions = []
        if (fn := cls.PARTITION_TRANSFORMATIONS.get(transformation)) is None:
            raise ValueError(f"Unsupported partition transformation: {transformation}")

        partitions.append(
            PartitionField(
                name=transformation,
                source_id=0,
                field_id=1001,
                transform=fn,
            )
        )

        return PartitionSpec(*partitions)