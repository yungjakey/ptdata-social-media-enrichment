from __future__ import annotations

import json
from datetime import date, datetime

import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.partitioning import (
    DayTransform,
    MonthTransform,
    PartitionField,
    PartitionSpec,
    YearTransform,
)
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        # Handle datetime
        if isinstance(o, datetime):
            return o.isoformat()

        # Handle bytes
        elif isinstance(o, bytes):
            try:
                return o.decode("ascii")
            except UnicodeDecodeError:
                return o.hex()

        # Handle other types by falling back to the parent method
        return super().default(o)


class ArrowConverter:
    @staticmethod
    def _get_arrow_type(python_type):
        """Get the corresponding Arrow type for a given Python type."""
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
        if isinstance(python_type, type) and issubclass(python_type, BaseModel):
            # Handle nested Pydantic models by converting them to struct type
            return pa.struct(
                [
                    pa.field(name, ArrowConverter._get_arrow_type(field.annotation))
                    for name, field in python_type.model_fields.items()
                ]
            )
        return pa.string()

    @classmethod
    def _process_field(cls, name: str, field) -> pa.Field:
        """Recursively process a field to handle nested structures."""
        field_type = field.annotation
        if not (isinstance(field_type, type) and issubclass(field_type, BaseModel)):
            return pa.field(name, cls._get_arrow_type(field_type))

        # Process nested BaseModel
        nested_fields = [
            cls._process_field(nested_name, nested_field)
            for nested_name, nested_field in field_type.model_fields.items()
        ]
        return pa.field(name, pa.struct(nested_fields))

    @classmethod
    def to_list(cls, model: type[BaseModel]) -> list[pa.Field]:
        """Convert Pydantic model to a list of Arrow fields."""
        fields = []
        for name, field in model.model_fields.items():
            fields.append(cls._process_field(name, field))
        return fields

    @classmethod
    def to_arrow_schema(cls, model: type[BaseModel]) -> pa.Schema:
        """Convert Pydantic model to Arrow schema."""
        return pa.schema(cls.to_list(model))


class IcebergConverter:
    """Converter for Pydantic models to various data formats including Iceberg."""

    PARTITION_TRANSFORMATIONS = {
        "year": YearTransform(),
        "month": MonthTransform(),
        "day": DayTransform(),
    }

    @classmethod
    def _get_iceberg_type(cls, arrow_type):  # noqa: C901
        """Map PyArrow types to Iceberg types."""
        # Handle struct types recursively
        if pa.types.is_struct(arrow_type):
            fields = [
                NestedField(
                    name=field.name,
                    field_id=i + 1,  # Increment field ID to avoid collisions
                    field_type=cls._get_iceberg_type(field.type),
                    required=not field.nullable,
                )
                for i, field in enumerate(arrow_type)
            ]
            return StructType(fields=fields)  # Ensure fields are passed correctly

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
        if arrow_type in {pa.binary(), pa.large_binary()}:
            return BinaryType()

        # Timestamp types
        if arrow_type in {
            pa.timestamp("us"),
            pa.timestamp("us", tz="UTC"),
            pa.timestamp("us", tz=None),
        }:
            return TimestampType()

        # Date type
        if arrow_type == pa.date32():
            return DateType()

        # Raise an error for unsupported types
        raise ValueError(f"Unsupported PyArrow type: {arrow_type}")

    @classmethod
    def to_iceberg_schema(cls, arrow_schema: pa.Schema, datetime_field: pa.Field) -> Schema:
        """Convert PyArrow schema to Iceberg schema."""
        fields = [
            NestedField(
                name=field.name,
                field_id=i + 1,
                field_type=cls._get_iceberg_type(field.type),
                required=not field.nullable,
            )
            for i, field in enumerate(arrow_schema)
        ]
        # Add datetime field explicitly if provided
        if datetime_field:
            fields.insert(
                0,
                NestedField(
                    name=datetime_field.name,
                    field_id=0,
                    field_type=cls._get_iceberg_type(datetime_field.type),
                    required=not datetime_field.nullable,
                ),
            )
        return Schema(*fields)

    @classmethod
    def to_partition_spec(cls, transformation: str) -> PartitionSpec:
        """Create an Iceberg partition spec."""
        if (transform := cls.PARTITION_TRANSFORMATIONS.get(transformation)) is None:
            raise ValueError(f"Unsupported partition transformation: {transformation}")

        partition_field = PartitionField(
            name="partition",
            source_id=0,
            field_id=1001,
            transform=transform,
        )

        return PartitionSpec(partition_field)
