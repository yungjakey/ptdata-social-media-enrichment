from __future__ import annotations

from datetime import date, datetime

import pyarrow as pa
from pydantic import BaseModel
from pyiceber.partitioning import PartitionSpec
from pyiceberg.schema import NestedField, PartitionField, PartitionSpec, Schema
from pyiceberg.transforms import DayTransform, MonthTransform, YearTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)


class ArrowConverter:
    @staticmethod
    def _get_arrow_type(python_type):
        if python_type is int:
            return pa.int64()
        elif python_type is str:
            return pa.string()
        elif python_type is float:
            return pa.float64()
        elif python_type is bool:
            return pa.bool_()
        elif python_type is datetime:
            return pa.timestamp("us")
        elif python_type is date:
            return pa.date32()
        elif python_type is bytes:
            return pa.binary()
        else:
            raise ValueError(f"Unsupported Python type: {python_type}")

    @classmethod
    def to_dict(cls, model: type[BaseModel]) -> dict[str, type]:
        """Convert Pydantic model to Arrow schema."""
        return {
            name: cls._get_arrow_type(field.annotation)
            for name, field in model.model_fields.items()
        }

    @classmethod
    def to_arrow_schema(cls, model: type[BaseModel]) -> pa.Schema:
        return pa.schema(cls.to_dict(model).values())


class IcebergConverter:
    """Converter for Pydantic models to various data formats including Iceberg."""

    PARTITION_TRANSFORMATIONS = {
        "year": YearTransform(),
        "month": MonthTransform(),
        "day": DayTransform(),
    }

    def __init__(
        self,
        model: type[BaseModel],
        datetime_field: str,
        index_field: str,
    ):
        self.model = model
        self.datetime_field = datetime_field
        self.index_field = index_field

    @staticmethod
    def _get_iceberg_type(arrow_type):  #  # noqa: C901
        if arrow_type == pa.int64():
            return LongType()
        if arrow_type == pa.string():
            return StringType()
        if arrow_type == pa.float64():
            return DoubleType()
        if arrow_type == pa.bool_():
            return BooleanType()
        if arrow_type == pa.timestamp("us"):
            return TimestampType()
        if arrow_type == pa.date32():
            return DateType()
        if arrow_type == pa.binary():
            return BinaryType()
        raise ValueError(f"Unsupported Arrow type: {arrow_type}")

        @classmethod
        def to_iceberg_schema(cls) -> Schema:
            fields = []
            fields.append(
                NestedField(
                    0,
                    cls.datetime_field,
                    TimestampType(),
                    False,
                )
            )
            for j, (name, field) in enumerate(cls.model.model_fields.items()):
                fields.append(
                    NestedField(
                        name=name,
                        field_id=j,
                        field_type=cls._get_iceberg_type(field.annotation),
                        required=False,
                    )
                )

            return Schema(fields)

        @classmethod
        def to_partition_spec(
            cls,
            transformations: list[dict],
        ) -> PartitionSpec:
            partitions = []
            for n in transformations:
                if (fn := cls.PARTITION_TRANSFORMATIONS.get(n)) is None:
                    raise ValueError(f"Unsupported partition transformation: {n}")

                partitions.append(
                    PartitionField(
                        name=n,
                        source_id=0,
                        field_id=0,
                        transform=fn,
                    )
                )

            return PartitionSpec(partitions)
