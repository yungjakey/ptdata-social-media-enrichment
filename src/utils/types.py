"""Type handling utilities."""

import json
from datetime import datetime
from typing import Any

from mypy_boto3_rds_data.type_defs import SqlParameterTypeDef


class TypeHandler:
    """Handles type conversions and formatting."""

    # Type mapping from strings to Python types
    TYPE_MAP = {
        # Basic types
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "datetime": datetime,
        "list[str]": list[str],
        "dict[str, Any]": dict[str, Any],
        # Database types
        "integer": int,
        "bigint": int,
        "smallint": int,
        "double": float,
        "decimal": float,
        "real": float,
        "varchar": str,
        "char": str,
        "text": str,
        "timestamp": datetime,
        "date": datetime,
        "boolean": bool,
        "array": list,
    }

    # Type mapping for format strings
    FORMAT_MAP = {
        float: "float (0-1)",
        str: "str",
        list[str]: "[str]",
        dict[str, Any]: "{}",
        list: "[str]",  # Default array type
    }

    @classmethod
    def to_python_type(cls, type_str: str) -> type:
        """Convert type string to Python type."""
        if type_str.startswith("array"):
            return list
        return cls.TYPE_MAP.get(type_str.lower(), str)

    @classmethod
    def to_format_string(cls, field_type: type) -> str:
        """Convert Python type to format string."""
        return cls.FORMAT_MAP.get(field_type, str(field_type))

    @classmethod
    def to_sql_value(cls, value: Any) -> SqlParameterTypeDef:
        """Format value for SQL parameter."""
        if value is None:
            return {"isNull": True}
        if isinstance(value, bool):
            return {"booleanValue": value}
        if isinstance(value, int):
            return {"longValue": value}
        if isinstance(value, float):
            return {"doubleValue": value}
        if isinstance(value, datetime):
            return {"stringValue": value.isoformat()}
        if isinstance(value, list | dict):
            return {"stringValue": json.dumps(value)}
        return {"stringValue": str(value)}

    @classmethod
    def get_model_fields(
        cls, model_class: type, exclude_fields: set[str] | None = None
    ) -> dict[str, str]:
        """Get fields and their format strings from a model class."""
        exclude_fields = exclude_fields or {"id", "post_key", "evaluation_time"}
        fields = {}

        for field_name, field_type in model_class.__annotations__.items():
            if field_name not in exclude_fields:
                fields[field_name] = cls.to_format_string(field_type)

        return fields
