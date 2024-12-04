from __future__ import annotations

from enum import Enum
from typing import Any

from jinja2 import BaseLoader, Environment

ENV = Environment(loader=BaseLoader(), trim_blocks=True, lstrip_blocks=True, autoescape=True)

SQL_CREATE_TABLE = """
CREATE EXTERNAL TABLE {{ table_name }}
{%- if partition_columns %}
PARTITIONED BY (
    {%- for col, dtype in partition_columns %}
    {{ col }} {{ dtype }}{% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- endif %}
STORED AS PARQUET
LOCATION '{{ base_dir }}'
{%- if table_properties %}
TBLPROPERTIES (
    {%- for key, value in table_properties %}
    '{{ key }}' = '{{ value }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- endif %}
""".strip()


def get_table_query(
    table_name: str,
    base_dir: str,
    partitions: dict[str, str] | None = None,
    properties: dict[str, str] | None = None,
) -> str:
    """Generate CREATE TABLE query for AWS Glue/Athena."""
    # Define default Parquet-specific properties
    default_parquet_properties = {
        # Compression for Parquet files
        "parquet.compression": "SNAPPY",
        "parquet.bloom.filter.enabled": "true",
        "parquet.write.validation": "true",
        "store.parquet.dictionary.encoding.enabled": "true",
        "parquet.metadata.read.cache.size": "1000",
    }

    # Merge default properties with user-provided properties
    table_properties = {**default_parquet_properties, **(properties or {})}

    # Handle partitioning
    if partitions:
        # Add partition projection properties
        table_properties.update(
            {
                "projection.enabled": "true",
                **{f"projection.{col}.type": dtype for col, dtype in partitions.items()},
                "storage.location.template": f"{base_dir}/{'/'.join(f'{col}=${{{col}}}' for col in partitions)}",
            }
        )

    template = ENV.from_string(SQL_CREATE_TABLE)
    return template.render(
        table_name=table_name,
        base_dir=base_dir,
        partition_columns=list(partitions.items()) if partitions else None,
        table_properties=table_properties,
    )


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
    def from_response(cls, status: dict[str, Any]) -> QueryState:
        """Create QueryState from Athena API response."""
        try:
            state = status["State"]
            return cls(state)
        except (KeyError, ValueError) as e:
            raise Exception(f"Error parsing Athena query state: {status}.") from e
