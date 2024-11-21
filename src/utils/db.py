"""Database utilities."""

import logging
import time
from enum import Enum
from typing import Any

from botocore.client import BaseClient
from omegaconf import DictConfig

from src.utils.types import TypeHandler

logger = logging.getLogger(__name__)


class TableType(Enum):
    """Database table types with their corresponding table names."""

    SOURCE_FACTS = "fact_social_media_reaction_post"
    SOURCE_DIMS = ["dim_post_details", "dim_channel", "dim_channel_group"]
    ENRICHED_FACTS = "fact_social_media_ai_metrics"
    ENRICHED_DIMS = ["dim_post_ai_details"]

    def get_table_name(self, config: DictConfig) -> str:
        """Get the actual table name from config based on type."""
        if self in (TableType.SOURCE_FACTS, TableType.SOURCE_DIMS):
            base = config.source
        else:
            base = config.enriched
        return (
            base.facts if self in (TableType.SOURCE_FACTS, TableType.ENRICHED_FACTS) else base.dims
        )


async def load_schema_from_database(
    client: BaseClient,
    database_config: DictConfig,
) -> dict[str, type]:
    """Load schema from database tables."""
    schema: dict[str, type] = {}
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = :database
    AND table_name = :table
    """
    parameters = [
        {"name": "database", "value": {"stringValue": database_config.database}},
        {"name": "table", "value": {"stringValue": database_config.source.facts}},
    ]
    results = execute_query(client, query, parameters, database_config=database_config)
    for row in results:
        schema[row["column_name"]] = TypeHandler.to_python_type(row["data_type"])
    return schema


def execute_query(
    client: BaseClient,
    query: str,
    parameters: list[dict[str, Any]] | None = None,
    database_config: DictConfig | None = None,
) -> list[dict[str, Any]]:
    """Execute a query using Athena."""
    if not database_config:
        raise ValueError("Database configuration is required")
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database_config.database},
        WorkGroup=database_config.workgroup,
        ResultConfiguration={"OutputLocation": database_config.output_location},
        Parameters=parameters or [],
    )
    query_execution_id = response["QueryExecutionId"]
    while True:
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(0.5)
    if state != "SUCCEEDED":
        error = query_status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        raise Exception(f"Query failed: {error}")

    results = []
    paginator = client.get_paginator("get_query_results")
    try:
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            result_set = page["ResultSet"]
            if not results:
                columns = [col["VarCharValue"] for col in result_set["Rows"][0]["Data"]]
            start_idx = 1 if not results else 0
            for row in result_set["Rows"][start_idx:]:
                results.append(
                    {columns[i]: value.get("VarCharValue") for i, value in enumerate(row["Data"])}
                )
    except Exception as e:
        logger.error(f"Error fetching results: {e}")
        raise
    return results


async def fetch_records(client: BaseClient, conf: DictConfig) -> list[dict[str, Any]]:
    """Fetch unprocessed records with metrics and details."""
    check_query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :database
    AND table_name = :table
    AND column_name = 'processed'
    """
    check_params = [
        {"name": "database", "value": {"stringValue": conf.database}},
        {"name": "table", "value": {"stringValue": conf.source.facts}},
    ]
    results = execute_query(client, check_query, check_params, database_config=conf)

    where_clause = "WHERE m.processed IS NULL OR m.processed = false" if results else ""
    query = """
    SELECT m.*, d.*
    FROM {database}.{source_facts} m
    LEFT JOIN {database}.{source_dims} d ON m.id = d.id
    {where_clause}
    LIMIT :limit
    """
    query = query.format(
        database=conf.database,
        source_facts=conf.source.facts,
        source_dims=conf.source.dims,
        where_clause=where_clause,
    )
    parameters = [{"name": "limit", "value": {"longValue": conf.batch_limit}}]
    return execute_query(client, query, parameters, database_config=conf)


def _create_parameter(name: str, value: Any) -> dict[str, Any]:
    """Create a parameter dict for Athena query."""
    return {"name": name, "value": {"stringValue": str(value)}}


def _format_array_params(prefix: str, values: list | set) -> list[dict[str, Any]]:
    """Format array parameters for Athena."""
    return [_create_parameter(f"{prefix}_{i}", v) for i, v in enumerate(values)]


def _format_map_params(prefix: str, data: dict) -> list[dict[str, Any]]:
    """Format map parameters for Athena."""
    key_params = [_create_parameter(f"{prefix}_k{i}", k) for i, k in enumerate(data.keys())]
    val_params = [_create_parameter(f"{prefix}_v{i}", v) for i, v in enumerate(data.values())]
    return key_params + val_params


async def write_records(
    client: BaseClient,
    table_type: TableType,
    records: list[dict[str, Any]],
    database_config: DictConfig,
) -> None:
    """Write records using Athena."""
    if not records:
        return

    # Get table name from enum
    table = table_type.get_table_name(database_config)

    # Build query
    columns = list(records[0].keys())
    placeholders = []
    parameters = []

    for i, record in enumerate(records):
        # Format values for this record
        values = []
        for j, val in enumerate(record.values()):
            param_prefix = f"{i}_{j}"

            if val is None:
                values.append("NULL")
            elif isinstance(val, list | set):
                values.append(f"ARRAY[{', '.join(':' + str(i) for i in range(len(val)))}]")
                parameters.extend(_format_array_params(param_prefix, val))
            elif isinstance(val, dict):
                values.append(f"MAP({', '.join([f':k{i}, :v{i}' for i in range(len(val))])})")
                parameters.extend(_format_map_params(param_prefix, val))
            else:
                values.append(f":{param_prefix}")
                parameters.append(_create_parameter(param_prefix, val))

        placeholders.append(f"({', '.join(values)})")

    query = f"""
    INSERT INTO {database_config.database}.{table}
    ({', '.join(columns)})
    VALUES {', '.join(placeholders)}
    """

    execute_query(client, query, parameters, database_config=database_config)


async def mark_records_as_processed(
    client: BaseClient, record_ids: list[bytes], database_config: DictConfig
) -> None:
    """Mark processed records to avoid reprocessing."""
    if not record_ids:
        return

    # Use parameterized query with proper placeholders
    query = """
    UPDATE :database.:table
    SET processed = true
    WHERE id IN ({{ placeholders }})
    """

    # Prepare parameters securely
    parameters = [
        {"name": "database", "value": {"stringValue": database_config.database}},
        {"name": "table", "value": {"stringValue": TableType.SOURCE_FACTS.value}},
    ]

    # Create placeholders and corresponding parameter values
    id_params = []
    for i, record_id in enumerate(record_ids):
        param_name = f"id{i}"
        parameters.append({"name": param_name, "value": {"stringValue": str(record_id)}})
        id_params.append(f":{param_name}")

    # Replace placeholders in the query
    query = query.replace("{{ placeholders }}", ", ".join(id_params))

    execute_query(client, query, parameters, database_config=database_config)
