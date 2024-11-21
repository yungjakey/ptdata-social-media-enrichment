"""Database utilities."""
import logging
import time
from typing import Any

from botocore.client import BaseClient
from omegaconf import DictConfig

from src.utils.types import TypeHandler

logger = logging.getLogger(__name__)

async def load_schema_from_database(
    client: BaseClient,
    database_config: DictConfig,
) -> dict[str, type]:
    """Load schema from database tables."""
    schema: dict[str, type] = {}
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{database_config.database}'
    AND table_name = '{database_config.source.facts}'
    """
    results = execute_query(client, query, database_config=database_config)
    for row in results:
        schema[row['column_name']] = TypeHandler.to_python_type(row['data_type'])
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
        QueryExecutionContext={'Database': database_config.database},
        WorkGroup=database_config.workgroup,
        ResultConfiguration={'OutputLocation': database_config.output_location}
    )
    query_execution_id = response['QueryExecutionId']
    while True:
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(0.5)
    if state != 'SUCCEEDED':
        error = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Query failed: {error}")

    results = []
    paginator = client.get_paginator('get_query_results')
    try:
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            result_set = page['ResultSet']
            if not results:
                # First row contains column names
                columns = [col['VarCharValue'] for col in result_set['Rows'][0]['Data']]
            # Skip header row after first page
            start_idx = 1 if not results else 0
            for row in result_set['Rows'][start_idx:]:
                results.append({
                    columns[i]: value.get('VarCharValue')
                    for i, value in enumerate(row['Data'])
                })
    except Exception as e:
        logger.error(f"Error fetching results: {e}")
        raise
    return results

async def fetch_records(
    client: BaseClient,
    conf: DictConfig
) -> list[dict[str, Any]]:
    """Fetch unprocessed records with metrics and details."""
    # First check if processed column exists
    check_query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = '{conf.database}' 
    AND table_name = '{conf.source.facts}'
    AND column_name = 'processed'
    """
    results = execute_query(client, check_query, database_config=conf)
    
    # Build main query based on whether processed column exists
    if results:
        where_clause = "WHERE m.processed IS NULL OR m.processed = false"
    else:
        where_clause = ""  # No filtering if column doesn't exist
        
    query = f"""
    SELECT m.*, d.*
    FROM {conf.database}.{conf.source.facts} m
    LEFT JOIN {conf.database}.{conf.source.dims} d ON m.id = d.id
    {where_clause}
    LIMIT {conf.batch_limit}
    """
    return execute_query(client, query, database_config=conf)

async def write_records(
    client: BaseClient,
    table_type: str,
    records: list[dict[str, Any]],
    database_config: DictConfig
) -> None:
    """Write records using Athena. """
    if not records:
        return

    # Get the target table based on type
    table = database_config.sink.facts if table_type == 'facts' else database_config.sink.dims

    columns = list(records[0].keys())
    values = []
    for record in records:
        row_values = [
            'NULL' if val is None else str(val) if isinstance(val, (int, float)) else f"'{str(val)}'"
            for val in record.values()
        ]
        values.append(f"({', '.join(row_values)})")
    query = f"""
    INSERT INTO {database_config.database}.{table}
    ({', '.join(columns)})
    VALUES {', '.join(values)}
    """
    execute_query(client, query, database_config=database_config)

async def mark_records_as_processed(
    client: BaseClient,
    record_ids: list[bytes],
    database_config: DictConfig
) -> None:
    """Mark processed records to avoid reprocessing."""
    if not record_ids:
        return
    id_list = ', '.join([f"'{id}'" for id in record_ids])
    query = f"""
    UPDATE {database_config.database}.{database_config.source.facts}
    SET processed = true
    WHERE id IN ({id_list})
    """
    execute_query(client, query, database_config=database_config)
