"""AWS Athena client implementation."""

import asyncio
import io
import logging
import time
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlparse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from src.connectors.types import ConnectorConfig, ConnectorProtocol

logger = logging.getLogger(__name__)


class QueryExecutionError(Exception):
    """Raised when a query execution fails."""

    def __init__(self, query_id: str, state: str, reason: str | None = None):
        self.query_id = query_id
        self.state = state
        self.reason = reason
        message = f"Query {query_id} failed with state {state}"
        if reason:
            message += f": {reason}"
        super().__init__(message)


class NotConnectedError(Exception):
    """Raised when trying to use client before connecting."""


class S3LocationError(Exception):
    """Raised when S3 location is invalid."""


class MalformedResponseError(Exception):
    """Raised when AWS response is missing required fields."""

    def __init__(self, response: dict[str, Any], missing_field: str):
        self.response = response
        self.missing_field = missing_field
        super().__init__(f"AWS response missing required field: {missing_field}")


class AWSClient(ConnectorProtocol):
    """AWS Athena client implementation."""

    POLL_INTERVAL = 1.0  # seconds
    TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}

    def __init__(self, config: ConnectorConfig) -> None:
        """Initialize AWS client."""
        self.type = config.type
        self.name = config.name
        self.params = config.params
        self._session: boto3.Session | None = None
        self._athena = None
        self._glue = None
        self._s3 = None

    def validate(self) -> None:
        """Validate AWS client configuration."""
        required = {"database", "region", "output_location", "workgroup"}
        missing = required - set(self.params.keys())
        if missing:
            raise ValueError(f"Missing required parameters: {missing}")

        # Validate S3 output location
        location = urlparse(self.params["output_location"])
        if location.scheme != "s3" or not location.netloc:
            raise S3LocationError(f"Invalid S3 location: {self.params['output_location']}")

    def connect(self) -> None:
        """Connect to AWS services."""
        self.validate()
        self._session = boto3.Session(region_name=self.params["region"])
        self._athena = self._session.client("athena")
        self._glue = self._session.client("glue")
        self._s3 = self._session.client("s3")

    def disconnect(self) -> None:
        """Disconnect from AWS services."""
        self._session = None
        self._athena = None
        self._glue = None
        self._s3 = None

    def _get_query_state(self, response: dict[str, Any]) -> tuple[str, str | None]:
        """Extract query state and reason from response."""
        query_execution = response.get("QueryExecution")
        if not query_execution:
            raise MalformedResponseError(response, "QueryExecution")

        status = query_execution.get("Status")
        if not status:
            raise MalformedResponseError(response, "QueryExecution.Status")

        state = status.get("State")
        if not state:
            raise MalformedResponseError(response, "QueryExecution.Status.State")

        return state, status.get("StateChangeReason")

    async def _wait_for_query(self, query_id: str) -> None:
        """Wait for query completion and handle errors."""
        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        while True:
            response = self._athena.get_query_execution(QueryExecutionId=query_id)
            state, reason = self._get_query_state(response)

            if state == "SUCCEEDED":
                return
            if state in {"FAILED", "CANCELLED"}:
                raise QueryExecutionError(query_id, state, reason)

            await asyncio.sleep(self.POLL_INTERVAL)

    async def execute_query(self, query: str) -> str:
        """Execute a query and return the execution ID."""
        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        try:
            response = self._athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.params["database"]},
                ResultConfiguration={"OutputLocation": self.params["output_location"]},
                WorkGroup=self.params["workgroup"],
            )
            query_execution_id = response.get("QueryExecutionId")
            if not query_execution_id:
                raise MalformedResponseError(response, "QueryExecutionId")
            return query_execution_id
        except ClientError as e:
            logger.error("Failed to execute query: %s", e)
            raise

    def _get_s3_location(self, path: str) -> tuple[str, str]:
        """Parse S3 location into bucket and key."""
        location = urlparse(path)
        if location.scheme != "s3":
            raise S3LocationError(f"Invalid S3 location: {path}")
        return location.netloc, location.path.lstrip("/")

    async def read(self) -> AsyncIterator[dict[str, Any]]:
        """Execute query and yield results."""
        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        try:
            # Execute the query
            query_id = await self.execute_query(self.params["query"])
            await self._wait_for_query(query_id)

            # Fetch results
            paginator = self._athena.get_paginator("get_query_results")
            for page in paginator.paginate(QueryExecutionId=query_id):
                rows = page.get("ResultSet", {}).get("Rows", [])
                if not rows:
                    continue

                # First row contains column names
                if "headers" not in locals():
                    headers = [col.get("VarCharValue", "") for col in rows[0].get("Data", [])]
                    rows = rows[1:]

                # Process data rows
                for row in rows:
                    data = row.get("Data", [])
                    yield {headers[i]: col.get("VarCharValue", "") for i, col in enumerate(data)}

        except (ClientError, QueryExecutionError) as e:
            logger.error("Failed to read data: %s", e)
            raise

    async def write(self, records: AsyncIterator[dict[str, Any]]) -> None:
        """Write data to Athena using S3 and INSERT INTO."""
        if not self._athena or not self._s3:
            raise NotConnectedError("Not connected to AWS")

        try:
            table = self.params["tables"][0]["name"]
            schema = self.params["tables"][0].get("schema", {})

            # Create table if not exists with schema from config
            if schema:
                column_defs = [f"{name} {type}" for name, type in schema.items()]
                create_table = f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    {', '.join(column_defs)}
                )
                STORED AS PARQUET
                LOCATION '{self.params["output_location"].rstrip("/")}/{table}/'
                """
                query_id = await self.execute_query(create_table)
                await self._wait_for_query(query_id)

            # Collect records and prepare output model
            table_data = []
            async for record in records:
                # Convert record to match schema types
                row = {
                    col: str(record.get(col, "")) if typ == "string" else record.get(col)
                    for col, typ in schema.items()
                }
                table_data.append(row)

            if not table_data:
                logger.warning("No records to write")
                return

            # Initialize PyArrow schema
            pa_schema = pa.schema(
                [
                    (name, pa.string() if typ == "string" else pa.from_numpy_dtype(typ))
                    for name, typ in schema.items()
                ]
            )

            # Write to Parquet in memory
            table = pa.Table.from_pylist(table_data, schema=pa_schema)
            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)

            # Create a temporary location in S3
            timestamp = int(time.time())
            temp_location = f"{self.params['output_location'].rstrip('/')}/temp/{table}_{timestamp}"
            s3_bucket, s3_key = self._get_s3_location(temp_location)

            # Upload to S3
            self._s3.upload_fileobj(parquet_buffer, s3_bucket, f"{s3_key}/data.parquet")

            # Create temporary table
            temp_table = f"{table}_temp_{timestamp}"
            create_temp_table = f"""
            CREATE TABLE {temp_table}
            ({', '.join(column_defs)})
            WITH (
                external_location = '{temp_location}',
                format = 'PARQUET'
            )
            """

            # Execute create table
            query_id = await self.execute_query(create_temp_table)
            await self._wait_for_query(query_id)

            # Insert data into target table
            insert_query = f"""
            INSERT INTO {table}
            ({', '.join(schema.keys())})
            SELECT {', '.join(schema.keys())}
            FROM {temp_table}
            """

            # Execute insert
            query_id = await self.execute_query(insert_query)
            await self._wait_for_query(query_id)

            # Clean up
            cleanup_query = f"DROP TABLE {temp_table}"
            query_id = await self.execute_query(cleanup_query)
            await self._wait_for_query(query_id)

            # Delete S3 objects
            self._s3.delete_object(Bucket=s3_bucket, Key=f"{s3_key}/data.parquet")

        except (ClientError, QueryExecutionError) as e:
            logger.error("Failed to write data: %s", e)
            raise
