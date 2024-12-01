"""AWS Athena client implementation."""

from __future__ import annotations

import asyncio
import io
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pydantic import BaseModel

from src.connectors.aws.types import (
    MalformedResponseError,
    NotConnectedError,
    QueryExecutionError,
    QueryState,
    S3LocationError,
)
from src.connectors.types import ConnectorConfig

logger = logging.getLogger(__name__)


class AWSClient(ConnectorConfig):
    """AWS Athena client implementation."""

    POLL_INTERVAL = 1.0  # seconds

    _PY2ATHENA = {
        str: "string",
        int: "int",
        float: "double",
        bool: "boolean",
        datetime: "timestamp",
    }
    _ATHENA2ARROW = {
        "string": pa.string(),
        "boolean": pa.bool_(),
        "double": pa.float64(),
        "int": pa.int64(),
        "binary": pa.binary(),
    }

    def __init__(self, config: ConnectorConfig) -> None:
        """Initialize AWS client."""
        from src.connectors.aws.types import AWSParams

        self.type = config.type
        self.name = config.name
        self.params = AWSParams(**config.params)
        self._session: aioboto3.Session | None = None
        self._athena = None
        self._glue = None
        self._s3 = None

    async def validate(self) -> None:
        """Validate AWS client configuration."""
        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        try:
            # Validate that the workgroup exists
            response = await self._athena.get_work_group(WorkGroup=self.params.workgroup)
            if not response.get("WorkGroup"):
                raise ValueError(f"WorkGroup '{self.params.workgroup}' not found")
        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidRequestException":
                raise ValueError(f"WorkGroup '{self.params.workgroup}' not found") from e
            raise

    async def connect(self) -> None:
        """Connect to AWS services."""
        if not self._session:
            self._session = aioboto3.Session(region_name=self.params.region)

        # Explicitly await client creation and use
        athena_client = await self._session.client("athena")
        self._athena = athena_client

        glue_client = await self._session.client("glue")
        self._glue = glue_client

        s3_client = await self._session.client("s3")
        self._s3 = s3_client

        # Validate workgroup after connecting
        await self.validate()

        # Validate S3 bucket
        if self.params.output_location is not None:
            await self._s3.head_bucket(Bucket=urlparse(self.params.output_location).netloc)

    async def disconnect(self) -> None:
        """Disconnect from AWS services."""
        # No need to explicitly call __aexit__ as async context managers handle this
        self._session = None
        self._athena = None
        self._glue = None
        self._s3 = None

    def _get_query_state(self, response: dict[str, Any]) -> tuple[QueryState, str | None]:
        """Extract query state and reason from response."""
        # Validate the response structure
        if not response or "QueryExecution" not in response:
            raise MalformedResponseError(response, "QueryExecution")

        query_execution = response.get("QueryExecution", {})
        status = query_execution.get("Status", {})

        # Validate the status structure
        if not status:
            raise MalformedResponseError(response, "Status")

        # Validate the state
        if "State" not in status:
            raise MalformedResponseError(response, "State")

        state_str = status["State"]
        state = QueryState[state_str]  # This will raise KeyError if state is invalid
        reason = status.get("StateChangeReason")
        return state, reason

    async def _wait_for_query(self, query_id: str, timeout: float = 300.0) -> None:
        """
        Wait for query completion with a timeout."""
        max_iterations = int(timeout / self.POLL_INTERVAL)

        for _ in range(max_iterations):
            # Get query execution status
            response = await self._athena.get_query_execution(QueryExecutionId=query_id)

            # Use the existing _get_query_state method
            state, reason = self._get_query_state(response)

            # Handle terminal states
            if state in QueryState.terminal_states():
                if state == QueryState.SUCCEEDED:
                    return
                else:
                    raise QueryExecutionError(query_id, state, reason)
            elif state in QueryState.running_states():
                await asyncio.sleep(self.POLL_INTERVAL)
                logger.info(f"Query {query_id} is in state {state.name}")

        # If we've exhausted all iterations without reaching a terminal state
        raise TimeoutError(f"Query {query_id} did not complete within {timeout} seconds")

    async def _execute_query(self, query: str) -> str:
        """Execute a query and return the execution ID."""
        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        try:
            response = await self._athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.params.database},
                ResultConfiguration={"OutputLocation": self.params.output_location},
                WorkGroup=self.params.workgroup,
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

    def _get_schema(self, output_model: type[BaseModel]) -> dict[str, str]:
        """Get schema from Pydantic model."""
        schema = {}
        for field_name, field in output_model.model_fields.items():
            annotation = field.annotation
            if annotation in self._PY2ATHENA:
                schema[field_name] = self._PY2ATHENA[annotation]
            else:
                # Default to string if type not mapped
                schema[field_name] = "string"
                logger.warning(
                    f"Field {field_name} has unmapped type {annotation}, defaulting to string"
                )
        return schema

    def _records_to_table(self, records: list[dict[str, Any]], schema: dict[str, str]) -> pa.Table:
        """Convert records to Arrow table."""
        pa_schema = pa.schema(
            [(name, self._ATHENA2ARROW.get(typ, pa.string())) for name, typ in schema.items()]
        )
        return pa.Table.from_pylist(records, schema=pa_schema)

    async def read(
        self,
        query: str | None = None,
        output_model: type[BaseModel] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Execute a query and yield results."""
        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        # Use config query if no query is provided
        if query is None:
            query = self.params.query
            if not query:
                raise ValueError("No query specified in configuration or method call")

        try:
            # Execute the query
            query_id = await self._execute_query(query)
            await self._wait_for_query(query_id)

            # Pagination handling
            next_token = None
            while True:
                # Get query results with optional next token
                kwargs = {"QueryExecutionId": query_id}
                if next_token:
                    kwargs["NextToken"] = next_token

                response = await self._athena.get_query_results(**kwargs)
                rows = response.get("ResultSet", {}).get("Rows", [])

                # Extract column names from the first row (only on first page)
                if not next_token and len(rows) > 1:
                    headers = [col.get("VarCharValue", "") for col in rows[0].get("Data", [])]
                    rows = rows[1:]  # Skip header row on first page

                # Process data rows
                for row in rows:
                    data = row.get("Data", [])
                    if len(data) == len(headers):
                        record = {
                            headers[i]: data[i].get("VarCharValue", "") for i in range(len(headers))
                        }
                        # Convert value to int if possible
                        if "value" in record:
                            try:
                                record["value"] = int(record["value"])
                            except ValueError:
                                pass
                        yield record

                # Check for more pages
                next_token = response.get("NextToken")
                if not next_token:
                    break

        except ClientError as e:
            logger.error("Failed to get query results: %s", e)
            raise

    async def write(
        self, records: list[dict[str, Any]], table_name: str, output_model: type[BaseModel]
    ) -> None:
        """Write data to Athena using S3."""
        if not self._athena or not self._s3:
            raise NotConnectedError("Not connected to AWS")

        # Get schema from model
        schema = self._get_schema(output_model)

        try:
            # Determine base S3 location for the table
            base_s3_location = f"{self.params.output_location.rstrip('/')}/{table_name}"

            # Create table if it doesn't exist
            create_table = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {', '.join(f'"{name}" {type_}' for name, type_ in schema.items())}
            )
            STORED AS PARQUET
            LOCATION '{base_s3_location}/'
            """
            query_id = await self._execute_query(create_table)
            await self._wait_for_query(query_id)

            # Convert records to Arrow table
            table = self._records_to_table(records, schema)

            # Upload Parquet file
            s3_key = f"{table_name}-{datetime.now():%s}.parquet"
            with io.BytesIO() as buffer:
                pq.write_table(table, buffer)
                buffer.seek(0)
                await self._s3.put_object(
                    Bucket=self.params.output_location.split("/")[2],
                    Key=f"{table_name}/{s3_key}",
                    Body=buffer.getvalue(),
                )

        except ClientError as e:
            logger.error("Failed to write data: %s", e)
            raise

    def __enter__(self) -> AWSClient:
        """Enter sync context manager."""
        return self

    def __exit__(self, exc_type: type | None, exc: Exception | None, tb: type | None) -> None:
        """Exit sync context manager."""
        if self._session:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.disconnect())
                else:
                    asyncio.run(self.disconnect())
            except Exception as e:
                logger.error(f"Error disconnecting from AWS: {e}")

    async def __aenter__(self) -> AWSClient:
        """Enter async context manager."""
        await self.connect()
        return self

    async def __aexit__(
        self, exc_type: type | None, exc: Exception | None, tb: type | None
    ) -> None:
        """Exit async context manager."""
        try:
            await self.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting from AWS: {e}")
