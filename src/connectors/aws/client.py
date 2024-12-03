"""AWS Athena client implementation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from itertools import zip_longest
from typing import Any
from urllib.parse import urlparse

import aioboto3
from botocore.exceptions import ClientError
from pyarrow import BufferOutputStream
from pyarrow import parquet as pq
from pydantic import BaseModel, ValidationError

from src.connectors.aws.types import AWSConfig
from src.connectors.aws.utils import (
    MalformedResponseError,
    NotConnectedError,
    QueryExecutionError,
    QueryState,
)
from src.connectors.types import BaseConnector

logger = logging.getLogger(__name__)


_BASE_PROPS = {
    "projection.enabled": "true",
    "projection.year.type": "integer",
    "projection.year.range": f"{datetime.now().year-1},{datetime.now().year+5}",
    "projection.month.type": "integer",
    "projection.month.range": "1,12",
}


class AWSClient(BaseConnector):
    """AWS Athena client implementation."""

    POLL_INTERVAL = 1.0  # seconds

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize AWS client."""

        # Validate config type and convert to AWSClientConfig
        if not isinstance(config, dict):
            raise ValueError("Config must be a dictionary")

        self.config = AWSConfig.from_dict(**config)
        self._session = None
        self._athena = None
        self._glue = None
        self._s3 = None

    async def validate(
        self,
    ) -> None:
        """Validate AWS client configuration."""
        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        try:
            # Validate that the workgroup exists
            response = await self._athena.get_work_group(WorkGroup=self.config.params.workgroup)
            if not response.get("WorkGroup"):
                raise ValueError(f"WorkGroup '{self.config.params.workgroup}' not found")
        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidRequestException":
                raise ValueError(f"WorkGroup '{self.config.params.workgroup}' not found") from e
            raise

    async def connect(self) -> None:
        """Connect to AWS services."""
        if not self._session:
            self._session = aioboto3.Session(region_name=self.config.params.region)

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
        if self.config.params.output_location is not None:
            await self._s3.head_bucket(Bucket=urlparse(self.config.params.output_location).netloc)

    async def disconnect(self) -> None:
        """Disconnect from AWS services."""

        # No need to explicitly call __aexit__ as async context managers handle this
        self._session = None
        self._athena = None
        self._glue = None
        self._s3 = None

    def _get_query_state(
        self,
        response: dict[str, Any],
    ) -> tuple[QueryState, str | None]:
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

    async def _submit(self, query: str) -> str:
        """Execute a query and return the execution ID."""

        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        try:
            response = await self._athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.config.params.database},
                ResultConfiguration={"OutputLocation": self.config.params.output_location},
                WorkGroup=self.config.params.workgroup,
            )
            query_execution_id = response.get("QueryExecutionId")
            if not query_execution_id:
                raise MalformedResponseError(response, "QueryExecutionId")

            return query_execution_id
        except ClientError as e:
            logger.error("Failed to execute query: %s", e)
            raise

    async def _wait(
        self,
        query_id: str,
        timeout: float = 300.0,
    ) -> None:
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
                logger.info(
                    f"Query {query_id} is in state {state.name}. Sleeping for {self.POLL_INTERVAL} seconds."
                )
                await asyncio.sleep(self.POLL_INTERVAL)

        # If we've exhausted all iterations without reaching a terminal state
        raise TimeoutError(f"Query {query_id} did not complete within {timeout} seconds")

    def _get(self, query_id: str) -> list[dict[str, type]]:
        """Process rows from Athena response into list of records."""

        response = self._athena.get_query_results(QueryExecutionId=query_id)

        # Extract headers from first row if not done
        rows = response.get("ResultSet", {}).result_set.get("Rows", [])
        if not rows:
            raise ValueError("ResultSet is empty")

        headers = [
            col.get("VarCharValue", f"column_{i}") for i, col in enumerate(rows[0].get("Data", []))
        ]
        rows = rows[1:] if len(rows) > 1 else []

        records = []
        for row in rows:
            # Skip invalid rows
            if not row or "Data" not in row:
                logger.warning("Skipping invalid row: %s", row)
                continue

            # Create record with safe header-value mapping
            record = {}
            for header, cell in zip_longest(headers or [], row.get("Data", []), fillvalue=None):
                # Handle missing headers or cells
                if header is None or cell is None:
                    continue

                value = cell.get("VarCharValue", "")
                record[header] = value.strip() if value else None

            # Add non-empty records
            if record:
                records.append(record)

        return records

    async def _write(
        self,
        records: AsyncIterator[BaseModel],
        output_name: str,
    ) -> None:
        # Convert records directly to pyarrow table
        data = [record.model_dump() async for record in records]
        table = pq.Table.from_pylist(data)

        # Create an in-memory buffer to write the table
        buffer = BufferOutputStream()
        pq.write_table(table, buffer)

        # Write to s3
        try:
            await self._s3.put_object(
                Bucket=urlparse(self.config.params.output_location).netloc,
                Key=output_name,
                Body=buffer.getvalue().to_pybytes(),
            )
        except ClientError as e:
            raise RuntimeError(f"Error writing to {output_name}") from e

    async def _create_or_update(
        self,
        database_name: str,
        table_name: str,
        base_dir: str,
        props: dict[str, str],
    ):
        props_str = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        try:
            # Check if table exists
            await self._glue.get_table(DatabaseName=database_name, Name=table_name)

            # Update existing table with partition projection
            alter_table = f"""
            ALTER TABLE "{table_name}" SET TBLPROPERTIES ({props_str})
            """
            await self._submit(alter_table)

        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                # Create new table with partition projection
                create_table = f"""
                CREATE EXTERNAL TABLE "{table_name}"
                PARTITIONED BY (
                    year string,
                    month string
                )
                STORED AS PARQUET
                LOCATION '{base_dir}'
                TBLPROPERTIES ({props_str})
                """
                try:
                    query_id = await self._submit(create_table)
                    await self._wait_for_query(query_id)
                except Exception as create_error:
                    raise RuntimeError(
                        f"Failed to create table {table_name}: {create_error}"
                    ) from create_error
            else:
                raise RuntimeError(f"Error accessing table {table_name}: {e}") from e

    async def read(
        self,
        input_model: type[BaseModel],
    ) -> AsyncIterator[BaseModel]:
        """Execute a query and yield results."""

        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

        # Validate input model
        if not issubclass(input_model, BaseModel):
            raise ValueError(f"Input model must be a Pydantic BaseModel, got {input_model}")

        # submit query
        query_id = await self._submit(self.config.params.query)

        # await result
        await self._wait(query_id, timeout=self.config.params.max_wait_time)

        # validate and yield results
        for record in self._get(query_id):
            try:
                yield input_model(**record)
            except ValidationError as e:
                logger.error(f"{e}: {record}")
                continue

    async def write(
        self,
        records: AsyncIterator[BaseModel],
        model_name: str,
        execution_time: datetime,
    ) -> None:
        # Generate partition-aware output directory
        base_dir = f"{self.config.params.output_location}/model={model_name}"
        output_name = f"{base_dir}/{execution_time:year=%y/month=%m/:%S}.parquet"

        # Write records to S3 first
        await self._write(records, output_name)

        # Define table properties
        table_props = {
            **_BASE_PROPS,
            "storage.location.template": f"{base_dir}/year=${{year}}/month=${{month}}",
        }
        await self._create_or_update(
            self.config.params.database,
            model_name,
            base_dir,
            table_props,
        )
