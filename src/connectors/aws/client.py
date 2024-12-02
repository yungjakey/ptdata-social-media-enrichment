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
from pyarrow import parquet as pq
from pydantic import BaseModel, ValidationError

from src.common.types import TypeConverter
from src.connectors.aws.types import (
    AWSClientConfig,
    MalformedResponseError,
    NotConnectedError,
    QueryExecutionError,
    QueryState,
)
from src.connectors.types import ConnectorConfig

logger = logging.getLogger(__name__)


class AWSClient(ConnectorConfig):
    """AWS Athena client implementation."""

    POLL_INTERVAL = 1.0  # seconds

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialize AWS client."""
        # Validate config type and convert to AWSClientConfig
        if not isinstance(config, dict):
            raise ValueError("Config must be a dictionary")

        self.config = AWSClientConfig.from_dict(**config)
        self._session = None
        self._athena = None
        self._glue = None
        self._s3 = None

    async def validate(self) -> None:
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

    async def _wait(self, query_id: str, timeout: float = 300.0) -> None:
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

    async def read(
        self,
        input_model: type[BaseModel],
    ) -> AsyncIterator[BaseModel | dict[str, type]]:
        """Execute a query and yield results."""
        if not self._athena:
            raise NotConnectedError("Not connected to AWS")

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

    async def _write_to_s3(
        self, records: AsyncIterator[BaseModel], output_model: type[BaseModel], output_dir: str
    ):
        """Write records to a table using CTAS with partitioning."""
        # validate results
        data = []
        async for record in records:
            if not isinstance(record, BaseModel):
                logger.warning(f"Skipping non-model record: {record}")
                continue
            data.append(record.model_dump())

        if not data:
            logger.warning("No valid records to write")
            return

        # create pyarrow table
        table = pq.Table.from_pandas(pd.DataFrame(data))

        # write to s3
        try:
            await self._s3.put_object(
                Bucket=urlparse(self.config.params.output_location).netloc,
                Key=f"{output_dir}/data.parquet",
                Body=table.to_buffer().tobytes(),
            )
        except ClientError as e:
            raise RuntimeError(f"Error writing to {output_dir}: {e}") from e

    async def _create_table(self, name: str, output_dir: str, schema: dict[str, str]) -> None:
        create_table = f"""
            CREATE TABLE IF NOT EXISTS "{self.config.params.database}.{name}" (
                {', '.join(f'"{name}" {type_}' for name, type_ in schema.items())}
            )
            PARTITIONED BY (year, month, day, hour)
            STORED AS PARQUET
            LOCATION '{output_dir}'
            """

        try:
            query_id = await self._submit(create_table)
            await self._wait_for_query(query_id)

            # Repair table to pick up new partition
            repair_query = f'MSCK REPAIR TABLE "{self.config.params.database}.{name}"'
            query_id = await self._submit(repair_query)
            await self._wait_for_query(query_id)
        except ClientError as e:
            raise ClientError(f"Failed to create table {name}: {e}") from e

    async def write(
        self,
        records: AsyncIterator[BaseModel],
        output_model: type[BaseModel],
        execution_time: datetime,
    ) -> None:
        """Write records to a table using CTAS with partitioning."""
        if not self._s3 or not self._athena:
            raise NotConnectedError("Not connected to AWS")

        # get schema
        schema = TypeConverter.py2athena(output_model)

        # partition location
        base_dir = urlparse(self.config.params.output_location).path
        output_dir = f"{base_dir}/{execution_time:year=%Y/month=%m/day=%d/hour=%H}/model={output_model.__name__}"

        # write to s
        await self._write_to_s3(records, output_model, output_dir)

        # create table
        await self._create_table(output_model.__name__, output_dir, schema)

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
