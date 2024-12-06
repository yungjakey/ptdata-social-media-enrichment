"""AWS connector implementation."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from datetime import datetime
from enum import Enum

import aioboto3
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from src.common.component import ComponentFactory

from .config import AWSConnectorConfig
from .utils import QueryState, TypeConverter, _get_partition_path

logger = logging.getLogger(__name__)


class AWSClientType(Enum):
    """Enum for AWS client types."""

    S3 = "s3"
    ATHENA = "athena"
    GLUE = "glue"


class AWSConnector(ComponentFactory):
    """AWS connector implementation."""

    _config = AWSConnectorConfig
    _clients: dict[AWSClientType, aioboto3.Client] = {c: None for c in AWSClientType}

    def __init__(self, config: AWSConnectorConfig) -> None:
        """Initialize AWS connector."""
        super().__init__(config)
        self.session: aioboto3.Session | None = None

    async def client(self, c: AWSClientType) -> aioboto3.Client:
        """Lazy load client."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        if self._clients[c] is None:
            async with self.session.client(c.value) as client:
                self._clients[c] = client

        return self._clients[c]

    async def connect(self) -> None:
        """Establish AWS session."""

        self.session = aioboto3.Session()
        for c in AWSClientType:
            self._clients[c] = None
        logger.debug("Connected to AWS services")

    async def disconnect(self) -> None:
        """Close AWS session."""
        if not self.session:
            return

        for client in self._clients.values():
            if client is not None:
                try:
                    await client.close()
                except Exception as e:
                    logger.debug(f"Error closing client: {e}")

        self._clients = {c: None for c in AWSClientType}
        self.session = None
        logger.debug("AWS session closed")

    async def execute_query(self, query: str) -> str | None:
        """Execute a query and wait for completion."""

        if (client := await self.client(AWSClientType.ATHENA)) is None:
            raise RuntimeError("Cant connect to Athena")

        response = await client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                "OutputLocation": self.config.output_location,
            },
            WorkGroup=self.config.workgroup,
        )
        execution_id = response["QueryExecutionId"]

        try:
            async with asyncio.timeout(self.config.timeout):
                while True:
                    status_response = await client.get_query_execution(
                        QueryExecutionId=execution_id
                    )
                    state = QueryState.from_response(status_response["QueryExecution"]["Status"])

                    if state in QueryState.terminal_states():
                        if state == QueryState.SUCCEEDED:
                            return execution_id
                        error_message = status_response["QueryExecution"]["Status"].get(
                            "StateChangeReason", "Unknown error"
                        )
                        raise RuntimeError(
                            f"Query failed with state: {state}. Error: {error_message}"
                        )

                    await asyncio.sleep(self.config.poll_interval)

        except TimeoutError:
            logger.error(f"Query timed out after {self.config.timeout} seconds")
            return None

    async def parse_results(
        self,
        query_id: str,
    ) -> AsyncIterator[dict[str, type] | None]:
        """Process rows from Athena response into records."""

        if (client := await self.client(AWSClientType.ATHENA)) is None:
            raise RuntimeError("Cant connect to Athena")

        try:
            async with asyncio.timeout(self.config.timeout):
                response = await client.get_query_results(QueryExecutionId=query_id)
                if not response["ResultSet"]["Rows"]:
                    raise RuntimeError("Query returned no results")

                # Get column names from the first row
                header_row = response["ResultSet"]["Rows"][0]["Data"]
                columns = []
                for col in header_row:
                    # Handle different possible formats of column data
                    if isinstance(col, dict):
                        columns.append(col.get("VarCharValue", ""))
                    else:
                        columns.append(str(col))

                # Process each data row
                for row in response["ResultSet"]["Rows"][1:]:  # Skip header row
                    values = []
                    for col in row["Data"]:
                        if isinstance(col, dict):
                            values.append(col.get("VarCharValue", ""))
                        else:
                            values.append(str(col))
                    record = dict(zip(columns, values, strict=False))
                    yield record

        except TimeoutError:
            logger.error(f"Result set timed out after {self.config.timeout} seconds")

    async def _write_to_s3(
        self,
        records: list[dict[str, type]],
        schema: pa.Schema,
        file_name: str,
    ) -> None:
        """Write records to S3."""

        if (client := await self.client(AWSClientType.S3)) is None:
            raise RuntimeError("Cant connect to S3")

        table = pa.Table.from_pylist(records, schema=schema)
        output = pa.BufferOutputStream()
        pq.write_table(table, output)
        buffer = output.getvalue().to_pybytes()

        await client.put_object(
            Bucket=self.config.bucket_name,
            Key=file_name,
            Body=buffer,
        )

    async def _create_or_update(
        self,
        schema: dict[str, str],
        base_dir: str,
        partitions: dict[str, str],
    ):
        """create or update table"""
        if (client := await self.client(AWSClientType.GLUE)) is None:
            raise RuntimeError("Cant connect to Glue")

        location = os.path.join(self.config.output_location, base_dir)
        try:
            await client.get_table(DatabaseName=self.config.database, Name=self.config.table_name)
            logger.info("Table found, updating")
            await client.update_table(
                DatabaseName=self.config.database,
                TableInput={
                    "Name": self.config.table_name,
                    "StorageDescriptor": {
                        "Columns": schema,
                        "Location": location,
                    },
                },
            )
        except client.exceptions.EntityNotFoundException:
            logger.info("Table not found, creating")
            table_input = {
                "Name": self.config.table_name,
                "DatabaseName": self.config.database,
                "StorageDescriptor": {
                    "Columns": schema,
                    "Location": location,
                },
                "PartitionKeys": partitions,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "parquet"},
            }
            await client.create_table(DatabaseName=self.config.database, TableInput=table_input)

    async def read(
        self,
    ) -> tuple[dict[str, type], dict[str, type]]:
        """Execute a query and yield results."""

        if not self.config.source_query:
            raise RuntimeError("Query not configured")
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        # Execute query
        try:
            execution_id = await self.execute_query(self.config.source_query)
        except Exception as e:
            raise RuntimeError("Error executing query") from e

        if not execution_id:
            raise RuntimeError("Not query id received")

        logger.info(f"Query {execution_id} executed")

        # Parse results
        try:
            async for header, *rows in self.parse_results(execution_id):
                logger.info(f"Query returned {len(rows)} rows")
                return header, rows
        except Exception as e:
            raise RuntimeError("Error retrieving query results") from e

    async def write(
        self,
        records: list[dict[str, type]],
        output_format: type[BaseModel],
        partitions: dict[str, type] | None = None,
    ) -> None:
        """Write records to S3 and ensure table exists."""

        if not self.session:
            raise RuntimeError("Not connected to AWS")

        if not records:
            raise RuntimeError("No records to write")

        if partitions is None:
            partitions = {
                "year": int,
                "month": int,
                "day": int,
            }

        now = datetime.now()
        base_dir = _get_partition_path(now, partitions)
        file_name = os.path.join(base_dir, f"{now:%S}.{self.config.target_format}")

        pydantic_schema = TypeConverter.pydantic2py(output_format)

        # write
        pa_schema = TypeConverter.py2pa(pydantic_schema)
        try:
            await self._write_to_s3(records, pa_schema, file_name)
            logger.info(f"Wrote {len(records)} records to {base_dir}")
        except Exception as e:
            logger.error(f"Error writing output: {e}", exc_info=True)
            raise

        # # create or update table
        # glue_schema = TypeConverter.py2glue(pydantic_schema)
        # try:
        #     await self._create_or_update(glue_schema, base_dir, partitions)
        #     logger.info("Table created/updated")
        # except Exception as e:
        #     logger.error(f"Error creating/updating table: {e}", exc_info=True)
        #     raise

    async def __aenter__(self) -> AWSConnector:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
