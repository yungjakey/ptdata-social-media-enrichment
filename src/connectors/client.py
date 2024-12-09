"""AWS connector implementation."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from datetime import datetime
from enum import Enum

import aioboto3
import jinja2
import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from src.common.component import ComponentFactory

from .config import AWSConnectorConfig
from .utils import QUERY_TEMPLATE, QueryState, TypeMap

logger = logging.getLogger(__name__)


class AWSClientType(str, Enum):
    """Enum for AWS client types."""

    S3 = "s3"
    ATHENA = "athena"
    GLUE = "glue"


# TODO: Refactor as Step Function!


class AWSConnector(ComponentFactory):
    """AWS connector implementation."""

    _config = AWSConnectorConfig
    _clients: dict[AWSClientType, aioboto3.Client] = {c: None for c in AWSClientType}

    def __init__(self, config: AWSConnectorConfig) -> None:
        """Initialize AWS connector."""

        super().__init__(config)
        self.session: aioboto3.Session | None = None
        self.table = self.config.table_config
        self.catalog = Catalog.load(self.config.table.database)
        self.iceberg_table: Table = self.catalog.load_table(
            f"{self.config.table.database}.{self.config.table.table_name}"
        )

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

    async def list(self, prefix: str) -> list[dict[str, any]]:
        """List files in the configured table location."""
        if (client := await self.client(AWSClientType.S3)) is None:
            raise RuntimeError("Cant connect to S3")

        try:
            path = os.path.join(self.config.bucket_name)
            response = await client.list_objects_v2(
                Bucket=self.config.bucket_name,
                Prefix=prefix,
            )

            if "Contents" not in response:
                response["Contents"] = {}

            logger.info(f"Found {len(response['Contents'])} files in {path}")
            return response["Contents"]

        except Exception as e:
            logger.error(f"Error listing S3 files: {e}")
            raise RuntimeError("Failed to list S3 files") from e

    async def _write_to_s3(
        self,
        table: pa.Table,
        key: str,
    ) -> None:
        """Write table to S3 using Iceberg format."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        # Get credentials from the session
        credentials = await self.session.get_credentials()
        if not credentials:
            raise RuntimeError("No AWS credentials available")

        # Write data to Iceberg table
        self.iceberg_table.append(table.to_pandas())
        logger.info(f"Data written to Iceberg table: {self.iceberg_table}")

    async def write(
        self,
        records: list[dict[str, type]],
        model: type[BaseModel],
        now: datetime | None = None,
    ) -> None:
        """Write records to S3 and update Glue catalog."""

        # TODO: Change to Iceberg format

        if not records:
            raise ValueError("No records to write")

        if not now:
            now = datetime.now()

        # Get partition info
        base_path = os.path.join(
            self.table.database,
            self.table.table_name,
        )
        partition_path = os.path.join(
            base_path,
            *[f"{k}={getattr(now, k)}" for k in self.table.partitions],
        )
        s3_key = os.path.join(
            partition_path,
            f"{now:%s}.{self.config.target_format}",
        )

        # Create iceberg table
        ice_schema = TypeMap.model2iceberg(model)
        await self.create_table(
            columns=ice_schema,
            partitions=self.table.partitions,
            output_location=base_path,
        )

        # Write to S3
        pa_schema = TypeMap.model2athena(model)
        table = pa.Table.from_pylist(records, schema=pa_schema)
        await self._write_to_s3(
            table=table,
            key=s3_key,
        )

    async def _execute_query(self, query: str) -> str | None:
        """Execute a query and wait for completion."""
        if (client := await self.client(AWSClientType.ATHENA)) is None:
            raise RuntimeError("Cannot connect to Athena")

        response = await client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                "OutputLocation": self.config.output_location,
            },
            WorkGroup=self.config.workgroup,
        )
        execution_id = response["QueryExecutionId"]
        logger.info(f"Started Athena query with ID: {execution_id}")

        async with asyncio.timeout(self.config.timeout):
            while True:
                status_response = await client.get_query_execution(QueryExecutionId=execution_id)
                state = QueryState.from_response(status_response["QueryExecution"]["Status"])

                if state in QueryState.terminal_states():
                    if state == QueryState.SUCCEEDED:
                        return execution_id

                    error_info = status_response["QueryExecution"]["Status"]
                    error_message = error_info.get("StateChangeReason", "Unknown error")
                    error_state = error_info.get("State", "UNKNOWN")

                    raise RuntimeError(
                        f"Athena query failed with state: {error_state}. Error: {error_message}"
                    )

                await asyncio.sleep(self.config.poll_interval)

    async def _parse_results(
        self,
        query_id: str,
    ) -> AsyncIterator[dict[str, type]]:
        """Process rows from Athena response into records."""

        if (client := await self.client(AWSClientType.ATHENA)) is None:
            raise RuntimeError("Cant connect to Athena")

        paginator = client.get_paginator("get_query_results")
        col_names, col_types = [], []

        async for page in paginator.paginate(QueryExecutionId=query_id):
            results = page["ResultSet"]

            if not col_names or not col_types:  # first page
                if len(results["Rows"]) <= 1:  # Only header row present
                    logger.info("Query returned no data rows, yielding nothing.")
                    return  # Simply exit the generator gracefully without yielding anything
                col_names, col_types = await self._get_column_metadata(results)

            for row in results["Rows"][1:]:
                yield self._process_row(row["Data"], col_names, col_types)

    async def _get_column_metadata(self, response: dict) -> tuple[list[str], list[str]]:
        """Extract and validate column metadata from Athena response."""
        metadata = response.get("ResultSetMetadata")
        if not metadata:
            logger.warning("No metadata in response")
            return [], []

        column_info = metadata.get("ColumnInfo", [])
        if not column_info:
            logger.warning("No columns in metadata")
            return [], []

        return zip(*[(col["Name"], col["Type"]) for col in column_info], strict=False)

    def _process_row(
        self, data: list[dict[str, type]], col_names: list[str], col_types: list[str]
    ) -> dict:
        """Process a single row of Athena results."""
        try:
            return {
                col_name: TypeMap.from_athena(col_type, data.get("VarCharValue"))
                for col_name, col_type, data in zip(col_names, col_types, data, strict=False)
            }
        except (KeyError, IndexError) as e:
            logger.error(f"Malformed row data: {e}")
            return {}

    async def read(self) -> list[dict[str, type]]:
        """Execute a query and return results."""

        # Execute query
        try:
            execution_id = await self._execute_query(self.config.source_query)
            if not execution_id:
                raise RuntimeError("No query id received from Athena")
        except TimeoutError as e:
            raise RuntimeError(
                f"Athena query timed out after {self.config.timeout.total_seconds()} seconds"
            ) from e
        except Exception as e:
            raise RuntimeError(f"Failed to execute Athena query: {str(e)}") from e

        # Return results
        try:
            return [record async for record in self._parse_results(execution_id)]
        except Exception as e:
            raise RuntimeError(f"Failed to parse Athena query results: {str(e)}") from e

    async def create_table(
        self, columns: list[dict], partitions: list[dict], output_location: str
    ) -> None:
        """Create a table using the Iceberg format."""

        template = jinja2.Template(QUERY_TEMPLATE)
        sql = template.render(
            table_name=f"{self.config.table.database}.{self.config.table.table_name}",
            columns=columns,
            partitions=partitions,
            output_location=output_location,
        )
        await self._execute_query(sql)
        logger.info(f"Created table with SQL: {sql}")

    async def __aenter__(self) -> AWSConnector:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
