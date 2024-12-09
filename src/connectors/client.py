"""AWS connector implementation."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from datetime import datetime
from enum import Enum
from typing import Literal

import aioboto3
import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from src.common.component import ComponentFactory

from .config import AWSConnectorConfig
from .utils import IcebergConverter, QueryState

logger = logging.getLogger(__name__)

PARTITION_FIELD: Literal = "written_at"


class AWSClientType(str, Enum):
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
        self.table = self.config.table_config
        try:
            self.catalog = load_catalog(
                type="glue",
                uri=f"glue://{self.config.region}/{self.table.database}",
                warehouse=self.table.location,
            )
        except Exception as e:
            logger.error(
                f"Failed to load Glue catalog: {e}. Make sure AWS SSO credentials are valid."
            )
            raise

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

    async def _get_or_create_table(self, converter: IcebergConverter) -> Table:
        try:
            return self.catalog.load_table(f"{self.table.database}.{self.table.table_name}")
        except NoSuchTableError:
            # Table doesn't exist, create it
            iceberg_schema = converter.to_iceberg_schema()
            partition_transforms = converter.get_partition_transforms()

        return self.catalog.create_table(
            identifier=f"{self.table.database}.{self.table.table_name}",
            schema=iceberg_schema,
            partition_spec=partition_transforms,
            location=self.table.location,
            properties={
                "write.format.default": "parquet",
                "write.metadata.compression-codec": "gzip",
            },
        )

    async def write(
        self,
        records: list[dict[str, type]],
        model: type[BaseModel],
        now: datetime | None = None,
    ) -> None:
        """Write records to S3 and update Glue catalog."""

        if not records:
            raise ValueError("No records to write")

        if not now:
            now = datetime.now()

        # Create converter
        converter = (
            IcebergConverter(model)
            .add_partitions(self.table.partitions)
            .set_datetime_field(self.table.datetime_field)
        )

        # Get or create table using Iceberg catalog
        try:
            iceberg_table = await self._get_or_create_table(converter)
        except Exception as e:
            raise RuntimeError(f"Failed to get or create Iceberg table: {str(e)}") from e

        # Convert records to PyArrow table
        pa_schema = converter.to_arrow_schema()
        table = pa.Table.from_pylist(records, schema=pa_schema)

        # Verify datetime field is not already in the records
        timestamp_array = pa.array([now] * len(records), type=pa.timestamp("us"))
        if self.table.datetime_field in table.column_names:
            logger.warning(
                f"Overriding datetime field '{self.table.datetime_field}' as it is already in the records"
            )
            table = table.remove_column(self.table.datetime_field)
        table = table.append_column(self.table.datetime_field, timestamp_array)

        # Write data using Iceberg table
        try:
            iceberg_table.append(table)
        except Exception as e:
            raise RuntimeError(f"Failed to append data to Iceberg table: {str(e)}") from e

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

    async def _process_row(
        self,
        records: list[dict[str, any]],
        col_names: list[str],
        col_types: list[str],
    ) -> dict[str, type]:
        res = {}
        for col_name, athena_type, data in zip(col_names, col_types, records, strict=False):
            athena_val = data.get("VarCharValue")
            if athena_val is None:
                res[col_name] = None
                continue

            py_type = IcebergConverter.athena2py(athena_type)
            py_val = py_type(athena_val) if athena_val else None

            res[col_name] = py_val

        return res

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
                col_names, col_types = await self._get_column_metadata(results)

                if len(results["Rows"]) <= 1:  # Only header row present
                    logger.info("Query returned no data rows, yielding nothing.")
                    return

            for row in results["Rows"][1:]:
                res = self._process_row(row["Data"], col_names, col_types)
                yield res

    async def read(self) -> list[dict[str, type]]:
        """Execute a query and return results."""

        # Execute query
        try:
            execution_id = await self._execute_query(self.config.source_query)
            if not execution_id:
                raise RuntimeError("No query id received from Athena")
        except TimeoutError as e:
            raise RuntimeError(
                f"Query timed out after {self.config.timeout.total_seconds()}s"
            ) from e
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {str(e)}") from e

        # Return results
        try:
            return [res async for res in self._parse_results(execution_id)]
        except Exception as e:
            raise RuntimeError(f"Failed to parse Athena query results: {str(e)}") from e

    async def __aenter__(self) -> AWSConnector:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
