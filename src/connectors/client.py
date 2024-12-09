"""AWS connector implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator
from datetime import datetime
from enum import Enum

import aioboto3
import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
from pydantic import BaseModel

from src.common.component import ComponentFactory

from .config import AWSConnectorConfig
from .utils import QueryState, TypeMap

logger = logging.getLogger(__name__)

# TODO: fix this abomination
HEX_FIELDS = {"id", "date_key", "channel_key", "post_key"}


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

    async def _get_or_create_table(
        self,
        base_path: str,
        model_columns: list[dict[str, str]],
    ) -> dict[str, type]:
        """Get or create table and return its schema."""

        if (client := await self.client(AWSClientType.GLUE)) is None:
            raise RuntimeError("Cant connect to Glue")

        # Ensure database exists
        try:
            await client.get_database(Name=self.table.database)
            logger.debug(f"Database {self.table.database} found")
        except client.exceptions.EntityNotFoundException as e:
            logger.error(f"Database {self.table.database} not found")
            raise RuntimeError("Database not found") from e

        # check table
        try:
            table = await client.get_table(
                DatabaseName=self.table.database,
                Name=self.table.table_name,
            )
            logger.info(f"Table {self.table.table_name} found")
        except client.exceptions.EntityNotFoundException:
            logger.info(f"Table {self.table.table_name} not found, creating")

            # Create storage descriptor for table
            storage_descriptor = {
                "Columns": model_columns,
                "Location": base_path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            }

            await client.create_table(
                DatabaseName=self.table.database,
                TableInput={
                    "Name": self.table.table_name,
                    "StorageDescriptor": storage_descriptor,
                    "PartitionKeys": self.table.partition_keys,
                },
            )

            # Construct table definition since create_table returns empty response
            table = {
                "StorageDescriptor": storage_descriptor,
                "PartitionKeys": self.table.partition_keys,
            }

        return table

    async def _drop_table(self) -> None:
        """Drop the table if it exists."""
        if (client := await self.client(AWSClientType.GLUE)) is None:
            raise RuntimeError("Cant connect to Glue")

        try:
            await client.delete_table(
                DatabaseName=self.table.database,
                Name=self.table.table_name,
            )
            logger.info(f"Table {self.table.table_name} dropped")
        except client.exceptions.EntityNotFoundException:
            logger.info(f"Table {self.table.table_name} does not exist")

    async def _ensure_partition(
        self,
        partition_path: str,
        partition_values: list[str],
    ) -> None:
        """Ensure partition exists, creating it if needed."""
        if (client := await self.client(AWSClientType.GLUE)) is None:
            raise RuntimeError("Cant connect to Glue")

        try:
            await client.get_partition(
                DatabaseName=self.table.database,
                TableName=self.table.table_name,
                PartitionValues=partition_values,
            )
            logger.info(f"Partition {partition_path} found")
            return
        except client.exceptions.EntityNotFoundException:
            logger.info(f"Partition {partition_path} not found, creating")

            # Create partition with Parquet format configuration
            storage_descriptor = {
                "Location": partition_path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
            }

            await client.create_partition(
                DatabaseName=self.table.database,
                TableName=self.table.table_name,
                PartitionInput={
                    "Values": partition_values,
                    "StorageDescriptor": storage_descriptor,
                },
            )
            logger.info(f"Partition {partition_path} created")

    async def _write_to_s3(
        self,
        table: pa.Table,
        key: str,
    ) -> None:
        """Write table to S3."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        # Get credentials from the session
        credentials = await self.session.get_credentials()
        if not credentials:
            raise RuntimeError("No AWS credentials available")
        frozen_creds = await credentials.get_frozen_credentials()

        # Create S3 filesystem
        fs = pa_fs.S3FileSystem(
            access_key=frozen_creds.access_key,
            secret_key=frozen_creds.secret_key,
            session_token=frozen_creds.token,
            region=self.config.region,
        )

        # Write to S3 with bucket name prefixed
        pq.write_table(
            table,
            where=os.path.join(self.config.bucket_name, key),
            filesystem=fs,
            use_dictionary=False,
            compression="snappy",
        )

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
        partition_values = [str(int(getattr(now, n))) for n in self.table.names]
        base_path = os.path.join(
            self.table.database,
            self.table.table_name,
        )
        partition_path = os.path.join(
            base_path,
            *[f"{n}={v}" for n, v in zip(self.table.names, partition_values, strict=False)],
        )

        # Get schema from model
        model_columns = TypeMap.model2athena(model)

        # Get table schema
        table = await self._get_or_create_table(
            os.path.join(self.config.output_location, base_path), model_columns
        )

        data_columns = table["StorageDescriptor"]["Columns"]
        partition_columns = table["PartitionKeys"]
        columns = data_columns + partition_columns

        logger.debug(f"Got schema: {json.dumps(columns, indent=2)}")

        # Convert schema to PyArrow
        schema = TypeMap.athena2pyarrow(columns)
        table = pa.Table.from_pylist(records, schema=schema)

        # Write to S3
        s3_key = os.path.join(
            partition_path,
            f"{now:%s}.{self.config.target_format}",
        )
        logger.info(f"Writing to {os.path.join(self.config.bucket_name, s3_key)}")
        await self._write_to_s3(
            table=table,
            key=s3_key,
        )

        # Update Glue catalog
        try:
            path = os.path.join(
                self.config.output_location,
                partition_path,
            )
            await self._ensure_partition(
                partition_path=path,
                partition_values=partition_values,
            )
        except Exception as e:
            logger.error(f"Error checking/adding partition: {e}", exc_info=True)
            raise RuntimeError("Failed to check/update partition") from e

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
        if not self.config.source_query:
            raise ValueError("Source query is required")

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

    async def __aenter__(self) -> AWSConnector:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
