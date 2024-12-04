"""AWS connector implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator
from datetime import datetime
from enum import Enum
from typing import Any

import aioboto3
from pyarrow import BufferOutputStream, write_table
from pydantic import BaseModel

from src.connectors.aws.config import AWSConnectorConfig
from src.connectors.aws.utils import QueryState, get_table_query
from src.connectors.component import ConnectorComponent

logger = logging.getLogger(__name__)


class AWSClientType(Enum):
    """Enum for AWS client types."""

    S3 = "S3"
    ATHENA = "athena"
    GLUE = "glue"


class AWSConnector(ConnectorComponent):
    """AWS connector implementation."""

    _config = AWSConnectorConfig

    def __init__(self) -> None:
        """Initialize AWS connector."""
        super().__init__()
        self.session: aioboto3.Session | None = None

    async def connect(self) -> None:
        """Establish AWS session."""
        self.session = aioboto3.Session()
        logger.info(f"Connected to AWS services for database: {self.config.database}")

    async def disconnect(self) -> None:
        """Close AWS session."""
        if self.session:
            self.session = None
            logger.info(f"Disconnected from AWS services: {self.config.database}")

    async def read(self, input_format: type[BaseModel]) -> AsyncIterator[BaseModel]:
        """Execute a query and yield results."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        try:
            execution_id = await self.execute_query(self.config.source_query)
            async for record in self._walk_response(execution_id):
                logger.debug(f"Parsed record: {json.dumps(record)}")
                try:
                    yield input_format(**record)
                except ValueError as e:
                    logger.error(f"Error parsing record: {e}")
        except Exception as e:
            raise RuntimeError("Error executing and retrieving query results") from e

    async def write(
        self,
        records: AsyncIterator[BaseModel],
        output_format: type[BaseModel],
        partitions: dict[str, str] | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Write records to S3 and ensure table exists."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        base_dir = os.path.join(
            self.config.output_location,
            output_format.__name__,
            [f"{col}=${{{col}}}" for col in partitions] if partitions else "",
        )
        file_name = os.path.join(base_dir, f"{datetime.now():%S}.parquet")
        table_name = f"{self.config.database}.{output_format.__name__}"

        try:
            await self._write_to_s3(records, file_name)
            table_query = await self._get_create_table_stmt(
                model_name=table_name, output_location=self.config.output_location
            )
            if table_query:
                await self.execute_query(table_query)
            if partitions:
                await self.execute_query(f"MSCK REPAIR TABLE {table_name} PARTITION ({base_dir})")
        except Exception as e:
            raise RuntimeError("Error writing output") from e

    async def execute_query(self, query: str) -> str:
        """Execute a query and wait for completion."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        async with self.session.client(AWSClientType.ATHENA.value) as client:
            try:
                response = await client.start_query_execution(QueryString=query)
                execution_id = response["QueryExecutionId"]

                async with asyncio.timeout(self.config.timeout):
                    while True:
                        status_response = await client.get_query_execution(
                            QueryExecutionId=execution_id
                        )
                        state = QueryState.from_response(
                            status_response["QueryExecution"]["Status"]
                        )

                        if state in QueryState.terminal_states():
                            if state == QueryState.SUCCEEDED:
                                return execution_id
                            raise RuntimeError(f"Query failed with state: {state}")

                        await asyncio.sleep(1)
            except Exception as e:
                raise RuntimeError(f"Error executing query: {e}") from e

    async def _walk_response(self, query_id: str) -> AsyncIterator[dict[str, Any]]:
        """Process rows from Athena response into records."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        async with self.session.client(AWSClientType.ATHENA.value) as client:
            try:
                paginator = client.get_paginator("get_query_results")
                async for page in paginator.paginate(QueryExecutionId=query_id):
                    for row in page["ResultSet"]["Rows"]:
                        yield {col["VarCharValue"] for col in row["Data"]}
            except aioboto3.exceptions.ClientError as e:
                raise RuntimeError(f"Error retrieving query results: {e}") from e

    async def _write_to_s3(self, records: AsyncIterator[BaseModel], output_name: str) -> None:
        """Write records to S3."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        async with self.session.client(AWSClientType.S3.value) as client:
            try:
                output = BufferOutputStream()
                write_table()  # Note: This seems incomplete, you might want to pass parameters
                await client.put_object(
                    Bucket=self.config.output_location, Key=output_name, Body=output.getvalue()
                )
            except aioboto3.exceptions.ClientError as e:
                raise RuntimeError(f"Error writing to S3: {e}") from e

    async def _get_create_table_stmt(self, model_name: str, output_location: str) -> str | None:
        """Get CREATE TABLE statement if table doesn't exist."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        async with self.session.client(AWSClientType.GLUE.value) as client:
            try:
                await client.get_table(DatabaseName=self.config.database, Name=model_name)
                return None
            except aioboto3.exceptions.ClientError as e:
                if "EntityNotFoundException" not in str(e):
                    raise RuntimeError(f"Error accessing table {model_name}: {e}") from e

                return get_table_query(model_name=model_name, base_dir=output_location)
