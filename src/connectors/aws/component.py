"""AWS connector implementation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from enum import Enum

import aioboto3
from pyarrow import BufferOutputStream, write_table

from src.connectors.aws.config import AWSConnectorConfig
from src.connectors.aws.utils import QueryState
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
    _clients: dict[AWSClientType, aioboto3.Client] = {c: None for c in AWSClientType}

    def __init__(self, config: AWSConnectorConfig) -> None:
        """Initialize AWS connector."""
        super().__init__()
        self.session: aioboto3.Session | None = None
        self.params: self.config.params.annotations = self.config.params

    @property
    def client(self, c: AWSClientType) -> aioboto3.Client:
        """Lazy load client."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        if c not in self._clients:
            self._clients[c] = self.session.client(c.value)

        return self._clients[c]

    async def connect(self) -> None:
        """Establish AWS session."""

        self.session = aioboto3.Session()
        logger.debug("Connected to AWS services")

    async def disconnect(self) -> None:
        """Close AWS session."""

        if not self.session:
            logger.warning("Not disconnecting, not connected to AWS")
            return

        for client in self._clients.values():
            try:
                await client.close()
            except Exception as e:
                logger.error(f"Error closing client: {e}")

        try:
            await self.session.close()
        except Exception as e:
            logger.error(f"Error closing session: {e}")
        finally:
            for name in self._clients:
                self._clients[name] = None

            self.session = None
            logger.debug("AWS session closed")

    async def read(
        self,
    ) -> tuple[dict[str, type], dict[str, type]]:
        """Execute a query and yield results."""

        if not self.session:
            raise RuntimeError("Not connected to AWS")

        try:
            execution_id = await self.execute_query(self.params.query)
        except Exception as e:
            raise RuntimeError("Error executing query") from e

        if not execution_id:
            raise RuntimeError("Not query id received")

        try:
            header = None
            data = []
            async for row in self.parse_results(execution_id):
                if not header:
                    header = row["Data"]

                data.append(row["Data"])
        except Exception as e:
            raise RuntimeError("Error executing and retrieving query results") from e

        if not header:
            raise RuntimeError("Query returned no results")

        if not data:
            raise RuntimeError("Query returned no data")

        return header, data

    async def execute_query(
        self,
        query: str,
    ) -> str | None:
        """Execute a query and wait for completion."""

        if client := self.client(AWSClientType.ATHENA) is None:
            raise RuntimeError("Cant connect to Athena")

        response = await client.start_query_execution(QueryString=query)
        execution_id = response["QueryExecutionId"]

        try:
            async with asyncio.timeout(self.params.timeout):
                while True:
                    status_response = await client.get_query_execution(
                        QueryExecutionId=execution_id
                    )
                    state = QueryState.from_response(status_response["QueryExecution"]["Status"])

                    if state in QueryState.terminal_states():
                        if state == QueryState.SUCCEEDED:
                            return execution_id
                        raise RuntimeError(f"Query failed with state: {state}")

                    await asyncio.sleep(self.params.poll_interval)

        except TimeoutError:
            logger.error(f"Query timed out after {self.params.timeout} seconds")
            return None

    async def parse_results(
        self,
        query_id: str,
    ) -> AsyncIterator[dict[str, type] | None]:
        """Process rows from Athena response into records."""
        if client := self.client(AWSClientType.ATHENA) is None:
            raise RuntimeError("Cant connect to Athena")

        try:
            async with asyncio.timeout(self.params.timeout):
                while True:
                    response = await client.get_query_results(QueryExecutionId=query_id)
                    yield from response["ResultSet"]["Rows"]

                    if not response["ResultSet"]["Rows"]:
                        return None
        except TimeoutError:
            logger.error(f"Result set timed out after {self.params.timeout} seconds")
            return None

    async def write(
        self,
        records: AsyncIterator[dict[str, type]],
    ) -> None:
        """Write records to S3 and ensure table exists."""
        if not self.session:
            raise RuntimeError("Not connected to AWS")

        now = datetime.now()
        base_dir = f"{now:year=%Y/month=%m/day=%d}"
        file_name = f"{now:%S}.{self.params.format}"

        try:
            await self._write_to_s3(records, base_dir, file_name)
        except Exception as e:
            raise RuntimeError("Error writing output") from e

    async def _write_to_s3(
        self, records: AsyncIterator[dict[str, type]], output_dir: str, output_name: str
    ) -> None:
        """Write records to S3."""
        if client := self.client(AWSClientType.S3) is None:
            raise RuntimeError("Cant connect to S3")

        output = BufferOutputStream()
        write_table(records, output)
        await client.put_object(Bucket=output_dir, Key=output_name, Body=output.getvalue())
