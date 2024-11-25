"""AWS client for Glue and Athena services."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from boto3 import Session
from botocore.exceptions import ClientError
from pandas import DataFrame

from src.aws import AWSConfig, State

logger = logging.getLogger(__name__)


@dataclass
class Table:
    """Glue table metadata."""

    _GLUE2PY = {
        "string": str,
        "int": int,
        "bigint": int,
        "double": float,
        "float": float,
        "boolean": bool,
        "binary": bytes,
        "timestamp": str,
        "date": str,
        "array": list,
        "map": dict,
        "struct": dict,
    }

    doc: str = ""
    columns: list[dict[str, str]] = field(default_factory=list)
    schema: dict[str, Any] = field(init=False)

    def __post_init__(self) -> None:
        """Initialize schema from columns."""
        logger.debug("Initializing schema from %d columns", len(self.columns))
        self.schema = {
            col["Name"]: self._GLUE2PY.get(col["Type"].lower(), str) for col in self.columns
        }

    @classmethod
    def from_glue(cls, doc: str, data: dict[str, Any]) -> Table:
        """Create table from Glue metadata."""
        logger.debug("Creating Table from Glue metadata")
        columns = data.get("StorageDescriptor", {}).get("Columns", [])
        return cls(doc=doc, columns=columns)

    def to_dict(self) -> dict[str, Any]:
        """Convert table to dictionary."""
        logger.debug("Converting table to dictionary")
        return {"columns": self.columns}

    def __bool__(self) -> bool:
        """Return True if table has columns."""
        return bool(self.columns)


class AWSClient:
    """A client for interacting with AWS Glue and Athena services.

    This client provides functionality to:
    - Execute SQL queries on AWS Athena
    - Retrieve table metadata from AWS Glue
    - Handle query pagination and result processing
    - Manage AWS session and client lifecycle

    The client implements both sync and async context managers for resource cleanup.

    Attributes:
        config (AWSConfig): Configuration for AWS services including region and output location
        athena: Boto3 Athena client for query execution
        glue: Boto3 Glue client for metadata operations
    """

    def __init__(self, config: AWSConfig) -> None:
        """Initialize AWS client with config."""
        self.config = config
        self._session = Session(region_name=self.config.region)

        try:
            self.athena = self._session.client("athena")
            self.glue = self._session.client("glue")
            logger.debug("AWS client initialized with config: %s", self.config)
        except Exception as e:
            self.__exit__(type(e), e, e.__traceback__)
            raise Exception("Failed to initialize AWS clients") from e

    def _get_query_results(self, query_execution_id: str) -> DataFrame:
        """Fetch results for a completed Athena query."""
        try:
            logger.debug("Fetching results for query: %s", query_execution_id)
            response = self.athena.get_query_results(QueryExecutionId=query_execution_id)
            result_set = response.get("ResultSet", {})

            if not result_set:
                logger.error("Empty result set from Athena")
                raise ValueError("No ResultSet returned from Athena")

            # Extract column metadata
            metadata = result_set.get("ResultSetMetadata", {})
            column_info = metadata.get("ColumnInfo", [])
            columns = [col.get("Name", f"col_{i}") for i, col in enumerate(column_info)]
            logger.debug("Query returned %d columns: %s", len(columns), columns)

            # Process results into DataFrame
            rows = []
            for row in result_set.get("Rows", [])[1:]:  # Skip header
                data = row.get("Data", [])
                values = [field.get("VarCharValue", "") for field in data]
                rows.append(values)

            df = DataFrame(rows, columns=columns)
            logger.info("Query completed successfully with %d rows", len(df))
            return df

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_msg = e.response["Error"]["Message"]
            logger.error("AWS error fetching results: %s - %s", error_code, error_msg)
            raise Exception("Error fetching Athena results") from e

    def execute_query(
        self,
        query: str,
        database: str,
        output_location: str | None = None,
    ) -> DataFrame:
        """Execute Athena query and return results as DataFrame."""
        output_location = output_location or self.config.output
        logger.info("Executing query on database: %s", database)
        logger.debug("Query: %s", query)

        try:
            response = self.athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": output_location},
            )

            query_id = response.get("QueryExecutionId")
            if not query_id:
                logger.error("No QueryExecutionId returned from Athena")
                raise ValueError("No QueryExecutionId returned from Athena")

            logger.info("Started Athena query: %s", query_id)
            return self._wait_for_query(query_id)

        except ClientError as e:
            err = e.response.get("Error")
            code = err.get("Code", "")
            msg = err.get("Message", "")

            raise Exception(f"Error {code} executing Athena query: {msg}") from e

    def _wait_for_query(self, query_id: str) -> DataFrame:
        """Wait for query completion and return results."""
        logger.debug("Waiting for query completion: %s", query_id)

        for attempt in range(self.config.max_retries):
            try:
                status = self.athena.get_query_execution(QueryExecutionId=query_id)
                state = status["QueryExecution"]["Status"]["State"]
                logger.debug(
                    "Query state: %s (attempt %d/%d)", state, attempt + 1, self.config.max_retries
                )

                match State(state):
                    case State.Category.NOK:
                        reason = status["QueryExecution"]["Status"]["StateChangeReason"]
                        logger.error("Query failed: %s", reason)
                        raise Exception(f"Query failed: {reason}")
                    case State.Category.OK:
                        logger.info("Query completed successfully")
                        return self._get_query_results(query_id)
                    case State.Category.PENDING:
                        logger.debug("Query still running: %s", query_id)
                    case _:
                        logger.warning("Query state not recognized: %s", state)

                wait_time = min(
                    (1.05 * self.config.wait_time) ** (attempt + 1),
                    self.config.max_wait_time,
                )
                logger.debug("Waiting %.2f seconds before next check", wait_time)
                time.sleep(wait_time)

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_msg = e.response["Error"]["Message"]
                logger.error("AWS error checking status: %s - %s", error_code, error_msg)

                raise Exception(f"Error checking query status: {error_msg}") from e

        logger.error("Query timed out: %s", query_id)
        raise TimeoutError(f"Query timed out after {self.config.max_retries} retries")

    def get_table(self, database: str, table_name: str) -> Table:
        """Get table details from Glue catalog."""
        logger.info("Getting table details: %s.%s", database, table_name)

        try:
            response = self.glue.get_table(DatabaseName=database, Name=table_name)
            table_data = response.get("Table")

            if not table_data:
                logger.error("No table information found for %s.%s", database, table_name)
                return Table()

            storage_desc = table_data.get("StorageDescriptor", {})
            doc = f"""
            Table: {database}.{table_name}
            Location: {storage_desc.get('Location', '')}
            Last Updated: {storage_desc.get('UpdateTime', '')}
            """

            logger.debug("Table details retrieved successfully")
            return Table.from_glue(doc.strip(), table_data)

        except ClientError as e:
            raise Exception("AWS Glue error: %s", str(e)) from e

    def __enter__(self) -> AWSClient:
        """Context manager entry."""
        logger.debug("Entering AWS client context")
        return self

    def __exit__(self, exc_type: type | None, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        logger.debug("Exiting AWS client context")
        try:
            self.athena.close()
            self.glue.close()
            self._session.close()
            logger.debug("AWS clients closed successfully")
        except Exception as e:
            logger.error("Error closing AWS clients: %s", str(e))
            raise
